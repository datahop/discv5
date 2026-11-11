//! Topic discovery (TopDisc) extension of discv5.
//!
//! A registrar keeps an ad cache of advertisements (`TopicTable`), admits advertisers
//! through a waiting-time function that penalises full caches, crowded topics and
//! clustered IP prefixes, and hands out HMAC-sealed tickets (`TicketSealer`) so an
//! advertiser can prove how long it has waited. An advertiser walks the registrars
//! around the topic hash with a `Registration` table, a searcher queries them with a
//! `Search` table. The wire messages live in `rpc`, the request handlers and the
//! registration and search loops in `service::topic`.

use crate::Enr;
use enr::NodeId;
use std::{net::IpAddr, time::Duration};

mod bound;
mod config;
mod iptree;
pub(crate) mod netutil;
pub mod params;
mod registration;
mod search;
mod searchfilter;
mod table;
mod ticket;

pub use config::TopicConfig;
pub use registration::{RegAttempt, RegAttemptState, Registration};
pub use search::Search;
pub use searchfilter::SearchFilter;
pub use table::TopicTable;
pub use ticket::{TICKET_SIZE, Ticket, TicketError, TicketSealer, TicketTiming, UsedTickets};

/// A topic (service) identifier: a 32-byte hash.
pub type TopicId = [u8; 32];

/// The service identifier of a service name: SHA-256 of its UTF-8 bytes.
pub fn topic_id(service: &str) -> TopicId {
    use sha2::{Digest, Sha256};
    Sha256::digest(service.as_bytes()).into()
}

/// The outcome of one REGTOPIC, from its REGCONFIRMATION.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegConfirmation {
    /// The registrar stores the advertisement for `lifetime`, as announced.
    Admitted { lifetime: Duration },
    /// Not admitted yet: retry with `ticket` after `wait`.
    Ticket { ticket: Vec<u8>, wait: Duration },
    /// The registrar rejected the request.
    Rejected,
}

impl RegConfirmation {
    /// Reads the `ticket` and `wait-time` of a REGCONFIRMATION.
    pub fn from_wire(ticket: Vec<u8>, wait_time_ms: u64) -> Self {
        let wait = Duration::from_millis(wait_time_ms);
        match (ticket.is_empty(), wait_time_ms) {
            (false, _) => RegConfirmation::Ticket { ticket, wait },
            (true, 0) => RegConfirmation::Rejected,
            (true, _) => RegConfirmation::Admitted { lifetime: wait },
        }
    }
}

/// The answer of a registrar to one REGTOPIC request.
#[derive(Debug, Clone)]
pub struct RegTopicResult {
    pub confirmation: RegConfirmation,
    /// Registrars near the topic that the registrar knows.
    pub aux: Vec<Enr>,
}

/// The answer of a registrar to one TOPICQUERY request.
#[derive(Debug, Clone)]
pub struct TopicQueryResult {
    /// Nodes registered for the topic.
    pub advertisers: Vec<Enr>,
    /// Registrars near the topic that the registrar knows.
    pub aux: Vec<Enr>,
    /// All announced packets arrived, up to `QUERY_RESP_LIMIT`. A partial response
    /// ended early: at the packet cap, a timeout or an error after a valid record.
    pub complete: bool,
}

/// What the registration and search tables need to know about a node.
pub trait TopicNode: Clone {
    fn node_id(&self) -> NodeId;
    /// The sequence number of the node record.
    fn seq(&self) -> u64;
    /// The address the node is dialled at, if its record has one.
    fn ip(&self) -> Option<IpAddr>;
}

impl TopicNode for Enr {
    fn node_id(&self) -> NodeId {
        Enr::node_id(self)
    }

    fn seq(&self) -> u64 {
        Enr::seq(self)
    }

    fn ip(&self) -> Option<IpAddr> {
        netutil::preferred_ip(self)
    }
}

/// The logarithmic distance between a topic and a node id: 0 when equal, else 1..=256.
pub(crate) fn log_dist(topic: &TopicId, id: &NodeId) -> usize {
    for (i, (a, b)) in topic.iter().zip(id.raw().iter()).enumerate() {
        let x = a ^ b;
        if x != 0 {
            return 256 - i * 8 - x.leading_zeros() as usize;
        }
    }
    0
}

/// The index of the bucket that holds `id` in a table of `depth` buckets ordered far
/// to close. Nodes closer than the last bucket go into the last bucket.
pub(crate) fn bucket_index(topic: &TopicId, id: &NodeId, depth: usize) -> usize {
    (256 - log_dist(topic, id)).min(depth - 1)
}

/// ENR key signalling the topic discovery capability.
pub const TOPIC_DISCOVERY_ENR_KEY: &str = "td";

/// Protocol version advertised in the `td` entry and required from peers.
pub const TOPIC_DISCOVERY_VERSION: u64 = 1;

/// Reports whether the record advertises a supported version of the topic discovery
/// capability. A missing entry, an undecodable value or another version all count as
/// unsupported.
pub fn supports_topic_discovery(enr: &Enr) -> bool {
    matches!(
        enr.get_decodable::<u64>(TOPIC_DISCOVERY_ENR_KEY),
        Some(Ok(TOPIC_DISCOVERY_VERSION))
    )
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use std::net::Ipv4Addr;

    /// A node with a chosen id, which a signed record cannot have.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct TestNode {
        pub(crate) id: NodeId,
        pub(crate) seq: u64,
        pub(crate) ip: Option<IpAddr>,
    }

    impl TopicNode for TestNode {
        fn node_id(&self) -> NodeId {
            self.id
        }

        fn seq(&self) -> u64 {
            self.seq
        }

        fn ip(&self) -> Option<IpAddr> {
            self.ip
        }
    }

    pub(crate) const TOPIC1: TopicId = {
        let mut t = [0u8; 32];
        t[0] = 1;
        t[1] = 2;
        t
    };

    /// A random id at logarithmic distance `dist` from `base`.
    pub(crate) fn random_id(base: &[u8; 32], dist: usize) -> NodeId {
        if dist == 0 {
            return NodeId::new(base);
        }
        let mut id = *base;
        let pos = 32 - (dist - 1) / 8 - 1;
        let bit = 1u8 << ((dist - 1) % 8);
        let tail: [u8; 32] = rand::random();
        id[pos] = (base[pos] ^ bit) & !(bit - 1) | tail[pos] & (bit - 1);
        id[pos + 1..].copy_from_slice(&tail[pos + 1..]);
        NodeId::new(&id)
    }

    pub(crate) fn int_ip(i: usize) -> IpAddr {
        Ipv4Addr::new(i as u8, 0, 2, i as u8).into()
    }

    pub(crate) fn node_at_distance(base: &[u8; 32], dist: usize, ip: IpAddr) -> TestNode {
        TestNode {
            id: random_id(base, dist),
            seq: 1,
            ip: Some(ip),
        }
    }

    /// `n` nodes at distance `dist` from `base`, with addresses from `int_ip(start)` on.
    pub(crate) fn nodes_at_distance_from(
        base: &[u8; 32],
        dist: usize,
        n: usize,
        start: usize,
    ) -> Vec<TestNode> {
        (0..n)
            .map(|i| node_at_distance(base, dist, int_ip(start + i)))
            .collect()
    }

    pub(crate) fn nodes_at_distance(base: &[u8; 32], dist: usize, n: usize) -> Vec<TestNode> {
        nodes_at_distance_from(base, dist, n, 1)
    }

    /// The defaults with a small ad cache, as the Go tests use.
    pub(crate) fn test_config() -> TopicConfig {
        TopicConfig {
            enabled: true,
            ad_cache_size: 20,
            ..Default::default()
        }
    }

    #[test]
    fn random_id_distance() {
        for dist in 0..=256 {
            let id = random_id(&TOPIC1, dist);
            assert_eq!(log_dist(&TOPIC1, &id), dist);
        }
        assert_eq!(bucket_index(&TOPIC1, &random_id(&TOPIC1, 256), 18), 0);
        assert_eq!(bucket_index(&TOPIC1, &random_id(&TOPIC1, 240), 18), 16);
        assert_eq!(bucket_index(&TOPIC1, &random_id(&TOPIC1, 5), 18), 17);
        assert_eq!(bucket_index(&TOPIC1, &random_id(&TOPIC1, 0), 18), 17);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enr::CombinedKey;

    #[test]
    fn enr_capability_key() {
        let key = CombinedKey::generate_secp256k1();
        let mut enr = Enr::builder().build(&key).unwrap();
        assert!(!supports_topic_discovery(&enr));

        enr.insert(TOPIC_DISCOVERY_ENR_KEY, &TOPIC_DISCOVERY_VERSION, &key)
            .unwrap();
        assert!(supports_topic_discovery(&enr));
        // The value is a plain RLP integer, as the Go implementation writes it.
        assert_eq!(enr.get_raw_rlp(TOPIC_DISCOVERY_ENR_KEY), Some(&[0x01][..]));

        enr.insert(TOPIC_DISCOVERY_ENR_KEY, &2u64, &key).unwrap();
        assert!(!supports_topic_discovery(&enr));
    }

    #[test]
    fn topic_id_sha256() {
        assert_eq!(
            hex::encode(topic_id("example-service")),
            "acac0a3cfc659b9392c42a643065965e71cae5229afeade460788cca53315f89"
        );
    }
}

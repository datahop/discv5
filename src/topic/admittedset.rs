//! The registrars a lookup queried, per `/24`, which caps the distinct registrars of one
//! prefix that the lookup queries.

use super::{
    TopicNode,
    netutil::{is_lan, prefix},
    params::{E, SEARCH_SUBNET_LIMIT},
};
use enr::NodeId;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// The prefix length of the search limits, for both address families.
pub(crate) const SEARCH_SUBNET: u8 = 24;

/// Remembers the nodes a lookup queried, by the `/24` of their endpoint address, for `E`
/// from the first query: a prefix with `SEARCH_SUBNET_LIMIT` node ids counted can only
/// have those queried again. A node whose query failed keeps its slot until it expires.
/// LAN and address-less nodes are not counted.
#[derive(Debug, Clone)]
pub struct AdmittedSet {
    ttl: Duration,
    /// The first query of every counted node, per prefix.
    subnets: HashMap<(bool, u128), HashMap<NodeId, Instant>>,
    /// When `expire` last ran.
    expired: Option<Instant>,
}

impl Default for AdmittedSet {
    fn default() -> Self {
        AdmittedSet {
            ttl: E,
            subnets: HashMap::new(),
            expired: None,
        }
    }
}

impl AdmittedSet {
    /// Reports whether `n` may be queried. It does not count `n`.
    pub fn allows<N: TopicNode>(&mut self, n: &N, now: Instant) -> bool {
        let Some(key) = key(n) else {
            return true;
        };
        self.expire(now);
        self.subnets
            .get(&key)
            .is_none_or(|ids| ids.contains_key(&n.node_id()) || ids.len() < SEARCH_SUBNET_LIMIT)
    }

    /// Counts `n` against its prefix. Counting a node again keeps its expiry.
    pub fn add<N: TopicNode>(&mut self, n: &N, now: Instant) {
        let Some(key) = key(n) else {
            return;
        };
        self.expire(now);
        self.subnets
            .entry(key)
            .or_default()
            .entry(n.node_id())
            .or_insert(now);
    }

    /// Drops the entries past the ttl, at most once a second.
    fn expire(&mut self, now: Instant) {
        if let Some(t) = self.expired
            && now.saturating_duration_since(t) < Duration::from_secs(1)
            && !self.subnets.is_empty()
        {
            return;
        }
        self.expired = Some(now);
        let ttl = self.ttl;
        self.subnets.retain(|_, ids| {
            ids.retain(|_, first| now.saturating_duration_since(*first) < ttl);
            !ids.is_empty()
        });
    }
}

fn key<N: TopicNode>(n: &N) -> Option<(bool, u128)> {
    n.ip()
        .filter(|ip| !is_lan(*ip))
        .map(|ip| prefix(ip, SEARCH_SUBNET))
}

#[cfg(test)]
mod tests {
    use super::{super::test_util::TestNode, *};
    use std::net::{IpAddr, Ipv4Addr};

    fn node_with_ip(ip: Option<IpAddr>) -> TestNode {
        TestNode {
            id: NodeId::random(),
            seq: 1,
            ip,
        }
    }

    /// `n` nodes in 203.0.113.0/24.
    fn subnet_nodes(n: usize) -> Vec<TestNode> {
        (0..n)
            .map(|i| node_with_ip(Some(Ipv4Addr::new(203, 0, 113, i as u8 + 1).into())))
            .collect()
    }

    // A /24 gets SEARCH_SUBNET_LIMIT distinct nodes admitted; after that only those are
    // allowed again. Checking a node does not count it.
    #[test]
    fn limit() {
        let now = Instant::now();
        let mut a = AdmittedSet::default();
        let nodes = subnet_nodes(SEARCH_SUBNET_LIMIT + 1);
        for (i, n) in nodes[..SEARCH_SUBNET_LIMIT].iter().enumerate() {
            assert!(a.allows(n, now), "node {i} refused below the limit");
            a.add(n, now);
        }
        assert!(!a.allows(&nodes[SEARCH_SUBNET_LIMIT], now));
        assert!(
            a.allows(&nodes[0], now),
            "counted node refused re-admission"
        );
        let other = node_with_ip(Some(Ipv4Addr::new(203, 0, 114, 1).into()));
        assert!(a.allows(&other, now), "node from another subnet refused");

        let mut a = AdmittedSet::default();
        for n in &nodes[..SEARCH_SUBNET_LIMIT - 1] {
            a.add(n, now);
        }
        let (x, y) = (&nodes[SEARCH_SUBNET_LIMIT - 1], &nodes[SEARCH_SUBNET_LIMIT]);
        assert!(a.allows(x, now), "node refused with one slot left");
        a.add(y, now);
        assert!(!a.allows(x, now), "checking a node reserved its slot");
        assert!(a.allows(y, now), "counted node refused");
    }

    // LAN and address-less nodes are never counted.
    #[test]
    fn exempt() {
        let now = Instant::now();
        let mut a = AdmittedSet::default();
        let mut nodes = Vec::new();
        for i in 0..=SEARCH_SUBNET_LIMIT {
            nodes.push(node_with_ip(Some(
                Ipv4Addr::new(10, 0, 0, i as u8 + 1).into(),
            )));
            nodes.push(node_with_ip(Some(Ipv4Addr::LOCALHOST.into())));
            nodes.push(node_with_ip(None));
        }
        for (i, n) in nodes.iter().enumerate() {
            assert!(a.allows(n, now), "exempt node {i} refused");
            a.add(n, now);
        }
        assert!(a.subnets.is_empty());
    }

    // A node's slot expires E after its first admission, however often it is admitted
    // again meanwhile.
    #[test]
    fn keeps_first_expiry() {
        let start = Instant::now();
        let mut a = AdmittedSet::default();
        let nodes = subnet_nodes(SEARCH_SUBNET_LIMIT + 1);
        for n in &nodes[..SEARCH_SUBNET_LIMIT] {
            a.add(n, start);
        }
        let now = start + E * 2 / 3;
        a.add(&nodes[0], now);
        assert!(!a.allows(&nodes[SEARCH_SUBNET_LIMIT], now));
        assert!(
            a.allows(&nodes[SEARCH_SUBNET_LIMIT], start + E),
            "re-admission extended the subnet's expiry"
        );
    }
}

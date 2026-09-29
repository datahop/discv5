//! Topic discovery (TopDisc) extension of discv5: the registrar side.
//!
//! A registrar keeps an ad cache of advertisements (`TopicTable`), admits advertisers
//! through a waiting-time function that penalises full caches, crowded topics and
//! clustered IP prefixes, and hands out HMAC-sealed tickets (`TicketSealer`) so an
//! advertiser can prove how long it has waited. The wire messages live in `rpc`, the
//! request handlers in `service::topic`.

use crate::Enr;

mod bound;
mod config;
mod iptree;
pub(crate) mod netutil;
mod table;
mod ticket;

pub use config::TopicConfig;
pub use table::{TOPIC_TABLE_WAIT_TIME_FLOOR, TopicTable};
pub use ticket::{TICKET_SIZE, Ticket, TicketError, TicketSealer};

/// A topic (service) identifier: a 32-byte hash.
pub type TopicId = [u8; 32];

/// ENR key signalling the topic discovery capability.
pub const TOPIC_DISCOVERY_ENR_KEY: &str = "ng";

/// Protocol version advertised in the `ng` entry and required from peers.
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
}

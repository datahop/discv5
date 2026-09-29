//! Remembers nodes for one ad lifetime, so that later search passes do not return, or
//! query, them again until the time has passed or their record has been updated.

use super::{TopicConfig, TopicNode};
use enr::NodeId;
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

const SEARCH_FILTER_LIMIT: usize = 50000;

struct Entry {
    seq: u64,
    expiry: Instant,
    /// Position in `order`.
    tick: u64,
}

pub struct SearchFilter {
    ttl: Duration,
    limit: usize,
    seen: HashMap<NodeId, Entry>,
    /// Oldest expiry first: the lifetime is constant and a refreshed entry moves back.
    order: BTreeMap<u64, NodeId>,
    next_tick: u64,
}

impl SearchFilter {
    /// Creates an empty filter that keeps entries for the ad lifetime.
    pub fn new(config: &TopicConfig) -> Self {
        SearchFilter {
            ttl: config.ad_lifetime,
            limit: SEARCH_FILTER_LIMIT,
            seen: HashMap::new(),
            order: BTreeMap::new(),
            next_tick: 0,
        }
    }

    /// Reports whether the node was added recently with the same or a newer record.
    pub fn seen<N: TopicNode>(&mut self, n: &N, now: Instant) -> bool {
        self.expire(now);
        self.seen
            .get(&n.node_id())
            .is_some_and(|e| n.seq() <= e.seq)
    }

    /// Records the node. Every id keeps a single entry.
    pub fn add<N: TopicNode>(&mut self, n: &N, now: Instant) {
        self.expire(now);
        let id = n.node_id();
        let tick = self.next_tick;
        self.next_tick += 1;
        let entry = Entry {
            seq: n.seq(),
            expiry: now + self.ttl,
            tick,
        };
        if let Some(old) = self.seen.insert(id, entry) {
            self.order.remove(&old.tick);
        } else {
            while self.seen.len() > self.limit {
                let Some((_, oldest)) = self.order.pop_first() else {
                    break;
                };
                self.seen.remove(&oldest);
            }
        }
        self.order.insert(tick, id);
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    fn expire(&mut self, now: Instant) {
        while let Some(entry) = self.order.first_entry() {
            if self.seen.get(entry.get()).is_some_and(|e| e.expiry > now) {
                break;
            }
            self.seen.remove(&entry.remove());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{super::test_util::*, *};

    fn filter(ttl: Duration) -> SearchFilter {
        SearchFilter::new(&TopicConfig {
            ad_lifetime: ttl,
            ..Default::default()
        })
    }

    fn node(seq: u64) -> TestNode {
        TestNode {
            id: NodeId::random(),
            seq,
            ip: Some(int_ip(1)),
        }
    }

    #[test]
    fn expiry() {
        let now = Instant::now();
        let minute = Duration::from_secs(60);
        let mut f = filter(minute);
        let n = node(1);

        assert!(!f.seen(&n, now));
        f.add(&n, now);
        assert!(f.seen(&n, now + minute - Duration::from_nanos(1)));
        assert!(!f.seen(&n, now + minute));
        assert!(f.is_empty());
        assert!(f.order.is_empty());
    }

    #[test]
    fn newer_record() {
        let now = Instant::now();
        let mut f = filter(Duration::from_secs(60));
        let old = node(1);
        let updated = TestNode {
            seq: 2,
            ..old.clone()
        };

        f.add(&old, now);
        assert!(!f.seen(&updated, now), "newer record filtered");
        let now = now + Duration::from_secs(30);
        f.add(&updated, now);
        assert!(f.seen(&old, now) && f.seen(&updated, now));
        assert_eq!(f.len(), 1);
        // The first expiry must not drop the entry refreshed by the second add.
        let now = now + Duration::from_secs(30);
        assert!(f.seen(&updated, now));
        let now = now + Duration::from_secs(30);
        assert!(!f.seen(&updated, now));
    }

    #[test]
    fn limit() {
        let now = Instant::now();
        let mut f = filter(Duration::from_secs(60));
        f.limit = 3;
        let nodes = [node(1), node(1), node(1), node(1)];
        for n in &nodes {
            f.add(n, now);
        }
        assert_eq!(f.len(), 3);
        assert_eq!(f.order.len(), 3);
        assert!(!f.seen(&nodes[0], now), "oldest entry not evicted");
        for n in &nodes[1..] {
            assert!(f.seen(n, now));
        }
    }
}

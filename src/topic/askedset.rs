//! A set of nodes kept for a fixed time from their first addition: the failed and
//! overloaded sets of the advertiser, the asked and failed sets of the discoverer.

use enr::NodeId;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Remembers nodes for `ttl` from the first add. Adding a node again does not extend
/// its expiry, it only records the time of the last add.
#[derive(Debug, Clone)]
pub struct AskedSet {
    ttl: Duration,
    at: HashMap<NodeId, Asked>,
    /// When `expire` last ran.
    expired: Option<Instant>,
}

#[derive(Debug, Clone)]
struct Asked {
    first: Instant,
    last: Instant,
}

impl AskedSet {
    pub fn new(ttl: Duration) -> Self {
        AskedSet {
            ttl,
            at: HashMap::new(),
            expired: None,
        }
    }

    /// Reports whether `id` was added within the ttl.
    pub fn has(&mut self, id: &NodeId, now: Instant) -> bool {
        self.expire(now);
        self.at.contains_key(id)
    }

    /// Records `id`. The first add sets the expiry.
    pub fn add(&mut self, id: NodeId, now: Instant) {
        self.expire(now);
        self.at
            .entry(id)
            .and_modify(|e| e.last = now)
            .or_insert(Asked {
                first: now,
                last: now,
            });
    }

    /// When `id` was last added, if it is in the set.
    pub fn last_asked(&mut self, id: &NodeId, now: Instant) -> Option<Instant> {
        self.expire(now);
        self.at.get(id).map(|e| e.last)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.at.len()
    }

    /// Drops the entries past the ttl, at most once a second.
    fn expire(&mut self, now: Instant) {
        if let Some(t) = self.expired
            && now.saturating_duration_since(t) < Duration::from_secs(1)
            && !self.at.is_empty()
        {
            return;
        }
        self.expired = Some(now);
        let ttl = self.ttl;
        self.at
            .retain(|_, e| now.saturating_duration_since(e.first) < ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // An entry expires a ttl after its first add, however often it is added again.
    #[test]
    fn keeps_first_expiry() {
        let start = Instant::now();
        let mut a = AskedSet::new(Duration::from_secs(60));
        let id = NodeId::random();
        a.add(id, start);
        let now = start + Duration::from_secs(40);
        a.add(id, now);
        assert!(a.has(&id, now));
        assert_eq!(a.last_asked(&id, now), Some(now));
        assert!(!a.has(&id, now + Duration::from_secs(21)));
        assert_eq!(a.last_asked(&id, now + Duration::from_secs(21)), None);
    }
}

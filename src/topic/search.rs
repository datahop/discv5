//! The searcher's table for one pass over a topic: the registrars to query, bucketed by
//! their distance to the topic hash, the choice of the next registrar, and the results.
//!
//! The search queries one bucket, the active one, and moves it by the number of ads the
//! registrars there return.

use super::{
    TopicConfig, TopicId, TopicNode,
    admittedset::{AdmittedSet, SEARCH_SUBNET},
    askedset::AskedSet,
    bucket_index,
    netutil::{DistinctNetSet, is_lan},
    params::{
        ASKED_FAILED_TTL, F_RETURN, SEARCH_AUX_RADIUS, SEARCH_MAX_JUMP, SEARCH_MIN_SAMPLES,
        SEARCH_TABLE_DEPTH, SEARCH_YIELD_FLOOR, SEARCH_YIELD_WINDOW,
    },
};
use crate::Enr;
use enr::NodeId;
use rand::seq::IteratorRandom;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Instant,
};
use tracing::debug;

// Candidates per /24 in one bucket. Theory: Service Tables.
const SEARCH_BUCKET_IP_LIMIT: usize = 2;

struct SearchBucket<N> {
    dist: u64,
    new: HashMap<NodeId, N>,
    /// Ad counts of the last `SEARCH_YIELD_WINDOW` replies.
    yields: VecDeque<usize>,
    ips: DistinctNetSet,
}

impl<N> SearchBucket<N> {
    fn contains(&self, id: &NodeId) -> bool {
        self.new.contains_key(id)
    }

    fn count(&self) -> usize {
        self.new.len()
    }
}

/// The records a lookup keeps across its passes: the registrars that answered, those
/// whose query failed, and those queried per `/24`.
#[derive(Debug, Clone)]
pub struct SearchSets {
    asked: AskedSet,
    failed: AskedSet,
    admitted: AdmittedSet,
}

impl Default for SearchSets {
    fn default() -> Self {
        SearchSets {
            asked: AskedSet::new(ASKED_FAILED_TTL),
            failed: AskedSet::new(ASKED_FAILED_TTL),
            admitted: AdmittedSet::default(),
        }
    }
}

impl SearchSets {
    #[cfg(test)]
    pub(crate) fn asked_len(&self) -> usize {
        self.asked.len()
    }
}

/// The state of one search pass for a single topic. When `is_done` reports true, the
/// state is abandoned and the next pass starts with a new one.
pub struct Search<N: TopicNode = Enr> {
    topic: TopicId,
    local_id: NodeId,
    config: TopicConfig,
    /// Ordered far to close.
    buckets: Vec<SearchBucket<N>>,
    /// The bucket the search queries.
    active: usize,
    sets: SearchSets,
    /// Asked nodes offered again. One is asked again per pass when nothing else is left.
    spare: HashMap<NodeId, N>,
    spare_used: bool,
    /// Nodes that answered or failed in this pass, not asked again in it.
    asked_now: HashSet<NodeId>,
    result_buffer: VecDeque<N>,
    result_seen: HashSet<NodeId>,
}

impl<N: TopicNode> Search<N> {
    pub fn new(topic: TopicId, local_id: NodeId, config: TopicConfig) -> Self {
        let buckets = (0..SEARCH_TABLE_DEPTH)
            .map(|i| SearchBucket {
                dist: (256 - i) as u64,
                new: HashMap::new(),
                yields: VecDeque::new(),
                ips: DistinctNetSet::new(SEARCH_SUBNET, SEARCH_BUCKET_IP_LIMIT),
            })
            .collect();
        Search {
            topic,
            local_id,
            config,
            buckets,
            active: 0,
            sets: SearchSets::default(),
            spare: HashMap::new(),
            spare_used: false,
            asked_now: HashSet::new(),
            result_buffer: VecDeque::new(),
            result_seen: HashSet::new(),
        }
    }

    /// The bucket the search queries.
    pub fn active_bucket(&self) -> usize {
        self.active
    }

    /// Sets the bucket to query first.
    pub fn set_active_bucket(&mut self, i: usize) {
        self.active = i.min(self.buckets.len() - 1);
    }

    /// Hands over the sets of the previous passes.
    pub fn set_sets(&mut self, sets: SearchSets) {
        self.sets = sets;
    }

    /// Returns the sets, for the next pass.
    pub fn into_sets(self) -> SearchSets {
        self.sets
    }

    /// Reports whether the pass is over. When nothing is left to ask, this moves the
    /// active bucket one step closer for the next pass, so it must be called once per
    /// such state.
    pub fn is_done(&mut self) -> bool {
        // Not done while there are unused results.
        if !self.result_buffer.is_empty() {
            return false;
        }
        if self.active_done() {
            return true;
        }
        if self.buckets.iter().any(|b| !b.new.is_empty()) {
            return false;
        }
        if !self.spare_used && !self.spare.is_empty() {
            return false;
        }
        if self.active < self.buckets.len() - 1 {
            self.active += 1;
        }
        true
    }

    /// Reports whether the active bucket is exhausted with enough replies to place the
    /// next pass.
    fn active_done(&self) -> bool {
        let b = &self.buckets[self.active];
        b.new.is_empty() && b.yields.len() >= SEARCH_MIN_SAMPLES
    }

    /// The distances within `SEARCH_AUX_RADIUS` of the active bucket at which the table
    /// has space available.
    pub fn buckets_with_free_space(&self) -> Vec<u64> {
        let last = self.buckets.len() - 1;
        let r = SEARCH_AUX_RADIUS;
        (self.active.saturating_sub(r)..=(self.active + r).min(last))
            .filter(|&i| self.buckets[i].count() < self.config.search_bucket_size)
            .map(|i| self.buckets[i].dist)
            .collect()
    }

    /// Adds potential registrars to the table. `src` is the node that returned them, or
    /// `None` when they come from the local routing table.
    pub fn add_nodes(
        &mut self,
        src: Option<&NodeId>,
        nodes: impl IntoIterator<Item = N>,
        now: Instant,
    ) {
        // One node per bucket from one response.
        let mut bucket_check = HashSet::new();

        for n in nodes {
            let id = n.node_id();
            if id == self.local_id || self.sets.failed.has(&id, now) {
                continue;
            }
            if !self.sets.admitted.allows(&n, now) {
                debug!(%id, reason = "subnetlimit", "Ignoring search node");
                continue;
            }
            if self.asked_now.contains(&id) {
                continue;
            }
            if self.sets.asked.has(&id, now) {
                self.spare.insert(id, n);
                continue;
            }
            let bi = self.bucket_index(&id);
            let b = &mut self.buckets[bi];
            if b.contains(&id) || b.count() >= self.config.search_bucket_size {
                continue;
            }
            if src.is_some() && bucket_check.contains(&bi) {
                debug!(%id, reason = "one-per-bucket-rule", "Ignoring search node");
                continue;
            }
            if let Some(ip) = n.ip()
                && !is_lan(ip)
                && !b.ips.add(ip)
            {
                debug!(%id, reason = "iplimit", "Ignoring search node");
                continue;
            }
            bucket_check.insert(bi);
            b.new.insert(id, n);
        }
    }

    /// Drops a node whose query failed, with its slot and subnet entry, and keeps it out
    /// for an ad lifetime across the passes that share the sets.
    pub fn handle_error_response(&mut self, id: &NodeId, now: Instant) {
        self.asked_now.insert(*id);
        self.sets.failed.add(*id, now);
        self.spare.remove(id);
        self.remove_node(id, false);
    }

    /// Drops a node from its bucket. With `keep_ip` its subnet stays counted against the
    /// bucket's IP limit for the pass.
    fn remove_node(&mut self, id: &NodeId, keep_ip: bool) {
        let bi = self.bucket_index(id);
        let b = &mut self.buckets[bi];
        if let Some(ip) = b.new.remove(id).and_then(|n| n.ip())
            && !keep_ip
            && !is_lan(ip)
        {
            b.ips.remove(ip);
        }
    }

    /// The node to send the next topic query to: an unasked node in the active bucket,
    /// or in the nearest bucket that has one, the farther side first, else one spare per
    /// pass, the one asked longest ago. The node is counted against its subnet; nodes
    /// whose subnet reached the limit since they were added are dropped.
    pub fn query_target(&mut self, now: Instant) -> Option<N> {
        if self.active_done() {
            return None;
        }
        let mut rng = rand::rng();
        for d in 0..self.buckets.len() {
            for i in [self.active.checked_sub(d), self.active.checked_add(d)] {
                let Some(i) = i.filter(|&i| i < self.buckets.len()) else {
                    continue;
                };
                while let Some(n) = self.buckets[i].new.values().choose(&mut rng).cloned() {
                    if self.sets.admitted.allows(&n, now) {
                        self.sets.admitted.add(&n, now);
                        return Some(n);
                    }
                    debug!(id = %n.node_id(), reason = "subnetlimit", "Dropping search node");
                    self.remove_node(&n.node_id(), false);
                    if self.active_done() {
                        return None;
                    }
                }
            }
        }
        if self.spare_used {
            return None;
        }
        let SearchSets {
            asked, admitted, ..
        } = &mut self.sets;
        self.spare.retain(|_, n| admitted.allows(n, now));
        let id = self
            .spare
            .keys()
            .min_by_key(|id| asked.last_asked(id, now))
            .copied()?;
        self.spare_used = true;
        let n = self.spare.remove(&id)?;
        admitted.add(&n, now);
        Some(n)
    }

    /// Records the ad count of a reply and moves the active bucket by the median of the
    /// recent replies from that bucket.
    fn observe(&mut self, bi: usize, ads: usize) {
        let last = self.buckets.len() - 1;
        let b = &mut self.buckets[bi];
        b.yields.push_back(ads);
        if b.yields.len() > SEARCH_YIELD_WINDOW {
            b.yields.pop_front();
        }
        if b.yields.len() < SEARCH_MIN_SAMPLES {
            return;
        }
        let mut sorted: Vec<usize> = b.yields.iter().copied().collect();
        sorted.sort_unstable();
        let (lower, upper) = (sorted[(sorted.len() - 1) / 2], sorted[sorted.len() / 2]);
        let floor = SEARCH_YIELD_FLOOR;
        self.active = if lower >= F_RETURN {
            bi.saturating_sub(1)
        } else if upper >= floor {
            bi
        } else if upper == 0 {
            last.min(bi + SEARCH_MAX_JUMP)
        } else {
            let jump = (floor as f64 / upper as f64).log2().ceil() as usize;
            last.min(bi + jump.min(SEARCH_MAX_JUMP))
        };
    }

    /// Adds the answer of `from` to a topic query. `from` keeps its subnet counted
    /// against its bucket's IP limit for the rest of the pass.
    pub fn add_query_results(
        &mut self,
        from: &N,
        results: impl IntoIterator<Item = N>,
        now: Instant,
    ) {
        let results: Vec<N> = results.into_iter().collect();
        let from_id = from.node_id();
        self.asked_now.insert(from_id);
        self.spare.remove(&from_id);
        let bi = self.bucket_index(&from_id);
        self.observe(bi, results.len());
        self.sets.asked.add(from_id, now);
        self.remove_node(&from_id, true);

        for n in results {
            let id = n.node_id();
            if id == self.local_id {
                continue;
            }
            if self.result_seen.insert(id) {
                debug!(topic = %hex::encode(self.topic), from = %from_id, %id, "Added topic search result");
                self.result_buffer.push_back(n);
            }
        }
    }

    /// The next result, if there is one.
    pub fn peek_result(&self) -> Option<&N> {
        self.result_buffer.front()
    }

    /// Removes the next result.
    pub fn pop_result(&mut self) -> Option<N> {
        self.result_buffer.pop_front()
    }

    fn bucket_index(&self, id: &NodeId) -> usize {
        bucket_index(&self.topic, id, self.buckets.len())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            params::{E, SEARCH_SUBNET_LIMIT},
            test_util::*,
        },
        *,
    };
    use std::{net::Ipv4Addr, time::Duration};

    const DEPTH: usize = 18;
    const SEC: Duration = Duration::from_secs(1);

    fn local_id() -> NodeId {
        NodeId::new(&[0xff; 32])
    }

    fn search(config: TopicConfig) -> Search<TestNode> {
        Search::new(TOPIC1, local_id(), config)
    }

    fn contains_all(s: &Search<TestNode>, bi: usize, nodes: &[TestNode]) -> bool {
        nodes.iter().all(|n| s.buckets[bi].contains(&n.id))
    }

    /// One node per distance, all in 203.0.113.0/24 from host address `start` on.
    fn subnet_nodes_at(start: usize, dists: &[usize]) -> Vec<TestNode> {
        dists
            .iter()
            .enumerate()
            .map(|(i, &d)| {
                node_at_distance(
                    &TOPIC1,
                    d,
                    Ipv4Addr::new(203, 0, 113, (start + i) as u8).into(),
                )
            })
            .collect()
    }

    // Nodes closer than the table depth go into the last bucket.
    #[test]
    fn bucket_distance_clamp() {
        let now = Instant::now();
        let mut s = search(test_config());
        let close5 = nodes_at_distance_from(&TOPIC1, 5, 1, 20);
        let close20 = nodes_at_distance_from(&TOPIC1, 20, 1, 30);
        s.add_nodes(None, close5.clone(), now);
        s.add_nodes(None, close20.clone(), now);
        assert_eq!(s.buckets.len(), DEPTH);
        assert!(contains_all(&s, DEPTH - 1, &close5));
        assert!(contains_all(&s, DEPTH - 1, &close20));
    }

    // An empty search is done, unasked nodes and buffered results keep it going, and it
    // is done again once both are used up.
    #[test]
    fn is_done_lifecycle() {
        let now = Instant::now();
        let mut s = search(test_config());
        assert!(s.is_done(), "empty search not done");
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 255, 2, 1), now);
        assert!(!s.is_done(), "done with unasked nodes");
        let results = nodes_at_distance_from(&TOPIC1, 200, 2, 100);
        while let Some(n) = s.query_target(now) {
            s.add_query_results(&n, results.clone(), now);
        }
        assert!(!s.is_done(), "done while results are buffered");
        while s.pop_result().is_some() {}
        assert!(s.is_done());
    }

    // The first target comes from the populated bucket nearest the active one, every
    // node is asked once, and there is no target once all are asked.
    #[test]
    fn query_target() {
        let now = Instant::now();
        let mut s = search(test_config());
        assert!(s.query_target(now).is_none());
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 253, 2, 1), now);
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 249, 2, 10), now);
        let n = s.query_target(now).unwrap();
        assert_eq!(s.bucket_index(&n.id), 3);

        let mut asked = HashSet::new();
        while let Some(n) = s.query_target(now) {
            assert!(asked.insert(n.id), "node picked twice");
            s.add_query_results(&n, [], now);
        }
        assert_eq!(asked.len(), 4);
        assert!(s.is_done());
    }

    // One node per bucket from one source in one call.
    #[test]
    fn add_nodes_one_per_bucket_rule() {
        let now = Instant::now();
        let mut s = search(test_config());
        let src = NodeId::new(&[0; 32]);
        let same_bucket = nodes_at_distance_from(&TOPIC1, 250, 2, 1);
        let bi = s.bucket_index(&same_bucket[0].id);
        s.add_nodes(Some(&src), same_bucket.clone(), now);
        assert_eq!(s.buckets[bi].count(), 1);

        // The rule holds per call, and not for the local routing table.
        s.add_nodes(Some(&src), same_bucket.clone(), now);
        assert_eq!(s.buckets[bi].count(), 2);
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 250, 2, 10), now);
        assert_eq!(s.buckets[bi].count(), 4);
    }

    // A node refused by the per-bucket subnet limit does not use up the bucket's one
    // node from that response.
    #[test]
    fn one_per_bucket_after_ip_limit() {
        let now = Instant::now();
        let mut s = search(test_config());
        let src = NodeId::new(&[0; 32]);
        let ip = Ipv4Addr::new(1, 0, 2, 1).into();
        let full: Vec<_> = (0..SEARCH_BUCKET_IP_LIMIT)
            .map(|_| node_at_distance(&TOPIC1, 256, ip))
            .collect();
        s.add_nodes(None, full, now);
        let refused = node_at_distance(&TOPIC1, 256, ip);
        let other = node_at_distance(&TOPIC1, 256, int_ip(9));
        s.add_nodes(Some(&src), [refused.clone(), other.clone()], now);
        assert!(!s.buckets[0].contains(&refused.id));
        assert!(s.buckets[0].contains(&other.id));
    }

    // A bucket takes at most SEARCH_BUCKET_IP_LIMIT nodes from one /24, which LAN
    // addresses are exempt from; another bucket has its own limit.
    #[test]
    fn ip_limit() {
        let now = Instant::now();
        let mut s = search(test_config());
        let same: Vec<_> = (0..=SEARCH_BUCKET_IP_LIMIT)
            .map(|i| node_at_distance(&TOPIC1, 256, Ipv4Addr::new(1, 0, 2, i as u8 + 1).into()))
            .collect();
        s.add_nodes(None, same.clone(), now);
        assert_eq!(s.buckets[0].count(), SEARCH_BUCKET_IP_LIMIT);
        let other = node_at_distance(&TOPIC1, 256, int_ip(9));
        s.add_nodes(None, [other.clone()], now);
        assert!(s.buckets[0].contains(&other.id));
        s.add_nodes(
            None,
            [node_at_distance(&TOPIC1, 255, same[0].ip.unwrap())],
            now,
        );
        assert_eq!(s.buckets[1].count(), 1);

        let lan = "127.0.0.1".parse().unwrap();
        for _ in 0..3 {
            s.add_nodes(None, [node_at_distance(&TOPIC1, 256, lan)], now);
        }
        assert_eq!(s.buckets[0].count(), SEARCH_BUCKET_IP_LIMIT + 4);
    }

    // A failed query drops the node and frees its slot and subnet entry.
    #[test]
    fn handle_error_response() {
        let now = Instant::now();
        let mut s = search(test_config());
        let far = vec![
            node_at_distance(&TOPIC1, 256, Ipv4Addr::new(1, 0, 2, 1).into()),
            node_at_distance(&TOPIC1, 256, Ipv4Addr::new(1, 0, 2, 2).into()),
        ];
        let mid = nodes_at_distance_from(&TOPIC1, 251, 1, 10);
        s.add_nodes(None, far.clone(), now);
        s.add_nodes(None, mid.clone(), now);

        s.handle_error_response(&far[0].id, now);
        assert!(!s.buckets[0].contains(&far[0].id));
        assert_eq!(s.buckets[0].count(), 1);

        let replacement = node_at_distance(&TOPIC1, 256, far[0].ip.unwrap());
        s.add_nodes(None, [replacement.clone()], now);
        assert!(s.buckets[0].contains(&replacement.id));

        let n = s.query_target(now).unwrap();
        assert!(s.buckets[0].contains(&n.id));

        s.handle_error_response(&far[1].id, now);
        s.handle_error_response(&replacement.id, now);
        let target = s
            .query_target(now)
            .expect("no target after bucket 0 failed out");
        assert!(s.buckets[5].contains(&target.id));

        s.handle_error_response(&mid[0].id, now);
        assert!(s.is_done());
    }

    // A node that answered keeps its subnet counted against the bucket's IP limit for
    // the pass: its slot is free, but not for a node from its own /24.
    #[test]
    fn replied_node_keeps_subnet() {
        let now = Instant::now();
        let mut s = search(test_config());
        let first: Vec<_> = (0..SEARCH_BUCKET_IP_LIMIT)
            .map(|i| node_at_distance(&TOPIC1, 256, Ipv4Addr::new(1, 0, 2, i as u8 + 1).into()))
            .collect();
        s.add_nodes(None, first.clone(), now);
        let mut n = None;
        for _ in &first {
            n = Some(reply(&mut s, 0, 0));
        }
        let n = n.unwrap();
        let same = node_at_distance(&TOPIC1, 256, n.ip.unwrap());
        s.add_nodes(Some(&n.id), [same.clone()], now);
        assert!(!s.buckets[0].contains(&same.id));
        let other = node_at_distance(&TOPIC1, 256, int_ip(9));
        s.add_nodes(Some(&n.id), [other.clone()], now);
        assert!(s.buckets[0].contains(&other.id));
    }

    // Results come out in the order they were received, once each, without the local
    // node.
    #[test]
    fn results_tracking() {
        let now = Instant::now();
        let mut s = search(test_config());
        let src = TestNode {
            id: NodeId::new(&[0; 32]),
            seq: 0,
            ip: None,
        };
        let nodes = nodes_at_distance(&[0; 32], 256, 10);
        let local = TestNode {
            id: local_id(),
            seq: 1,
            ip: None,
        };
        let mut results = vec![local];
        results.extend(nodes.clone());
        results.extend(nodes.clone());
        s.add_query_results(&src, results, now);

        for n in &nodes {
            assert_eq!(s.peek_result().map(|r| r.id), Some(n.id));
            assert_eq!(s.pop_result().map(|r| r.id), Some(n.id));
        }
        assert!(s.peek_result().is_none());
        assert!(s.pop_result().is_none());
    }

    // A node already returned in this pass is not returned again with a newer record.
    #[test]
    fn result_newer_record() {
        let now = Instant::now();
        let mut s = search(test_config());
        let regs = nodes_at_distance_from(&TOPIC1, 256, 2, 1);
        s.add_nodes(None, regs.clone(), now);
        let old = TestNode {
            id: NodeId::random(),
            seq: 1,
            ip: Some(int_ip(1)),
        };
        let updated = TestNode {
            seq: 2,
            ..old.clone()
        };
        s.add_query_results(&regs[0], [old], now);
        s.pop_result();
        s.add_query_results(&regs[1], [updated], now);
        assert!(s.peek_result().is_none());
    }

    // The buckets with room around the active one are listed.
    #[test]
    fn buckets_with_free_space() {
        let now = Instant::now();
        let config = test_config();
        let mut s = search(config.clone());
        assert_eq!(s.buckets_with_free_space(), [256, 255]);

        let full = nodes_at_distance_from(&TOPIC1, 256, config.search_bucket_size, 1);
        s.add_nodes(None, full, now);
        assert_eq!(s.buckets[0].count(), config.search_bucket_size);
        assert_eq!(s.buckets_with_free_space(), [255]);

        s.set_active_bucket(3);
        assert_eq!(s.buckets_with_free_space(), [254, 253, 252]);
        s.set_active_bucket(DEPTH - 1);
        assert_eq!(s.buckets_with_free_space(), [240, 239]);
    }

    /// An adaptive search with two candidates in every bucket the tests move through.
    fn adaptive_search() -> Search<TestNode> {
        let now = Instant::now();
        let mut s = search(test_config());
        for bi in 0..=12 {
            s.add_nodes(
                None,
                nodes_at_distance_from(&TOPIC1, 256 - bi, 2, 1 + 10 * bi),
                now,
            );
        }
        s
    }

    /// Answers a query, expected to go to bucket `bi`, with `n` ads.
    fn reply(s: &mut Search<TestNode>, bi: usize, n: usize) -> TestNode {
        reply_at(s, bi, n, Instant::now())
    }

    fn reply_at(s: &mut Search<TestNode>, bi: usize, n: usize, now: Instant) -> TestNode {
        let target = s
            .query_target(now)
            .unwrap_or_else(|| panic!("no target while bucket {bi} is expected"));
        assert_eq!(s.bucket_index(&target.id), bi, "query to the wrong bucket");
        let results = nodes_at_distance_from(&TOPIC1, 100, n, 200);
        s.add_query_results(&target, results, now);
        target
    }

    // The search moves toward the topic by the number of density doublings needed to
    // meet the floor; empty replies are the largest jump.
    #[test]
    fn adaptive_advance() {
        let mut s = adaptive_search();
        assert_eq!(s.active_bucket(), 0);
        reply(&mut s, 0, 1);
        assert_eq!(s.active_bucket(), 0, "moved on a single sample");
        reply(&mut s, 0, 1);
        assert_eq!(s.active_bucket(), 3);
        reply(&mut s, 3, 2);
        reply(&mut s, 3, 2);
        assert_eq!(s.active_bucket(), 5);
        reply(&mut s, 5, 0);
        reply(&mut s, 5, 0);
        assert_eq!(s.active_bucket(), 5 + SEARCH_MAX_JUMP);
    }

    // Full replies move the search one bucket farther, replies at the floor keep it.
    #[test]
    fn adaptive_retreat() {
        let limit = F_RETURN;
        let mut s = adaptive_search();
        s.set_active_bucket(6);
        reply(&mut s, 6, limit);
        reply(&mut s, 6, limit);
        assert_eq!(s.active_bucket(), 5);
        reply(&mut s, 5, 7);
        reply(&mut s, 5, 8);
        assert_eq!(s.active_bucket(), 5);
        s.set_active_bucket(8);
        reply(&mut s, 8, limit);
        reply(&mut s, 8, 2);
        assert_eq!(s.active_bucket(), 8);
    }

    // One reply cannot steer the search on its own.
    #[test]
    fn adaptive_liar() {
        let now = Instant::now();
        let limit = F_RETURN;
        let mut s = adaptive_search();
        s.set_active_bucket(4);
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 252, 2, 100), now);
        reply(&mut s, 4, limit);
        reply(&mut s, 4, 2);
        assert_eq!(s.active_bucket(), 4);
        reply(&mut s, 4, 2);
        assert_eq!(s.active_bucket(), 6);

        s.set_active_bucket(2);
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 254, 2, 100), now);
        for n in [0, 8, 8, 8] {
            reply(&mut s, 2, n);
            assert_eq!(s.active_bucket(), 2, "one empty reply advanced the search");
        }
    }

    // An active bucket without candidates: the nearest populated bucket is asked, the
    // farther side first, and aux nodes are requested around the active distance.
    #[test]
    fn adaptive_helper() {
        let now = Instant::now();
        let mut s = search(test_config());
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 254, 1, 1), now);
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 249, 1, 2), now);
        s.set_active_bucket(5);

        // Aux nodes are requested around the active bucket only.
        assert_eq!(s.buckets_with_free_space(), [252, 251, 250]);

        let n = s.query_target(now).unwrap();
        assert_eq!(s.bucket_index(&n.id), 7);
        s.add_query_results(&n, [], now);
        let n = s.query_target(now).unwrap();
        assert_eq!(s.bucket_index(&n.id), 2);
    }

    // An adaptive pass ends once the active bucket is exhausted, and not while results
    // are buffered. No query goes out once the bucket is exhausted.
    #[test]
    fn adaptive_is_done() {
        let now = Instant::now();
        let mut s = adaptive_search();
        s.set_active_bucket(3);
        assert!(!s.is_done());
        reply(&mut s, 3, 8);
        reply(&mut s, 3, 8);
        assert!(!s.is_done(), "done while results are buffered");
        assert!(
            s.query_target(now).is_none(),
            "query after the active bucket"
        );
        while s.pop_result().is_some() {}
        assert!(s.is_done());
        assert_eq!(s.active_bucket(), 3);

        // A bucket with a single candidate cannot decide on its own.
        let mut s = search(test_config());
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 251, 1, 1), now);
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 249, 2, 10), now);
        s.set_active_bucket(5);
        reply(&mut s, 5, 0);
        assert!(!s.is_done());
        let n = s.query_target(now).unwrap();
        assert_eq!(s.bucket_index(&n.id), 7);
    }

    // An asked node leaves the table and is not admitted again, in this pass or the
    // next one that gets the sets.
    #[test]
    fn adaptive_explores() {
        let now = Instant::now();
        let config = TopicConfig {
            search_bucket_size: 2,
            ..test_config()
        };
        let mut s = search(config.clone());
        let nodes = nodes_at_distance_from(&TOPIC1, 256, 2, 1);
        s.add_nodes(None, nodes.clone(), now);
        assert_ne!(s.buckets_with_free_space().first(), Some(&256));

        let asked = reply(&mut s, 0, 1);
        assert!(!s.buckets[0].contains(&asked.id));
        assert_eq!(s.buckets_with_free_space().first(), Some(&256));

        let other = nodes.iter().find(|n| n.id != asked.id).unwrap().clone();
        s.add_nodes(Some(&other.id), [asked.clone()], now);
        assert!(
            !s.buckets[0].contains(&asked.id),
            "asked node admitted again"
        );
        let fresh = nodes_at_distance_from(&TOPIC1, 256, 1, 50);
        s.add_nodes(Some(&other.id), fresh.clone(), now);
        assert!(s.buckets[0].contains(&fresh[0].id));

        let mut next = search(config);
        next.set_sets(s.into_sets());
        next.add_nodes(None, nodes, now);
        assert!(!next.buckets[0].contains(&asked.id));
        assert!(next.buckets[0].contains(&other.id));
    }

    // A search with nothing left to ask moves one bucket closer for the next pass, and
    // stops at the closest one.
    #[test]
    fn adaptive_walked_dry() {
        let now = Instant::now();
        let mut s = search(test_config());
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 256, 1, 1), now);
        reply(&mut s, 0, 0);
        assert!(s.is_done());
        assert_eq!(s.active_bucket(), 1);
        s.set_active_bucket(DEPTH + 5);
        assert_eq!(s.active_bucket(), DEPTH - 1);
        assert!(s.is_done());
        assert_eq!(s.active_bucket(), DEPTH - 1);
    }

    // A pass with every known node already asked asks one of them again, once, and
    // walks on from the nodes it brings.
    #[test]
    fn adaptive_spare() {
        let now = Instant::now();
        let mut s = search(test_config());
        let seeds = nodes_at_distance_from(&TOPIC1, 256, 2, 1);
        s.add_nodes(None, seeds.clone(), now);
        reply(&mut s, 0, 0);
        reply(&mut s, 0, 0);

        let mut next = search(test_config());
        next.set_sets(s.into_sets());
        next.add_nodes(None, seeds, now);
        assert!(!next.is_done(), "done with spare nodes still to ask");
        let helper = next.query_target(now).expect("no spare node offered");
        assert!(next.query_target(now).is_none(), "second spare offered");

        let fresh = nodes_at_distance_from(&TOPIC1, 256, 1, 50);
        next.add_nodes(Some(&helper.id), fresh.clone(), now);
        next.add_query_results(&helper, [], now);
        assert_eq!(next.query_target(now).map(|n| n.id), Some(fresh[0].id));
    }

    // An asked node is not asked again within the ttl of the asked set, also with a
    // newer record, and is let back in after it.
    #[test]
    fn asked_by_id() {
        let now = Instant::now();
        let mut s = search(test_config());
        let n = nodes_at_distance_from(&TOPIC1, 256, 1, 1).remove(0);
        s.add_nodes(None, [n.clone()], now);
        reply_at(&mut s, 0, 0, now);

        let mut next = search(test_config());
        next.set_sets(s.into_sets());
        let bumped = TestNode {
            seq: n.seq + 1,
            ..n.clone()
        };
        next.add_nodes(None, [bumped], now);
        assert!(
            !next.buckets[0].contains(&n.id),
            "asked node admitted again"
        );
        assert!(next.spare.contains_key(&n.id));

        let mut later = search(test_config());
        later.set_sets(next.into_sets());
        later.add_nodes(None, [n.clone()], now + ASKED_FAILED_TTL);
        assert!(later.buckets[0].contains(&n.id));
    }

    // A failed node is kept out of the table for the ttl of the failed set, in the same
    // pass and in the passes that share the sets, without stalling them.
    #[test]
    fn failed_excluded() {
        let now = Instant::now();
        let mut s = search(test_config());
        let nodes = nodes_at_distance_from(&TOPIC1, 256, 1, 1);
        s.add_nodes(None, nodes.clone(), now);
        let n = s.query_target(now).unwrap();
        s.handle_error_response(&n.id, now);
        s.add_nodes(None, nodes.clone(), now);
        assert!(!s.buckets[0].contains(&n.id) && s.spare.is_empty());

        let mut next = search(test_config());
        next.set_sets(s.into_sets());
        next.add_nodes(None, nodes.clone(), now);
        assert!(!next.buckets[0].contains(&n.id) && next.spare.is_empty());
        assert!(next.query_target(now).is_none());
        assert!(next.is_done());

        let mut last = search(test_config());
        last.set_sets(next.into_sets());
        last.add_nodes(None, nodes, now + ASKED_FAILED_TTL);
        assert!(last.buckets[0].contains(&n.id));
    }

    // A spare whose re-ask failed is not offered as a spare again.
    #[test]
    fn failed_spare() {
        let now = Instant::now();
        let mut s = search(test_config());
        let seeds = nodes_at_distance_from(&TOPIC1, 256, 2, 1);
        s.add_nodes(None, seeds.clone(), now);
        reply(&mut s, 0, 0);
        reply(&mut s, 0, 0);

        let mut second = search(test_config());
        second.set_sets(s.into_sets());
        second.add_nodes(None, seeds.clone(), now);
        let failed = second.query_target(now).expect("no spare node offered");
        second.handle_error_response(&failed.id, now);

        let mut third = search(test_config());
        third.set_sets(second.into_sets());
        third.add_nodes(None, seeds, now);
        assert!(!third.spare.contains_key(&failed.id));
        let n = third.query_target(now).expect("no spare node offered");
        assert_ne!(n.id, failed.id);
    }

    // A node that answered in this pass is not re-asked as a spare when it comes back
    // as an aux node.
    #[test]
    fn no_reask_in_pass() {
        let now = Instant::now();
        let mut s = search(test_config());
        s.add_nodes(None, nodes_at_distance_from(&TOPIC1, 256, 1, 1), now);
        let n = reply_at(&mut s, 0, 0, now);
        s.add_nodes(Some(&n.id), [n.clone()], now);
        assert!(s.spare.is_empty());
        assert!(s.query_target(now).is_none());
    }

    // The spare of a pass is the node asked longest ago, so the passes rotate through
    // the asked nodes.
    #[test]
    fn spare_rotation() {
        let mut now = Instant::now();
        let mut s = search(test_config());
        let seeds = nodes_at_distance_from(&TOPIC1, 256, 3, 1);
        s.add_nodes(None, seeds.clone(), now);
        for _ in &seeds {
            reply_at(&mut s, 0, 0, now);
            now += SEC;
        }
        let mut sets = s.into_sets();
        let mut asked = HashMap::new();
        for _ in &seeds {
            let mut next = search(test_config());
            next.set_sets(sets);
            next.add_nodes(None, seeds.clone(), now);
            while let Some(n) = next.query_target(now) {
                *asked.entry(n.id).or_insert(0) += 1;
                next.add_query_results(&n, [], now);
                now += SEC;
            }
            sets = next.into_sets();
        }
        for n in &seeds {
            assert_eq!(asked.get(&n.id), Some(&1), "each node asked once");
        }
    }

    /// A pass whose seeds were all asked by the previous pass: both are spares.
    fn spare_pass(now: Instant) -> (Search<TestNode>, Vec<TestNode>) {
        let mut s = search(test_config());
        let seeds = nodes_at_distance_from(&TOPIC1, 256, 2, 1);
        s.add_nodes(None, seeds.clone(), now);
        reply_at(&mut s, 0, 0, now);
        reply_at(&mut s, 0, 0, now);
        let mut next = search(test_config());
        next.set_active_bucket(s.active_bucket());
        next.set_sets(s.into_sets());
        next.add_nodes(None, seeds.clone(), now);
        assert_eq!(next.spare.len(), 2);
        (next, seeds)
    }

    // A spare whose asked entry expired after it answered in this pass is not asked
    // again in the pass, and neither is one asked from the table after its own entry
    // expired.
    #[test]
    fn spare_no_reask_after_expiry() {
        let start = Instant::now();
        let (mut next, seeds) = spare_pass(start);
        let mut asked = HashMap::new();
        let spare = next.query_target(start).unwrap();
        *asked.entry(spare.id).or_insert(0) += 1;
        next.add_query_results(&spare, [], start);

        let now = start + ASKED_FAILED_TTL * 2;
        next.add_nodes(None, seeds.clone(), now);
        while let Some(n) = next.query_target(now) {
            *asked.entry(n.id).or_insert(0) += 1;
            next.add_query_results(&n, [], now);
        }
        for n in &seeds {
            assert!(asked.get(&n.id).is_none_or(|&c| c <= 1), "asked twice");
        }
    }

    // A spare asked from the table in this pass leaves the spares, whether it answers
    // or fails: the pass does not wait on a spare it will never ask.
    #[test]
    fn spare_asked_from_table() {
        for fail in [false, true] {
            let start = Instant::now();
            let (mut next, seeds) = spare_pass(start);
            let t = next.query_target(start).unwrap();
            next.add_query_results(&t, [], start);
            let now = start + ASKED_FAILED_TTL * 2;
            next.add_nodes(None, seeds, now);
            let n = next.query_target(now).unwrap();
            assert!(next.buckets[0].contains(&n.id) && next.spare.contains_key(&n.id));
            if fail {
                next.handle_error_response(&n.id, now);
            } else {
                next.add_query_results(&n, [], now);
            }
            assert!(next.is_done(), "fail {fail}: pass not done");
            assert!(next.query_target(now).is_none(), "fail {fail}: asked again");
        }
    }

    // Table nodes of a /24 that sit in far buckets and are never queried do not take the
    // prefix's slots from a near node of the same /24.
    #[test]
    fn subnet_limit_unqueried_seeds() {
        const ACTIVE: usize = 14;
        let start = Instant::now();
        let seeds = subnet_nodes_at(1, &[256, 256, 255, 255, 254, 254, 253, 253, 252, 252]);
        let helper = node_at_distance(&TOPIC1, 243, int_ip(9));
        let mut sets = SearchSets::default();
        let mut now = start;
        let mut pass = 0;
        while now <= start + E * 2 {
            let mut s = search(test_config());
            s.set_active_bucket(ACTIVE);
            s.set_sets(sets);
            s.add_nodes(None, seeds.clone(), now);
            let near = subnet_nodes_at(100 + pass, &[256 - ACTIVE]).remove(0);
            s.add_nodes(Some(&helper.id), [near.clone()], now);
            let n = s.query_target(now).map(|n| n.id);
            assert_eq!(n, Some(near.id), "pass {pass}: near node not queried first");
            s.add_query_results(&near, [], now);
            sets = s.into_sets();
            now += E / 3;
            pass += 1;
        }
    }

    // A node whose /24 reaches the limit while it waits in the table is dropped instead
    // of queried, and frees its bucket slot and IP-limit entry.
    #[test]
    fn subnet_limit_drops_held_node() {
        let now = Instant::now();
        let mut s = search(TopicConfig {
            search_bucket_size: 1,
            ..test_config()
        });
        let held = subnet_nodes_at(1, &[253]).remove(0);
        s.add_nodes(None, [held.clone()], now);
        let bi = s.bucket_index(&held.id);
        let other = node_at_distance(&TOPIC1, 253, int_ip(9));
        s.add_nodes(None, [other.clone()], now);
        assert!(
            !s.buckets[bi].contains(&other.id),
            "full bucket took a node"
        );

        for n in subnet_nodes_at(10, &[200; SEARCH_SUBNET_LIMIT]) {
            s.sets.admitted.add(&n, now);
        }
        s.set_active_bucket(bi);
        assert!(s.query_target(now).is_none());
        assert_eq!(s.buckets[bi].count(), 0);
        assert_eq!(s.buckets[bi].ips.len(), 0);
        s.add_nodes(None, [other.clone()], now);
        assert!(s.buckets[bi].contains(&other.id));
    }

    // At most SEARCH_SUBNET_LIMIT distinct nodes of a /24 are queried across the passes
    // of a search, until the first of them expire.
    #[test]
    fn subnet_limit_across_passes() {
        fn pass(
            sets: SearchSets,
            nodes: &[TestNode],
            now: Instant,
        ) -> (Search<TestNode>, Vec<TestNode>) {
            let mut s = search(test_config());
            s.set_sets(sets);
            s.add_nodes(None, nodes.iter().cloned(), now);
            let mut got = Vec::new();
            while let Some(n) = s.query_target(now) {
                s.add_query_results(&n, [], now);
                got.push(n);
            }
            (s, got)
        }

        let start = Instant::now();
        let mut now = start;
        let mut sets = SearchSets::default();
        let mut queried = HashSet::new();
        let nodes = subnet_nodes_at(1, &[256; SEARCH_SUBNET_LIMIT + 2]);
        for i in (0..nodes.len()).step_by(2) {
            let (s, got) = pass(sets, &nodes[i..i + 2], now);
            queried.extend(got.iter().map(|n| n.id));
            if i >= SEARCH_SUBNET_LIMIT {
                assert!(s.buckets[0].count() == 0 && s.spare.is_empty());
            }
            sets = s.into_sets();
            now += SEC;
        }
        assert_eq!(queried.len(), SEARCH_SUBNET_LIMIT);

        let (_, got) = pass(sets, &nodes[SEARCH_SUBNET_LIMIT..], start + E);
        assert_eq!(got.len(), 2, "nodes queried after the first slots expired");

        // Nodes of one /24 that are not queried are not counted.
        let mut s = search(test_config());
        s.add_nodes(
            None,
            subnet_nodes_at(50, &[256, 256, 255, 255, 254, 254]),
            now,
        );
        assert_eq!(s.buckets.iter().map(|b| b.count()).sum::<usize>(), 6);
    }

    // A spare is counted against its subnet when it is re-asked, and dropped if its
    // subnet reached the limit meanwhile.
    #[test]
    fn subnet_limit_spares() {
        let start = Instant::now();
        let mut now = start;
        let spares = subnet_nodes_at(1, &[256, 255]);
        let mut asked = AskedSet::new(ASKED_FAILED_TTL);
        for n in &spares {
            asked.add(n.id, now);
            now += SEC;
        }
        let others = subnet_nodes_at(10, &[200; SEARCH_SUBNET_LIMIT]);
        let pass = || {
            let mut sets = SearchSets {
                asked: asked.clone(),
                ..Default::default()
            };
            for n in &others[..SEARCH_SUBNET_LIMIT - 1] {
                sets.admitted.add(n, now);
            }
            let mut s = search(test_config());
            s.set_sets(sets);
            s.add_nodes(None, spares.clone(), now);
            assert_eq!(s.spare.len(), spares.len());
            s
        };

        let mut s = pass();
        let n = s.query_target(now).map(|n| n.id);
        assert_eq!(n, Some(spares[0].id), "not the longest unasked spare");
        let last = &others[SEARCH_SUBNET_LIMIT - 1];
        assert!(
            !s.sets.admitted.allows(last, now),
            "re-asked spare not counted"
        );

        let mut s = pass();
        s.sets.admitted.add(last, now);
        assert!(s.query_target(now).is_none());
        assert!(s.spare.is_empty(), "spares over the subnet limit kept");
    }

    // Dropping the last candidate of an active bucket that already has its yield samples
    // ends the pass instead of querying a helper.
    #[test]
    fn subnet_limit_drop_ends_pass() {
        let now = Instant::now();
        let mut s = search(test_config());
        s.set_active_bucket(3);
        let regs = nodes_at_distance_from(&TOPIC1, 253, 2, 1);
        let helper = node_at_distance(&TOPIC1, 252, int_ip(9));
        s.add_nodes(None, regs.iter().cloned().chain([helper]), now);
        reply_at(&mut s, 3, SEARCH_YIELD_FLOOR, now);
        reply_at(&mut s, 3, SEARCH_YIELD_FLOOR, now);
        while s.pop_result().is_some() {}

        let counted = subnet_nodes_at(10, &[200; SEARCH_SUBNET_LIMIT]);
        for n in &counted[..SEARCH_SUBNET_LIMIT - 1] {
            s.sets.admitted.add(n, now);
        }
        let held = subnet_nodes_at(1, &[253]).remove(0);
        s.add_nodes(Some(&regs[0].id), [held.clone()], now);
        assert!(s.buckets[3].contains(&held.id));
        s.sets.admitted.add(&counted[SEARCH_SUBNET_LIMIT - 1], now);
        assert!(s.query_target(now).is_none());
        assert!(s.is_done());
    }
}

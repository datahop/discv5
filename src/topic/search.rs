//! The searcher's table for one pass over a topic: the registrars to query, bucketed by
//! their distance to the topic hash, the choice of the next registrar, and the results.
//!
//! The search queries one bucket, the active one, and moves it by the number of ads the
//! registrars there return.

use super::{
    SearchFilter, TopicConfig, TopicId, TopicNode, bucket_index,
    netutil::{DistinctNetSet, is_lan},
    params::{
        F_RETURN, SEARCH_AUX_RADIUS, SEARCH_MAX_JUMP, SEARCH_MIN_SAMPLES, SEARCH_TABLE_DEPTH,
        SEARCH_YIELD_FLOOR, SEARCH_YIELD_WINDOW,
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

// Registrars per /24 in one bucket.
const SEARCH_BUCKET_SUBNET: u8 = 24;
const SEARCH_BUCKET_IP_LIMIT: usize = 1;

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
    /// The nodes queried within the last ad lifetime.
    asked: SearchFilter,
    /// Nodes already asked. One is asked again per pass when nothing else is left.
    spare: HashMap<NodeId, N>,
    spare_used: bool,
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
                ips: DistinctNetSet::new(SEARCH_BUCKET_SUBNET, SEARCH_BUCKET_IP_LIMIT),
            })
            .collect();
        Search {
            topic,
            local_id,
            asked: SearchFilter::new(&config),
            config,
            buckets,
            active: 0,
            spare: HashMap::new(),
            spare_used: false,
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

    /// Hands over the record of queried nodes of the previous pass.
    pub fn set_asked_filter(&mut self, filter: SearchFilter) {
        self.asked = filter;
    }

    /// Returns the record of queried nodes, for the next pass.
    pub fn into_asked_filter(self) -> SearchFilter {
        self.asked
    }

    /// Reports whether the pass is over. When nothing is left to ask, this moves the
    /// active bucket one step closer for the next pass, so it must be called once per
    /// such state.
    pub fn is_done(&mut self) -> bool {
        // Not done while there are unused results.
        if !self.result_buffer.is_empty() {
            return false;
        }
        // A pass ends when the active bucket is exhausted and has enough replies to
        // place the next pass.
        let b = &self.buckets[self.active];
        if b.new.is_empty() && b.yields.len() >= SEARCH_MIN_SAMPLES {
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
            if id == self.local_id {
                continue;
            }
            let bi = self.bucket_index(&id);

            if self.asked.seen(&n, now) {
                self.spare.insert(id, n);
                continue;
            }
            let b = &mut self.buckets[bi];
            if b.contains(&id) || b.count() >= self.config.search_bucket_size {
                continue;
            }
            if src.is_some() && !bucket_check.insert(bi) {
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
            b.new.insert(id, n);
        }
    }

    /// Drops a node whose query failed, with its slot and subnet entry.
    pub fn handle_error_response(&mut self, id: &NodeId) {
        self.remove_node(id);
    }

    fn remove_node(&mut self, id: &NodeId) {
        let bi = self.bucket_index(id);
        let b = &mut self.buckets[bi];
        if let Some(ip) = b.new.remove(id).and_then(|n| n.ip())
            && !is_lan(ip)
        {
            b.ips.remove(ip);
        }
    }

    /// The node to send the next topic query to: an unasked node in the active bucket,
    /// or in the nearest bucket that has one, the farther side first.
    pub fn query_target(&mut self) -> Option<N> {
        let mut rng = rand::rng();
        for d in 0..self.buckets.len() {
            for i in [self.active.checked_sub(d), self.active.checked_add(d)] {
                let Some(b) = i.and_then(|i| self.buckets.get(i)) else {
                    continue;
                };
                if let Some(n) = b.new.values().choose(&mut rng) {
                    return Some(n.clone());
                }
            }
        }
        if !self.spare_used
            && let Some(id) = self.spare.keys().choose(&mut rng).copied()
        {
            self.spare_used = true;
            return self.spare.remove(&id);
        }
        None
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

    /// Adds the answer of `from` to a topic query.
    pub fn add_query_results(
        &mut self,
        from: &N,
        results: impl IntoIterator<Item = N>,
        now: Instant,
    ) {
        let results: Vec<N> = results.into_iter().collect();
        let from_id = from.node_id();
        let bi = self.bucket_index(&from_id);
        self.observe(bi, results.len());
        self.asked.add(from, now);
        self.remove_node(&from_id);

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
    use super::{super::test_util::*, *};

    const DEPTH: usize = 18;

    fn local_id() -> NodeId {
        NodeId::new(&[0xff; 32])
    }

    fn search(config: TopicConfig) -> Search<TestNode> {
        Search::new(TOPIC1, local_id(), config)
    }

    fn contains_all(s: &Search<TestNode>, bi: usize, nodes: &[TestNode]) -> bool {
        nodes.iter().all(|n| s.buckets[bi].contains(&n.id))
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

    // The per-bucket subnet limit, which LAN addresses are exempt from.
    #[test]
    fn ip_limit() {
        let now = Instant::now();
        let mut s = search(test_config());
        let ip = "192.0.2.1".parse().unwrap();
        s.add_nodes(None, [node_at_distance(&TOPIC1, 256, ip)], now);
        s.add_nodes(None, [node_at_distance(&TOPIC1, 256, ip)], now);
        assert_eq!(s.buckets[0].count(), 1);
        // The same subnet is free in another bucket.
        s.add_nodes(None, [node_at_distance(&TOPIC1, 255, ip)], now);
        assert_eq!(s.buckets[1].count(), 1);

        let lan = "127.0.0.1".parse().unwrap();
        for _ in 0..3 {
            s.add_nodes(None, [node_at_distance(&TOPIC1, 256, lan)], now);
        }
        assert_eq!(s.buckets[0].count(), 4);
    }

    // A failed query drops the node and frees its slot and subnet entry.
    #[test]
    fn handle_error_response() {
        let now = Instant::now();
        let mut s = search(test_config());
        let far = nodes_at_distance_from(&TOPIC1, 256, 2, 1);
        let mid = nodes_at_distance_from(&TOPIC1, 251, 1, 10);
        s.add_nodes(None, far.clone(), now);
        s.add_nodes(None, mid.clone(), now);

        s.handle_error_response(&far[0].id);
        assert!(!s.buckets[0].contains(&far[0].id));
        assert_eq!(s.buckets[0].count(), 1);

        let replacement = node_at_distance(&TOPIC1, 256, far[0].ip.unwrap());
        s.add_nodes(None, [replacement.clone()], now);
        assert!(s.buckets[0].contains(&replacement.id));

        let n = s.query_target().unwrap();
        assert!(s.buckets[0].contains(&n.id));

        s.handle_error_response(&far[1].id);
        s.handle_error_response(&replacement.id);
        let target = s
            .query_target()
            .expect("no target after bucket 0 failed out");
        assert!(s.buckets[5].contains(&target.id));

        s.handle_error_response(&mid[0].id);
        assert!(s.is_done());
    }

    // An answered node leaves the table and frees its subnet entry.
    #[test]
    fn answered_node_frees_ip() {
        let now = Instant::now();
        let mut s = search(test_config());
        let nodes = nodes_at_distance_from(&TOPIC1, 256, 1, 1);
        let n = nodes[0].clone();
        s.add_nodes(None, nodes, now);

        s.add_query_results(&n, [], now);
        assert!(!s.buckets[0].contains(&n.id));
        let replacement = node_at_distance(&TOPIC1, 256, n.ip.unwrap());
        s.add_nodes(None, [replacement.clone()], now);
        assert!(s.buckets[0].contains(&replacement.id));
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
        let target = s
            .query_target()
            .unwrap_or_else(|| panic!("no target while bucket {bi} is expected"));
        assert_eq!(s.bucket_index(&target.id), bi, "query to the wrong bucket");
        let results = nodes_at_distance_from(&TOPIC1, 100, n, 200);
        s.add_query_results(&target, results, Instant::now());
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

        let n = s.query_target().unwrap();
        assert_eq!(s.bucket_index(&n.id), 7);
        s.add_query_results(&n, [], now);
        let n = s.query_target().unwrap();
        assert_eq!(s.bucket_index(&n.id), 2);
    }

    // An adaptive pass ends once the active bucket is exhausted, and not while results
    // are buffered.
    #[test]
    fn adaptive_is_done() {
        let now = Instant::now();
        let mut s = adaptive_search();
        s.set_active_bucket(3);
        assert!(!s.is_done());
        reply(&mut s, 3, 8);
        reply(&mut s, 3, 8);
        assert!(!s.is_done(), "done while results are buffered");
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
        let n = s.query_target().unwrap();
        assert_eq!(s.bucket_index(&n.id), 7);
    }

    // An asked node leaves the table and is not admitted again, in this pass or the
    // next one that gets the asked filter.
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
        next.set_asked_filter(s.into_asked_filter());
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
        next.set_asked_filter(s.into_asked_filter());
        next.add_nodes(None, seeds, now);
        assert!(!next.is_done(), "done with spare nodes still to ask");
        let helper = next.query_target().expect("no spare node offered");
        assert!(next.query_target().is_none(), "second spare offered");

        let fresh = nodes_at_distance_from(&TOPIC1, 256, 1, 50);
        next.add_nodes(Some(&helper.id), fresh.clone(), now);
        next.add_query_results(&helper, [], now);
        assert_eq!(next.query_target().map(|n| n.id), Some(fresh[0].id));
    }

    // The asked filter lets a node back in after one ad lifetime, or with a newer
    // record.
    #[test]
    fn adaptive_asked_filter_expiry() {
        let now = Instant::now();
        let config = test_config();
        let mut s = search(config.clone());
        let seeds = nodes_at_distance_from(&TOPIC1, 256, 2, 1);
        s.add_nodes(None, seeds.clone(), now);
        let asked = reply(&mut s, 0, 0);
        let filter = s.into_asked_filter();

        let mut next = search(config.clone());
        next.set_asked_filter(filter);
        let newer = TestNode {
            seq: asked.seq + 1,
            ..asked.clone()
        };
        next.add_nodes(None, [asked.clone()], now);
        assert!(!next.buckets[0].contains(&asked.id));
        next.add_nodes(None, [newer], now);
        assert!(next.buckets[0].contains(&asked.id));

        let mut later = search(config.clone());
        later.set_asked_filter(next.into_asked_filter());
        later.add_nodes(None, [asked.clone()], Instant::now() + config.ad_lifetime);
        assert!(later.buckets[0].contains(&asked.id));
    }
}

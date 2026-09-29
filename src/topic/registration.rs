//! The advertiser's registration table for one topic: the registrars it tries to place
//! an ad on, bucketed by their distance to the topic hash, and the attempt queue.

use super::{
    TopicConfig, TopicId, TopicNode, bucket_index,
    netutil::{DistinctNetSet, is_lan, same_net},
};
use crate::Enr;
use enr::NodeId;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::IpAddr,
    time::{Duration, Instant},
};
use tracing::{debug, trace};

// Registrars per /24 in one bucket.
const REG_BUCKET_SUBNET: u8 = 24;
const REG_BUCKET_IP_LIMIT: usize = 1;

/// The state of a registration attempt on one registrar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegAttemptState {
    /// A replacement, not contacted yet.
    Standby = 0,
    /// A REGTOPIC is due at `next_time`.
    Waiting = 1,
    /// The registrar holds the ad until `next_time`.
    Registered = 2,
}

/// Where an attempt is relative to the attempt queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    Idle,
    Queued(u64),
    InFlight,
}

/// The registration process against a single registrar.
#[derive(Debug, Clone)]
pub struct RegAttempt<N> {
    pub state: RegAttemptState,
    /// `Waiting`: the time of the next request. `Registered`: the time the ad expires.
    pub next_time: Instant,
    /// The registrar.
    pub node: N,
    /// The ticket returned by the last request.
    pub ticket: Vec<u8>,
    total_wait_time: Duration,
    req_count: usize,
    /// Buckets this registrar already contributed a node to.
    filled_buckets: HashSet<usize>,
    slot: Slot,
}

impl<N> RegAttempt<N> {
    /// The time spent waiting on this registrar so far.
    pub fn total_wait_time(&self) -> Duration {
        self.total_wait_time
    }

    /// The number of requests sent to this registrar.
    pub fn req_count(&self) -> usize {
        self.req_count
    }
}

struct RegBucket<N> {
    dist: u64,
    att: HashMap<NodeId, RegAttempt<N>>,
    count: [usize; 3],
    ips: DistinctNetSet,
}

impl<N> RegBucket<N> {
    /// Moves the subnet accounting from `old` to `new` and reports whether `new` fits.
    /// Missing and LAN addresses are always accepted and not counted.
    fn update_ip(&mut self, old: Option<IpAddr>, new: Option<IpAddr>) -> bool {
        let old = old.filter(|ip| !is_lan(*ip));
        let new = new.filter(|ip| !is_lan(*ip));
        if let (Some(old), Some(new)) = (old, new)
            && same_net(REG_BUCKET_SUBNET, old, new)
        {
            return true;
        }
        if let Some(new) = new
            && !self.ips.add(new)
        {
            return false;
        }
        if let Some(old) = old {
            self.ips.remove(old);
        }
        true
    }
}

/// The state of registering for a single topic.
pub struct Registration<N: TopicNode = Enr> {
    topic: TopicId,
    local_id: NodeId,
    config: TopicConfig,
    /// Ordered far to close.
    buckets: Vec<RegBucket<N>>,
    /// Attempts in state `Waiting` or `Registered`, by `next_time`.
    queue: BTreeMap<(Instant, u64), NodeId>,
    next_seq: u64,
}

impl<N: TopicNode> Registration<N> {
    pub fn new(topic: TopicId, local_id: NodeId, config: TopicConfig) -> Self {
        let depth = config.reg_table_depth.clamp(1, 256);
        let buckets = (0..depth)
            .map(|i| RegBucket {
                // As in the Go table: the label runs opposite to the bucket order.
                dist: (256 - (depth - 1) + i) as u64,
                att: HashMap::new(),
                count: [0; 3],
                ips: DistinctNetSet::new(REG_BUCKET_SUBNET, REG_BUCKET_IP_LIMIT),
            })
            .collect();
        Registration {
            topic,
            local_id,
            config,
            buckets,
            queue: BTreeMap::new(),
            next_seq: 0,
        }
    }

    pub fn topic(&self) -> TopicId {
        self.topic
    }

    /// The number of nodes across all buckets.
    pub fn node_count(&self) -> usize {
        self.buckets
            .iter()
            .map(|b| b.count.iter().sum::<usize>())
            .sum()
    }

    /// The distances at which the table has space available.
    pub fn buckets_with_free_space(&self) -> Vec<u64> {
        self.buckets
            .iter()
            .filter(|b| {
                b.count[RegAttemptState::Standby as usize] < self.config.reg_bucket_standby_limit
            })
            .map(|b| b.dist)
            .collect()
    }

    /// The attempt on a registrar, if the table holds one.
    pub fn attempt(&self, id: &NodeId) -> Option<&RegAttempt<N>> {
        self.buckets[self.bucket_index(id)].att.get(id)
    }

    /// Notifies the table of found nodes. `src` is the registrar that returned them, or
    /// `None` when they come from the local routing table.
    pub fn add_nodes(
        &mut self,
        src: Option<&NodeId>,
        nodes: impl IntoIterator<Item = N>,
        now: Instant,
    ) {
        // The one-per-source-per-bucket rule is tracked on the attempt of the source.
        let src = src
            .filter(|id| self.attempt(id).is_some())
            .map(|id| (*id, self.bucket_index(id)));

        for n in nodes {
            let id = n.node_id();
            if id == self.local_id {
                continue;
            }
            let bi = self.bucket_index(&id);
            let standby_limit = self.config.reg_bucket_standby_limit;
            let b = &mut self.buckets[bi];

            if let Some(attempt) = b.att.get_mut(&id) {
                // Already scheduled: update the record if newer.
                if attempt.node.seq() >= n.seq() {
                    continue;
                }
                let old_ip = attempt.node.ip();
                if !b.update_ip(old_ip, n.ip()) {
                    debug!(%id, reason = "iplimit-on-record-update", "Dropping registration node");
                    self.remove_attempt(bi, &id, "iplimit-on-record-update");
                    self.refill_attempts(bi, now);
                    continue;
                }
                if let Some(attempt) = b.att.get_mut(&id) {
                    attempt.node = n;
                }
                continue;
            }

            if let Some((src_id, src_bi)) = src
                && self.buckets[src_bi]
                    .att
                    .get(&src_id)
                    .is_some_and(|a| a.filled_buckets.contains(&bi))
            {
                debug!(%id, reason = "source-already-in-bucket", "Ignoring registration node");
                continue;
            }

            let b = &mut self.buckets[bi];
            if b.count[RegAttemptState::Standby as usize] >= standby_limit {
                // There are enough replacements already.
                continue;
            }
            if !b.update_ip(None, n.ip()) {
                debug!(%id, reason = "iplimit", "Ignoring registration node");
                continue;
            }

            b.att.insert(
                id,
                RegAttempt {
                    state: RegAttemptState::Standby,
                    next_time: now,
                    node: n,
                    ticket: Vec::new(),
                    total_wait_time: Duration::ZERO,
                    req_count: 0,
                    filled_buckets: HashSet::new(),
                    slot: Slot::Idle,
                },
            );
            b.count[RegAttemptState::Standby as usize] += 1;
            if let Some((src_id, src_bi)) = src
                && let Some(src_att) = self.buckets[src_bi].att.get_mut(&src_id)
            {
                src_att.filled_buckets.insert(bi);
            }
            self.refill_attempts(bi, now);
        }
    }

    /// Drops the attempt on a node that left the routing table. An attempt with a
    /// request in flight is left for its response to remove.
    pub fn remove_node(&mut self, id: &NodeId, now: Instant) {
        let bi = self.bucket_index(id);
        match self.buckets[bi].att.get(id) {
            None => return,
            Some(att) if att.slot == Slot::InFlight => return,
            Some(_) => {}
        }
        self.remove_attempt(bi, id, "evicted");
        self.refill_attempts(bi, now);
    }

    /// The next time `update` should be called.
    pub fn next_update_time(&self) -> Option<Instant> {
        self.queue.first_key_value().map(|((time, _), _)| *time)
    }

    /// Processes the attempt queue: drops the attempts whose ad expired and returns the
    /// registrar of the next due attempt in state `Waiting`. The attempt stays at the
    /// head of the queue until `start_request`.
    pub fn update(&mut self, now: Instant) -> Option<NodeId> {
        while let Some((&(time, _), &id)) = self.queue.first_key_value() {
            if time > now {
                break;
            }
            let bi = self.bucket_index(&id);
            match self.buckets[bi].att.get(&id).map(|a| a.state) {
                Some(RegAttemptState::Waiting) => return Some(id),
                Some(RegAttemptState::Registered) => {
                    self.remove_attempt(bi, &id, "expired");
                    self.refill_attempts(bi, now);
                }
                Some(RegAttemptState::Standby) | None => {
                    debug_assert!(false, "bad attempt in the registration queue");
                    self.queue.pop_first();
                }
            }
        }
        None
    }

    /// Must be called when a request is sent for the attempt. Returns false, and
    /// changes nothing, when the attempt is not a queued `Waiting` one.
    pub fn start_request(&mut self, id: &NodeId) -> bool {
        let bi = self.bucket_index(id);
        let Some(att) = self.buckets[bi].att.get_mut(id) else {
            return false;
        };
        let Slot::Queued(seq) = att.slot else {
            return false;
        };
        if att.state != RegAttemptState::Waiting {
            return false;
        }
        self.queue.remove(&(att.next_time, seq));
        att.slot = Slot::InFlight;
        att.req_count += 1;
        true
    }

    /// Must be called when the registrar answers with a ticket and a waiting time.
    pub fn handle_ticket_response(
        &mut self,
        id: &NodeId,
        ticket: Vec<u8>,
        wait_time: Duration,
        now: Instant,
    ) {
        let Some(bi) = self.in_flight(id) else {
            return;
        };
        // One quote above the whole budget of the attempt.
        if wait_time > self.config.reg_attempt_timeout {
            self.remove_attempt(bi, id, "wtime-above-budget");
            return;
        }
        let timeout = self.config.reg_attempt_timeout;
        let Some(att) = self.buckets[bi].att.get_mut(id) else {
            return;
        };
        att.total_wait_time += wait_time;
        if att.total_wait_time > timeout {
            self.remove_attempt(bi, id, "wtime-too-high");
            return;
        }
        att.ticket = ticket;
        self.enqueue(bi, id, now + wait_time);
    }

    /// Must be called when the registrar confirms the registration.
    pub fn handle_registered(&mut self, id: &NodeId, ttl: Duration, now: Instant) {
        let Some(bi) = self.in_flight(id) else {
            return;
        };
        // A registrar cannot announce more than the local ad lifetime.
        let ttl = ttl.min(self.config.ad_lifetime);
        trace!(%id, ?ttl, "Topic registration successful");
        self.set_attempt_state(bi, id, RegAttemptState::Registered);
        self.enqueue(bi, id, now + ttl);
        self.refill_attempts(bi, now);
    }

    /// Must be called when a request fails.
    pub fn handle_error_response(&mut self, id: &NodeId, now: Instant) {
        let Some(bi) = self.in_flight(id) else {
            return;
        };
        self.remove_attempt(bi, id, "error");
        self.refill_attempts(bi, now);
    }

    /// The bucket of the attempt, when it has a request in flight.
    fn in_flight(&self, id: &NodeId) -> Option<usize> {
        let bi = self.bucket_index(id);
        let in_flight = self.buckets[bi]
            .att
            .get(id)
            .is_some_and(|a| a.slot == Slot::InFlight);
        if !in_flight {
            debug!(%id, "Response for a registration attempt that is not in flight");
        }
        in_flight.then_some(bi)
    }

    fn enqueue(&mut self, bi: usize, id: &NodeId, time: Instant) {
        let seq = self.next_seq;
        self.next_seq += 1;
        if let Some(att) = self.buckets[bi].att.get_mut(id) {
            att.next_time = time;
            att.slot = Slot::Queued(seq);
            self.queue.insert((time, seq), *id);
        }
    }

    fn set_attempt_state(&mut self, bi: usize, id: &NodeId, state: RegAttemptState) {
        let b = &mut self.buckets[bi];
        if let Some(att) = b.att.get_mut(id) {
            trace!(%id, ?state, prev = ?att.state, "Registration attempt state changed");
            b.count[att.state as usize] -= 1;
            b.count[state as usize] += 1;
            att.state = state;
        }
    }

    /// Promotes one node of the bucket from `Standby` to `Waiting` when the bucket has
    /// an active slot free. Must be called after every attempt state change.
    fn refill_attempts(&mut self, bi: usize, now: Instant) {
        let b = &self.buckets[bi];
        let active = b.count[RegAttemptState::Waiting as usize]
            + b.count[RegAttemptState::Registered as usize];
        if active >= self.config.reg_bucket_size {
            return;
        }
        let standby = b
            .att
            .iter()
            .find(|(_, a)| a.state == RegAttemptState::Standby)
            .map(|(id, _)| *id);
        if let Some(id) = standby {
            self.set_attempt_state(bi, &id, RegAttemptState::Waiting);
            self.enqueue(bi, &id, now);
        }
    }

    fn remove_attempt(&mut self, bi: usize, id: &NodeId, reason: &'static str) {
        let b = &mut self.buckets[bi];
        let Some(att) = b.att.remove(id) else {
            return;
        };
        trace!(%id, state = ?att.state, reason, "Removing registration attempt");
        if let Slot::Queued(seq) = att.slot {
            self.queue.remove(&(att.next_time, seq));
        }
        b.count[att.state as usize] -= 1;
        if let Some(ip) = att.node.ip() {
            b.ips.remove(ip);
        }
    }

    fn bucket_index(&self, id: &NodeId) -> usize {
        bucket_index(&self.topic, id, self.buckets.len())
    }
}

#[cfg(test)]
mod tests {
    use super::{super::test_util::*, *};
    use std::net::Ipv4Addr;

    fn registration(config: TopicConfig) -> Registration<TestNode> {
        Registration::new(TOPIC1, NodeId::new(&[0xff; 32]), config)
    }

    fn contains_all(r: &Registration<TestNode>, bi: usize, nodes: &[TestNode]) -> bool {
        nodes.iter().all(|n| r.buckets[bi].att.contains_key(&n.id))
    }

    fn bucket_count(r: &Registration<TestNode>, bi: usize) -> usize {
        r.buckets[bi].count.iter().sum()
    }

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        Ipv4Addr::new(a, b, c, d).into()
    }

    // Basic assignment of nodes into registration buckets.
    #[test]
    fn buckets() {
        let now = Instant::now();
        let mut r = registration(test_config());
        let far256 = nodes_at_distance_from(&TOPIC1, 256, 3, 1);
        let far255 = nodes_at_distance_from(&TOPIC1, 255, 3, 10);
        let close5 = nodes_at_distance_from(&TOPIC1, 5, 1, 20);
        let close20 = nodes_at_distance_from(&TOPIC1, 20, 1, 30);
        r.add_nodes(None, far256.clone(), now);
        r.add_nodes(None, far255.clone(), now);
        r.add_nodes(None, close5.clone(), now);
        r.add_nodes(None, close20.clone(), now);

        let last = r.buckets.len() - 1;
        assert_eq!(last, 17);
        assert!(contains_all(&r, 0, &far256));
        assert!(contains_all(&r, 1, &far255));
        assert!(contains_all(&r, last, &close5));
        assert!(contains_all(&r, last, &close20));
        assert_eq!(r.node_count(), 8);
    }

    // The local node never enters the table.
    #[test]
    fn skips_local_node() {
        let mut r = registration(test_config());
        let local = TestNode {
            id: NodeId::new(&[0xff; 32]),
            seq: 1,
            ip: Some(int_ip(1)),
        };
        r.add_nodes(None, [local], Instant::now());
        assert_eq!(r.node_count(), 0);
    }

    // The distance labels run opposite to the bucket order, as in the Go table.
    #[test]
    fn free_space_labels() {
        let now = Instant::now();
        let mut config = test_config();
        config.reg_bucket_standby_limit = 2;
        config.reg_bucket_size = 1;
        let mut r = registration(config);
        let dists = r.buckets_with_free_space();
        assert_eq!(dists, (239..=256).collect::<Vec<u64>>());

        // Bucket 0 holds the nodes at distance 256 and carries the label 239. One node
        // becomes active, two fill the standby list.
        for n in nodes_at_distance(&TOPIC1, 256, 3) {
            r.add_nodes(None, [n], now);
        }
        assert_eq!(bucket_count(&r, 0), 3);
        assert_eq!(
            r.buckets_with_free_space(),
            (240..=256).collect::<Vec<u64>>()
        );
    }

    // One node per source per bucket within a single call.
    #[test]
    fn one_per_bucket_check() {
        let now = Instant::now();
        let mut r = registration(test_config());
        let src = node_at_distance(&TOPIC1, 255, int_ip(1));
        r.add_nodes(None, [src.clone()], now);

        let bi = r.bucket_index(&random_id(&TOPIC1, 200));
        r.add_nodes(Some(&src.id), nodes_at_distance(&TOPIC1, 200, 10), now);
        assert_eq!(bucket_count(&r, bi), 1);
    }

    // One node per source per bucket across calls; other sources and the local table
    // are not limited by it.
    #[test]
    fn source_persistent_cap() {
        let now = Instant::now();
        let mut r = registration(test_config());
        let src1 = node_at_distance(&TOPIC1, 255, int_ip(1));
        let src2 = node_at_distance(&TOPIC1, 254, int_ip(2));
        r.add_nodes(None, [src1.clone(), src2.clone()], now);
        let bi = r.bucket_index(&random_id(&TOPIC1, 200));

        r.add_nodes(
            Some(&src1.id),
            [node_at_distance(&TOPIC1, 200, int_ip(3))],
            now,
        );
        assert_eq!(bucket_count(&r, bi), 1);
        r.add_nodes(
            Some(&src1.id),
            [node_at_distance(&TOPIC1, 200, int_ip(4))],
            now,
        );
        assert_eq!(bucket_count(&r, bi), 1);
        r.add_nodes(
            Some(&src2.id),
            [node_at_distance(&TOPIC1, 200, int_ip(5))],
            now,
        );
        assert_eq!(bucket_count(&r, bi), 2);
        r.add_nodes(None, [node_at_distance(&TOPIC1, 200, int_ip(6))], now);
        assert_eq!(bucket_count(&r, bi), 3);
    }

    // The per-source accounting goes away with the attempt of the source.
    #[test]
    fn source_cap_freed_on_eviction() {
        let now = Instant::now();
        let mut r = registration(test_config());
        let src = node_at_distance(&TOPIC1, 255, int_ip(1));
        r.add_nodes(None, [src.clone()], now);
        let bi = r.bucket_index(&random_id(&TOPIC1, 200));

        r.add_nodes(
            Some(&src.id),
            [node_at_distance(&TOPIC1, 200, int_ip(3))],
            now,
        );
        r.add_nodes(
            Some(&src.id),
            [node_at_distance(&TOPIC1, 200, int_ip(4))],
            now,
        );
        assert_eq!(bucket_count(&r, bi), 1);

        let src_bi = r.bucket_index(&src.id);
        r.remove_attempt(src_bi, &src.id, "test");
        r.add_nodes(None, [src.clone()], now);
        r.add_nodes(
            Some(&src.id),
            [node_at_distance(&TOPIC1, 200, int_ip(7))],
            now,
        );
        assert_eq!(bucket_count(&r, bi), 2);
    }

    // The per-bucket subnet limit.
    #[test]
    fn ip_check() {
        let now = Instant::now();
        let mut r = registration(test_config());
        let src1 = random_id(&TOPIC1, 255);
        let src2 = random_id(&TOPIC1, 255);
        r.add_nodes(
            Some(&src1),
            [node_at_distance(&TOPIC1, 200, ip(192, 0, 2, 1))],
            now,
        );
        r.add_nodes(
            Some(&src2),
            [node_at_distance(&TOPIC1, 200, ip(192, 0, 2, 2))],
            now,
        );
        assert_eq!(r.node_count(), 1);
    }

    // LAN addresses are not counted against the subnet limit.
    #[test]
    fn lan_addresses_exempt() {
        let now = Instant::now();
        let mut r = registration(test_config());
        let nodes: Vec<_> = (1..=4)
            .map(|i| node_at_distance(&TOPIC1, 256, ip(127, 0, 0, i)))
            .collect();
        r.add_nodes(None, nodes, now);
        assert_eq!(r.node_count(), 4);
        assert_eq!(r.buckets[0].ips.len(), 0);
    }

    // Attempts are created for found nodes.
    #[test]
    fn requests() {
        let now = Instant::now();
        let config = test_config();
        let mut r = registration(config.clone());
        assert!(r.update(now).is_none());
        assert!(r.next_update_time().is_none());

        for dist in 50..256 {
            r.add_nodes(None, nodes_at_distance(&TOPIC1, dist, 5), now);
        }
        let req = r.update(now).expect("no request scheduled");
        assert_eq!(r.update(now), Some(req), "top request changed");

        assert!(r.start_request(&req));
        assert!(!r.start_request(&req), "request started twice");
        assert_ne!(r.update(now), Some(req), "top request not removed");
        assert_eq!(r.attempt(&req).unwrap().req_count(), 1);
        r.handle_registered(&req, config.ad_lifetime, now);
        assert_eq!(r.attempt(&req).unwrap().state, RegAttemptState::Registered);
    }

    // The bucket keeps `reg_bucket_size` active attempts and promotes a standby node
    // when one ends.
    #[test]
    fn refill_from_standby() {
        let now = Instant::now();
        let mut config = test_config();
        config.reg_bucket_size = 2;
        let mut r = registration(config);
        r.add_nodes(None, nodes_at_distance(&TOPIC1, 256, 5), now);
        let count = |r: &Registration<TestNode>| r.buckets[0].count;
        assert_eq!(count(&r), [3, 2, 0]);

        let first = r.update(now).unwrap();
        assert!(r.start_request(&first));
        r.handle_error_response(&first, now);
        assert!(r.attempt(&first).is_none());
        assert_eq!(count(&r), [2, 2, 0]);

        let second = r.update(now).unwrap();
        assert!(r.start_request(&second));
        r.handle_registered(&second, Duration::from_secs(10), now);
        assert_eq!(count(&r), [2, 1, 1]);
    }

    // Attempts expire with the ad, and the node can be added again.
    #[test]
    fn expiry() {
        let now = Instant::now();
        let mut config = test_config();
        config.ad_lifetime = Duration::from_secs(20);
        let mut r = registration(config.clone());
        let node = nodes_at_distance(&TOPIC1, 30, 1);
        r.add_nodes(None, node.clone(), now);

        let att = r.update(now).expect("no request scheduled");
        assert_eq!(r.attempt(&att).unwrap().state, RegAttemptState::Waiting);
        assert!(r.start_request(&att));
        // A longer lifetime than the local one is cut.
        r.handle_registered(&att, config.ad_lifetime * 3, now);
        assert_eq!(r.next_update_time(), Some(now + config.ad_lifetime));
        assert!(
            r.update(now + config.ad_lifetime - Duration::from_nanos(1))
                .is_none()
        );
        assert!(r.attempt(&att).is_some());

        let now = now + config.ad_lifetime;
        assert!(r.update(now).is_none());
        assert!(r.queue.is_empty(), "attempt not removed");
        assert_eq!(r.node_count(), 0);

        let now = now + Duration::from_secs(1);
        r.add_nodes(None, node, now);
        let att = r.update(now).expect("no request scheduled");
        assert_eq!(r.attempt(&att).unwrap().state, RegAttemptState::Waiting);
    }

    // A ticket schedules the next request after the waiting time.
    #[test]
    fn ticket_response() {
        let now = Instant::now();
        let mut r = registration(test_config());
        r.add_nodes(None, nodes_at_distance(&TOPIC1, 30, 1), now);
        let att = r.update(now).unwrap();
        assert!(r.start_request(&att));
        r.handle_ticket_response(&att, b"ticket".to_vec(), Duration::from_secs(60), now);

        let attempt = r.attempt(&att).unwrap();
        assert_eq!(attempt.state, RegAttemptState::Waiting);
        assert_eq!(attempt.ticket, b"ticket");
        assert_eq!(attempt.total_wait_time(), Duration::from_secs(60));
        assert_eq!(r.next_update_time(), Some(now + Duration::from_secs(60)));
        assert!(r.update(now + Duration::from_secs(59)).is_none());
        assert_eq!(r.update(now + Duration::from_secs(60)), Some(att));
    }

    // The attempt ends when the waiting times add up to more than the timeout.
    #[test]
    fn ticket_response_total_above_budget() {
        let mut now = Instant::now();
        let mut config = test_config();
        config.reg_attempt_timeout = Duration::from_secs(100);
        let mut r = registration(config);
        r.add_nodes(None, nodes_at_distance(&TOPIC1, 30, 1), now);
        let att = r.update(now).unwrap();
        for _ in 0..2 {
            assert!(r.start_request(&att));
            r.handle_ticket_response(&att, b"t".to_vec(), Duration::from_secs(50), now);
            now += Duration::from_secs(50);
            assert_eq!(r.update(now), Some(att));
        }
        assert!(r.start_request(&att));
        r.handle_ticket_response(&att, b"t".to_vec(), Duration::from_secs(1), now);
        assert!(r.attempt(&att).is_none());
        assert_eq!(r.node_count(), 0);
    }

    // `remove_node` drops a parked attempt and leaves one with a request in flight.
    #[test]
    fn remove_node() {
        let now = Instant::now();
        let config = test_config();
        let mut r = registration(config.clone());

        let waiting = nodes_at_distance(&TOPIC1, 30, 1);
        r.add_nodes(None, waiting.clone(), now);
        assert!(r.update(now).is_some());
        r.remove_node(&waiting[0].id, now);
        assert!(r.attempt(&waiting[0].id).is_none());
        assert!(r.queue.is_empty());

        let registered = nodes_at_distance(&TOPIC1, 40, 1);
        r.add_nodes(None, registered.clone(), now);
        let att = r.update(now).unwrap();
        assert!(r.start_request(&att));
        r.handle_registered(&att, config.ad_lifetime, now);
        r.remove_node(&registered[0].id, now);
        assert!(r.attempt(&registered[0].id).is_none());

        r.remove_node(&NodeId::new(&[42; 32]), now);

        let inflight = nodes_at_distance(&TOPIC1, 50, 1);
        r.add_nodes(None, inflight.clone(), now);
        let att = r.update(now).unwrap();
        assert!(r.start_request(&att));
        r.remove_node(&inflight[0].id, now);
        assert!(r.attempt(&inflight[0].id).is_some());
    }

    // A quote above the attempt timeout drops the attempt.
    #[test]
    fn ticket_response_drop_above_budget() {
        let now = Instant::now();
        let mut config = test_config();
        config.ad_lifetime = Duration::from_secs(15 * 60);
        config.reg_attempt_timeout = Duration::from_secs(22 * 60 + 30);
        let mut r = registration(config);
        let node = nodes_at_distance(&TOPIC1, 30, 1);
        r.add_nodes(None, node.clone(), now);
        let att = r.update(now).unwrap();
        assert!(r.start_request(&att));
        r.handle_ticket_response(
            &att,
            b"t".to_vec(),
            Duration::from_secs(49 * 24 * 3600),
            now,
        );
        assert!(r.attempt(&node[0].id).is_none());
    }

    // A response for an attempt that is not in flight changes nothing.
    #[test]
    fn response_without_request() {
        let now = Instant::now();
        let mut r = registration(test_config());
        let node = nodes_at_distance(&TOPIC1, 30, 1);
        r.add_nodes(None, node.clone(), now);
        r.handle_registered(&node[0].id, Duration::from_secs(1), now);
        r.handle_ticket_response(&node[0].id, vec![1], Duration::from_secs(1), now);
        r.handle_error_response(&node[0].id, now);
        let attempt = r.attempt(&node[0].id).unwrap();
        assert_eq!(attempt.state, RegAttemptState::Waiting);
        assert!(attempt.ticket.is_empty());
        assert_eq!(r.queue.len(), 1);
    }

    // A newer record of a scheduled node keeps the subnet accounting consistent.
    #[test]
    fn record_update() {
        struct Case {
            name: &'static str,
            limit: usize,
            setup: Vec<IpAddr>,
            move_ip: IpAddr,
            same_seq: bool,
            want_kept: bool,
            probes: Vec<(IpAddr, bool)>,
        }
        let cases = [
            Case {
                name: "stale-seq-ignored",
                limit: 1,
                setup: vec![ip(3, 0, 2, 1)],
                move_ip: ip(4, 0, 2, 1),
                same_seq: true,
                want_kept: true,
                probes: vec![(ip(3, 0, 2, 9), false), (ip(4, 0, 2, 9), true)],
            },
            Case {
                name: "into-empty-subnet",
                limit: 1,
                setup: vec![ip(3, 0, 2, 1)],
                move_ip: ip(6, 0, 2, 1),
                same_seq: false,
                want_kept: true,
                probes: vec![(ip(3, 0, 2, 9), true), (ip(6, 0, 2, 9), false)],
            },
            Case {
                name: "into-full-subnet-limit-2",
                limit: 2,
                setup: vec![
                    ip(3, 0, 2, 1),
                    ip(3, 0, 2, 2),
                    ip(4, 0, 2, 1),
                    ip(4, 0, 2, 2),
                ],
                move_ip: ip(4, 0, 2, 99),
                same_seq: false,
                want_kept: false,
                probes: vec![(ip(3, 0, 2, 3), true), (ip(3, 0, 2, 4), false)],
            },
            Case {
                name: "within-same-subnet",
                limit: 1,
                setup: vec![ip(3, 0, 2, 1)],
                move_ip: ip(3, 0, 2, 250),
                same_seq: false,
                want_kept: true,
                probes: vec![],
            },
        ];
        let now = Instant::now();
        for c in cases {
            let mut r = registration(test_config());
            let bi = r.bucket_index(&random_id(&TOPIC1, 200));
            r.buckets[bi].ips.limit = c.limit;

            let nodes: Vec<_> = c
                .setup
                .iter()
                .map(|ip| node_at_distance(&TOPIC1, 200, *ip))
                .collect();
            r.add_nodes(None, nodes.clone(), now);
            for n in &nodes {
                assert!(r.buckets[bi].att.contains_key(&n.id), "{}: setup", c.name);
            }

            let update = TestNode {
                id: nodes[0].id,
                seq: nodes[0].seq + u64::from(!c.same_seq),
                ip: Some(c.move_ip),
            };
            r.add_nodes(None, [update], now);

            let att = r.buckets[bi].att.get(&nodes[0].id);
            assert_eq!(att.is_some(), c.want_kept, "{}: kept", c.name);
            if let Some(att) = att {
                let want = if c.same_seq { c.setup[0] } else { c.move_ip };
                assert_eq!(att.node.ip, Some(want), "{}: endpoint", c.name);
            }
            for n in &nodes[1..] {
                assert!(r.buckets[bi].att.contains_key(&n.id), "{}: others", c.name);
            }
            for (probe_ip, admit) in c.probes {
                let probe = node_at_distance(&TOPIC1, 200, probe_ip);
                r.add_nodes(None, [probe.clone()], now);
                assert_eq!(
                    r.buckets[bi].att.contains_key(&probe.id),
                    admit,
                    "{}: probe {probe_ip}",
                    c.name
                );
            }
        }
    }
}

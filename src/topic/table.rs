use super::{
    TopicConfig, TopicId,
    bound::LowerBound,
    iptree::IpTree,
    netutil,
    params::{P_OCC, W_BASE, W_MIN},
};
use crate::Enr;
use enr::NodeId;
use rand::seq::IteratorRandom;
use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    time::{Duration, Instant},
};

/// The ad cache: advertisements admitted by this registrar, per topic, with the state
/// of the waiting-time function (IP trees and lower bounds).
pub struct TopicTable {
    /// All ads in expiry order; the lifetime is constant, so insertion order is expiry order.
    all: VecDeque<AdKey>,
    topics: HashMap<TopicId, HashMap<NodeId, Ad>>,
    wt: WaitTimeState,
    config: TopicConfig,
}

#[derive(Clone)]
struct AdKey {
    node_id: NodeId,
    topic: TopicId,
    expiry: Instant,
}

/// One held advertisement.
#[derive(Debug, Clone)]
pub struct Ad {
    pub node: Enr,
    pub expiry: Instant,
}

struct WaitTimeState {
    ipv4: IpTree,
    ipv6: IpTree,
    topic_bounds: HashMap<TopicId, LowerBound>,
}

/// The freshly computed, un-floored waiting-time components of one request, kept so
/// the lower bounds can be recorded if a ticket is issued.
struct WaitTimeComponents {
    topic: TopicId,
    service_secs: f64,
    ip_secs: f64,
    /// Longest-prefix-match nodes (address, depth) in the IPv4 and IPv6 trees.
    ip_nodes: [Option<(IpAddr, usize)>; 2],
}

impl TopicTable {
    pub fn new(config: TopicConfig) -> Self {
        TopicTable {
            all: VecDeque::new(),
            topics: HashMap::new(),
            wt: WaitTimeState {
                ipv4: IpTree::new(32),
                ipv6: IpTree::new(128),
                topic_bounds: HashMap::new(),
            },
            config,
        }
    }

    pub fn config(&self) -> &TopicConfig {
        &self.config
    }

    pub fn ad_lifetime(&self) -> Duration {
        self.config.ad_lifetime
    }

    /// All nodes registered for a topic whose ad has not expired at `now`.
    pub fn nodes(&self, topic: &TopicId, now: Instant) -> Vec<Enr> {
        self.topics
            .get(topic)
            .map(|ads| {
                ads.values()
                    .filter(|ad| ad.expiry > now)
                    .map(|ad| ad.node.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every ad held for a topic, including any past expiry that `expire` has not
    /// swept yet.
    pub fn ads(&self, topic: &TopicId) -> Vec<Ad> {
        self.topics
            .get(topic)
            .map(|ads| ads.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Up to `n` random nodes registered for a topic that pass `check`.
    pub fn random_nodes(
        &self,
        topic: &TopicId,
        n: usize,
        check: impl Fn(&Enr) -> bool,
    ) -> Vec<Enr> {
        let Some(ads) = self.topics.get(topic) else {
            return Vec::new();
        };
        if n == 0 {
            return Vec::new();
        }
        ads.values()
            .filter(|ad| check(&ad.node))
            .sample(&mut rand::rng(), n)
            .into_iter()
            .map(|ad| ad.node.clone())
            .collect()
    }

    pub fn is_registered(&self, node_id: &NodeId, topic: &TopicId) -> bool {
        self.topics
            .get(topic)
            .is_some_and(|ads| ads.contains_key(node_id))
    }

    pub fn topic_size(&self, topic: &TopicId) -> usize {
        self.topics.get(topic).map_or(0, HashMap::len)
    }

    /// Number of ads held and the cache capacity.
    pub fn occupancy(&self) -> (usize, usize) {
        (self.all.len(), self.config.ad_cache_size)
    }

    /// When the next ad expires, if any is held.
    pub fn next_expiry(&self) -> Option<Instant> {
        self.all.front().map(|ad| ad.expiry)
    }

    /// Removes the ads that have expired at `now`.
    pub fn expire(&mut self, now: Instant) {
        while let Some(front) = self.all.front() {
            if front.expiry > now {
                break;
            }
            let key = self.all.pop_front().expect("front exists");
            self.remove_ad(&key);
        }
    }

    /// Adds an ad without admission control. Fails when the cache is full.
    pub fn add(&mut self, node: &Enr, topic: TopicId, now: Instant) -> bool {
        if self.all.len() < self.config.ad_cache_size {
            self.add_ad(node, topic, now);
            true
        } else {
            false
        }
    }

    /// Removes every ad advertised by a node, across all topics.
    pub fn remove_node(&mut self, node_id: &NodeId) {
        let keys: Vec<AdKey> = self
            .all
            .iter()
            .filter(|ad| ad.node_id == *node_id)
            .cloned()
            .collect();
        if keys.is_empty() {
            return;
        }
        self.all.retain(|ad| ad.node_id != *node_id);
        for key in &keys {
            self.remove_ad(key);
        }
    }

    /// The time a node must have waited to be admitted for a topic as of `now`.
    pub fn wait_time(&self, node: &Enr, topic: &TopicId, now: Instant) -> Duration {
        self.wait_time_components(node, topic, now).0
    }

    /// Attempts to register `node` for `topic` after it has waited `waited`. Returns
    /// zero when admitted, otherwise the further time to wait before retrying. Ads
    /// expired at `now` are removed first.
    pub fn register(
        &mut self,
        node: &Enr,
        topic: TopicId,
        waited: Duration,
        now: Instant,
    ) -> Duration {
        self.expire(now);
        if self.is_registered(&node.node_id(), &topic) {
            return Duration::ZERO;
        }

        // With a full cache the Go waiting-time function degenerates (division by zero
        // occupancy) and its result never gates admission; the node is told to come back
        // when the next ad expires.
        let full = self.all.len() >= self.config.ad_cache_size;
        if !full {
            let (required, components) = self.wait_time_components(node, &topic, now);
            if waited < required {
                let remaining = required - waited;
                if remaining > W_MIN {
                    // A ticket is being issued: record the lower bounds so the node cannot
                    // get a cheaper quote by asking again.
                    self.record_wait_time(&components, now);
                    return remaining;
                }
            }
        } else {
            return self
                .next_expiry()
                .map_or(Duration::ZERO, |exp| exp.saturating_duration_since(now));
        }

        self.add_ad(node, topic, now);
        Duration::ZERO
    }

    fn add_ad(&mut self, node: &Enr, topic: TopicId, now: Instant) {
        let node_id = node.node_id();
        let expiry = now + self.config.ad_lifetime;
        self.all.push_back(AdKey {
            node_id,
            topic,
            expiry,
        });
        self.topics.entry(topic).or_default().insert(
            node_id,
            Ad {
                node: node.clone(),
                expiry,
            },
        );
        self.wt.add_reg(node);
    }

    /// Removes an ad already taken out of `all`.
    fn remove_ad(&mut self, key: &AdKey) {
        let Some(ads) = self.topics.get_mut(&key.topic) else {
            return;
        };
        let Some(ad) = ads.remove(&key.node_id) else {
            return;
        };
        if ads.is_empty() {
            // The topic's last ad leaves: its service component returns to zero and its
            // lower bound can go, keeping the bounds map bounded by the topics held.
            self.topics.remove(&key.topic);
            self.wt.topic_bounds.remove(&key.topic);
        }
        self.wt.remove_reg(&ad.node);
    }

    fn wait_time_components(
        &self,
        node: &Enr,
        topic: &TopicId,
        now: Instant,
    ) -> (Duration, WaitTimeComponents) {
        let reg_count = self.all.len();

        // The inverse of the fill ratio: 1.0 for an empty cache.
        let occupancy = 1.0 - reg_count as f64 / self.config.ad_cache_size as f64;
        // Purely occupancy-based wait: AdLifetime/10 for an empty cache, growing past
        // AdLifetime as the cache fills.
        let base_time = W_BASE * self.config.ad_lifetime.as_secs_f64() / occupancy.powi(P_OCC);
        // Share of the cache already taken by this topic.
        let topic_mod = self.topic_size(topic) as f64 / (reg_count + 1) as f64;
        let (ip_mod, ip_nodes, ip_floor) = self.wt.ip_score(node, now);

        let components = WaitTimeComponents {
            topic: *topic,
            service_secs: base_time * topic_mod,
            ip_secs: base_time * ip_mod,
            ip_nodes,
        };

        let topic_floor = self
            .wt
            .topic_bounds
            .get(topic)
            .map_or(0.0, |b| b.remaining(now).as_secs_f64());
        let charged_service = components.service_secs.max(topic_floor);
        let charged_ip = components.ip_secs.max(ip_floor.as_secs_f64());
        (
            seconds_to_duration(charged_service + charged_ip),
            components,
        )
    }

    fn record_wait_time(&mut self, c: &WaitTimeComponents, now: Instant) {
        if c.service_secs > 0.0 {
            self.wt
                .topic_bounds
                .entry(c.topic)
                .or_default()
                .bump(seconds_to_duration(c.service_secs), now);
        }
        let ip_dur = seconds_to_duration(c.ip_secs);
        if ip_dur > Duration::ZERO {
            for (ip, depth) in c.ip_nodes.iter().flatten() {
                match ip {
                    IpAddr::V4(a) => self.wt.ipv4.bump_bound(&a.octets(), *depth, ip_dur, now),
                    IpAddr::V6(a) => self.wt.ipv6.bump_bound(&a.octets(), *depth, ip_dur, now),
                }
            }
        }
    }
}

impl WaitTimeState {
    /// The IP similarity score of a node, the tree nodes its bound would be recorded on,
    /// and the floor already in force on its prefixes. LAN addresses are exempt.
    fn ip_score(&self, node: &Enr, now: Instant) -> (f64, [Option<(IpAddr, usize)>; 2], Duration) {
        let mut score4 = 0.0;
        let mut score6 = 0.0;
        let mut nodes = [None, None];
        let mut floor = Duration::ZERO;
        if let Some(ip) = node.ip4()
            && !netutil::is_lan(IpAddr::V4(ip))
        {
            let octets = ip.octets();
            let (score, depth) = self.ipv4.score_node(&octets);
            score4 = score;
            nodes[0] = Some((IpAddr::V4(ip), depth));
            floor = floor.max(self.ipv4.path_floor(&octets, now));
        }
        if let Some(ip) = node.ip6()
            && !netutil::is_lan(IpAddr::V6(ip))
        {
            let octets = ip.octets();
            let (score, depth) = self.ipv6.score_node(&octets);
            score6 = score;
            nodes[1] = Some((IpAddr::V6(ip), depth));
            floor = floor.max(self.ipv6.path_floor(&octets, now));
        }
        (score4.max(score6), nodes, floor)
    }

    fn add_reg(&mut self, node: &Enr) {
        if let Some(ip) = node.ip4() {
            self.ipv4.insert(&ip.octets());
        }
        if let Some(ip) = node.ip6() {
            self.ipv6.insert(&ip.octets());
        }
    }

    fn remove_reg(&mut self, node: &Enr) {
        if let Some(ip) = node.ip4() {
            self.ipv4.remove(&ip.octets());
        }
        if let Some(ip) = node.ip6() {
            self.ipv6.remove(&ip.octets());
        }
    }
}

/// Converts a wait time in fractional seconds to a duration, rounding up so a stored
/// floor is never below the value it represents.
fn seconds_to_duration(secs: f64) -> Duration {
    if secs.is_nan() || secs <= 0.0 {
        return Duration::ZERO;
    }
    if !secs.is_finite() {
        return Duration::MAX;
    }
    Duration::from_nanos((secs * 1e9).ceil() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use enr::CombinedKey;
    use std::net::Ipv4Addr;

    const TOPIC1: TopicId = {
        let mut t = [0u8; 32];
        t[0] = 1;
        t[1] = 2;
        t
    };
    const TOPIC2: TopicId = [8u8; 32];

    fn config() -> TopicConfig {
        TopicConfig {
            ad_cache_size: 200,
            ..Default::default()
        }
    }

    fn node() -> Enr {
        let key = CombinedKey::generate_secp256k1();
        Enr::builder().build(&key).unwrap()
    }

    fn node_with_ip(ip: Ipv4Addr) -> Enr {
        let key = CombinedKey::generate_secp256k1();
        Enr::builder().ip4(ip).udp4(30303).build(&key).unwrap()
    }

    #[test]
    fn wait_then_admit() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        let n = node();
        let wt = tab.register(&n, TOPIC1, Duration::ZERO, now);
        // An empty cache has no service or IP component: admitted at once.
        assert_eq!(wt, Duration::ZERO);
        assert!(tab.is_registered(&n.node_id(), &TOPIC1));

        // A crowded topic requires waiting; presenting the required wait admits.
        for _ in 0..16 {
            assert!(tab.add(&node(), TOPIC1, now));
        }
        let m = node();
        let wt = tab.register(&m, TOPIC1, Duration::ZERO, now);
        assert!(wt > W_MIN, "got {wt:?}");
        assert!(!tab.is_registered(&m.node_id(), &TOPIC1));
        assert_eq!(tab.register(&m, TOPIC1, wt, now), Duration::ZERO);
        assert!(tab.is_registered(&m.node_id(), &TOPIC1));
        assert_eq!(tab.topic_size(&TOPIC1), 18);
    }

    #[test]
    fn wait_time_formula() {
        // 17 ads in a 200-slot cache, all on one topic, no public IPs:
        // base = 0.1 * 900 / (1 - 17/200)^10, service = base * 17/18, ip = 0.
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        for _ in 0..17 {
            assert!(tab.add(&node(), TOPIC1, now));
        }
        let base = 0.1 * 900.0 / (1.0 - 17.0 / 200.0f64).powi(10);
        let want = base * 17.0 / 18.0;
        let got = tab.wait_time(&node(), &TOPIC1, now).as_secs_f64();
        assert!((got - want).abs() < 1e-6, "got {got}, want {want}");
        assert_eq!(tab.wait_time(&node(), &TOPIC2, now), Duration::ZERO);
    }

    #[test]
    fn register_twice() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        let n = node();
        assert!(tab.add(&n, TOPIC1, now));
        assert_eq!(
            tab.register(&n, TOPIC1, Duration::ZERO, now),
            Duration::ZERO
        );
        assert_eq!(tab.topic_size(&TOPIC1), 1);
    }

    #[test]
    fn random_nodes() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        const N: usize = 20;
        for _ in 0..N {
            assert!(tab.add(&node(), TOPIC1, now));
        }
        for (n, want) in [(N, N), (N - 1, N - 1), (N + 1, N), (0, 0)] {
            let result = tab.random_nodes(&TOPIC1, n, |_| true);
            let mut ids: Vec<[u8; 32]> = result.iter().map(|enr| enr.node_id().raw()).collect();
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), result.len(), "results are not unique");
            assert_eq!(ids.len(), want);
        }
        assert!(tab.random_nodes(&TOPIC1, N, |_| false).is_empty());
        assert!(tab.random_nodes(&TOPIC2, N, |_| true).is_empty());
    }

    #[test]
    fn expiry_order() {
        let t0 = Instant::now();
        let mut tab = TopicTable::new(TopicConfig {
            ad_cache_size: 3,
            ..Default::default()
        });
        let lifetime = tab.ad_lifetime();
        let (a, b) = (node(), node());
        assert!(tab.add(&a, TOPIC1, t0));
        assert!(tab.add(&b, TOPIC1, t0 + Duration::from_secs(60)));
        assert_eq!(tab.next_expiry(), Some(t0 + lifetime));

        tab.expire(t0 + lifetime - Duration::from_secs(1));
        assert_eq!(tab.occupancy().0, 2);
        tab.expire(t0 + lifetime);
        assert_eq!(tab.occupancy().0, 1);
        assert!(!tab.is_registered(&a.node_id(), &TOPIC1));
        assert!(tab.is_registered(&b.node_id(), &TOPIC1));
        assert_eq!(
            tab.next_expiry(),
            Some(t0 + lifetime + Duration::from_secs(60))
        );
        tab.expire(t0 + lifetime + Duration::from_secs(60));
        assert_eq!(tab.occupancy().0, 0);
        assert_eq!(tab.next_expiry(), None);
        assert!(tab.topics.is_empty());
    }

    #[test]
    fn full_cache_quotes_next_expiry() {
        let t0 = Instant::now();
        let mut tab = TopicTable::new(TopicConfig {
            ad_cache_size: 2,
            ..Default::default()
        });
        assert!(tab.add(&node(), TOPIC1, t0));
        assert!(tab.add(&node(), TOPIC2, t0 + Duration::from_secs(10)));
        assert!(!tab.add(&node(), TOPIC2, t0));

        let now = t0 + Duration::from_secs(20);
        let wt = tab.register(&node(), TOPIC2, Duration::ZERO, now);
        assert_eq!(wt, tab.ad_lifetime() - Duration::from_secs(20));
        assert_eq!(tab.occupancy().0, 2);
    }

    #[test]
    fn remove_node_across_topics() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        let n = node_with_ip(Ipv4Addr::new(1, 2, 3, 4));
        assert!(tab.add(&n, TOPIC1, now));
        assert!(tab.add(&n, TOPIC2, now));
        assert!(tab.add(&node(), TOPIC1, now));
        // Each registration inserts the address once.
        assert_eq!(tab.wt.ipv4.count(), 2);
        tab.remove_node(&n.node_id());
        assert_eq!(tab.topic_size(&TOPIC1), 1);
        assert_eq!(tab.topic_size(&TOPIC2), 0);
        assert_eq!(tab.occupancy().0, 1);
        assert_eq!(tab.wt.ipv4.count(), 0);
    }

    #[test]
    fn topic_lower_bound_decays() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        let n = node();
        let mut bound = LowerBound::default();
        bound.bump(Duration::from_secs(300), now);
        tab.wt.topic_bounds.insert(TOPIC1, bound);

        assert!(tab.wait_time(&n, &TOPIC1, now) >= Duration::from_secs(300));
        assert!(tab.wait_time(&n, &TOPIC2, now) <= Duration::from_secs(1));
        let later = now + Duration::from_secs(60);
        let wt = tab.wait_time(&n, &TOPIC1, later);
        assert!(wt >= Duration::from_secs(240) && wt <= Duration::from_secs(241));
        assert!(
            tab.wait_time(&n, &TOPIC1, now + Duration::from_secs(360)) <= Duration::from_secs(1)
        );
    }

    #[test]
    fn topic_bound_recorded_and_collected() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        for _ in 0..16 {
            assert!(tab.add(&node(), TOPIC1, now));
        }
        let wt = tab.register(&node(), TOPIC1, Duration::ZERO, now);
        assert!(wt > W_MIN);
        let recorded = tab.wt.topic_bounds[&TOPIC1].remaining(now);
        assert!(recorded > Duration::ZERO);
        // The service component is floored at the recorded bound.
        assert!(tab.wait_time(&node(), &TOPIC1, now) >= recorded);

        tab.expire(now + tab.ad_lifetime());
        assert!(!tab.wt.topic_bounds.contains_key(&TOPIC1));
    }

    #[test]
    fn ip_bound_recorded_and_collected() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        // A cache dominated by one /24 gives another address in it a positive score.
        for i in 1..=60u8 {
            assert!(tab.add(&node_with_ip(Ipv4Addr::new(203, 0, 113, i)), TOPIC1, now));
        }
        let target = Ipv4Addr::new(203, 0, 113, 200);
        let (score, depth) = tab.wt.ipv4.score_node(&target.octets());
        assert!(score > 0.0);
        assert!(depth > 0);

        // Registering for an empty topic exercises only the IP component.
        let wt = tab.register(&node_with_ip(target), TOPIC2, Duration::ZERO, now);
        assert!(wt > W_MIN, "got {wt:?}");
        let floor = tab.wt.ipv4.path_floor(&target.octets(), now);
        assert!(floor > Duration::ZERO);
        assert_eq!(tab.wt.ipv4.bound_at(&target.octets(), depth, now), floor);
        assert!(tab.wait_time(&node_with_ip(target), &TOPIC2, now) >= floor);

        // LAN addresses are exempt from the IP term.
        assert_eq!(
            tab.wait_time(&node_with_ip(Ipv4Addr::new(10, 0, 0, 1)), &TOPIC2, now),
            Duration::ZERO
        );

        tab.expire(now + tab.ad_lifetime());
        assert_eq!(tab.wt.ipv4.count(), 0);
        assert!(tab.wt.ipv4.is_empty_tree());
    }

    #[test]
    fn ip_bound_survives_deeper_lpm() {
        let now = Instant::now();
        let mut tab = TopicTable::new(TopicConfig {
            ad_cache_size: 2000,
            ..Default::default()
        });
        for i in 1..=60u8 {
            assert!(tab.add(&node_with_ip(Ipv4Addr::new(203, 0, 113, i)), TOPIC1, now));
        }
        let target = Ipv4Addr::new(203, 0, 113, 200);
        assert!(tab.register(&node_with_ip(target), TOPIC2, Duration::ZERO, now) > W_MIN);
        let (_, bound_depth) = tab.wt.ipv4.score_node(&target.octets());
        assert!(tab.wt.ipv4.bound_at(&target.octets(), bound_depth, now) > Duration::ZERO);

        // Ads in the surrounding /23 deepen the tree below the node carrying the bound.
        for i in 1..=200u8 {
            assert!(tab.add(&node_with_ip(Ipv4Addr::new(203, 0, 112, i)), TOPIC1, now));
        }
        let (_, lpm_depth) = tab.wt.ipv4.score_node(&target.octets());
        assert!(lpm_depth > bound_depth, "LPM node did not move deeper");
        assert_eq!(
            tab.wt.ipv4.bound_at(&target.octets(), lpm_depth, now),
            Duration::ZERO
        );
        assert!(tab.wt.ipv4.path_floor(&target.octets(), now) > Duration::ZERO);

        tab.wt.ipv4.bump_bound(
            &target.octets(),
            bound_depth,
            Duration::from_secs(7200),
            now,
        );
        assert!(tab.wait_time(&node_with_ip(target), &TOPIC2, now) >= Duration::from_secs(7200));
    }
}

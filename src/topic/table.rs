use super::{
    TopicConfig, TopicId,
    bound::LowerBound,
    iptree::IpTree,
    netutil,
    params::{G, MAX_WAIT_TIME, P_OCC, W_BASE, W_MIN},
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
    /// The addresses inserted in the IP trees and the levels each insert incremented.
    ips: [Option<(IpAddr, u8)>; 2],
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

    /// Up to `n` random nodes registered for a topic, unexpired at `now`, that pass
    /// `check`.
    pub fn random_nodes(
        &self,
        topic: &TopicId,
        n: usize,
        now: Instant,
        check: impl Fn(&Enr) -> bool,
    ) -> Vec<Enr> {
        let Some(ads) = self.topics.get(topic) else {
            return Vec::new();
        };
        if n == 0 {
            return Vec::new();
        }
        ads.values()
            .filter(|ad| ad.expiry > now && check(&ad.node))
            .sample(&mut rand::rng(), n)
            .into_iter()
            .map(|ad| ad.node.clone())
            .collect()
    }

    /// Whether `node_id` holds an ad for `topic` that is unexpired at `now`.
    pub fn is_registered(&self, node_id: &NodeId, topic: &TopicId, now: Instant) -> bool {
        self.topics
            .get(topic)
            .and_then(|ads| ads.get(node_id))
            .is_some_and(|ad| ad.expiry > now)
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

    /// Adds an ad without admission control, after removing the ads expired at `now`.
    /// An ad already held is left unchanged. Fails when the cache is full.
    pub fn add(&mut self, node: &Enr, topic: TopicId, now: Instant) -> bool {
        self.expire(now);
        if self.is_registered(&node.node_id(), &topic, now) {
            return true;
        }
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

    /// The time a node must have waited to be admitted for a topic as of `now`. Ads
    /// expired at `now` are removed first.
    pub fn wait_time(&mut self, node: &Enr, topic: &TopicId, now: Instant) -> Duration {
        self.expire(now);
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
        if self.is_registered(&node.node_id(), &topic, now) {
            return Duration::ZERO;
        }

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

        // A full cache quotes MAX_WAIT_TIME, so this is reached only by a credit within
        // W_MIN of it: the node retries when the next ad expires.
        if self.all.len() >= self.config.ad_cache_size {
            return self
                .next_expiry()
                .map_or(MAX_WAIT_TIME, |exp| exp.saturating_duration_since(now));
        }

        self.add_ad(node, topic, now);
        Duration::ZERO
    }

    fn add_ad(&mut self, node: &Enr, topic: TopicId, now: Instant) {
        let node_id = node.node_id();
        let expiry = now + self.config.ad_lifetime;
        let ips = self.wt.add_reg(node);
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
                ips,
            },
        );
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
        self.wt.remove_reg(&ad.ips);
    }

    fn wait_time_components(
        &self,
        node: &Enr,
        topic: &TopicId,
        now: Instant,
    ) -> (Duration, WaitTimeComponents) {
        let reg_count = self.all.len();

        // The inverse of the fill ratio: 1.0 for an empty cache, 0.0 for a full one.
        let occupancy = 1.0 - reg_count as f64 / self.config.ad_cache_size as f64;
        // E/10 for an empty cache, infinite for a full one.
        let base_time = W_BASE * self.config.ad_lifetime.as_secs_f64() / occupancy.powi(P_OCC);
        let topic_mod = self.topic_size(topic) as f64 / (reg_count + 1) as f64;
        let (ip_mod, ip_nodes, ip_floor) = self.wt.ip_score(node, now);

        let components = WaitTimeComponents {
            topic: *topic,
            service_secs: scale_base(base_time, topic_mod),
            ip_secs: scale_base(base_time, ip_mod),
            ip_nodes,
        };

        let topic_floor = self
            .wt
            .topic_bounds
            .get(topic)
            .map_or(0.0, |b| b.remaining(now).as_secs_f64());
        let charged_service = components.service_secs.max(topic_floor);
        let charged_ip = components.ip_secs.max(ip_floor.as_secs_f64());
        // The safety term has no lower bound.
        let needed = charged_service + charged_ip + base_time * G;
        (seconds_to_duration(needed), components)
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
            for &(ip, depth) in c.ip_nodes.iter().flatten() {
                self.wt.bump_bound(ip, depth, ip_dur, now);
            }
        }
    }
}

impl WaitTimeState {
    /// The IP similarity score of a node, the tree nodes its bound would be recorded on,
    /// and the floor already in force on its prefixes. LAN addresses score 0.
    fn ip_score(&self, node: &Enr, now: Instant) -> (f64, [Option<(IpAddr, usize)>; 2], Duration) {
        let mut score = 0.0f64;
        let mut nodes = [None, None];
        let mut floor = Duration::ZERO;
        for (slot, ip) in ad_ips(node).into_iter().enumerate() {
            let Some(ip) = ip.filter(|ip| !netutil::is_lan(*ip)) else {
                continue;
            };
            let (s, depth, f) = self.with_tree(ip, |tree, octets| {
                let (s, depth) = tree.score_node(octets);
                (s, depth, tree.path_floor(octets, now))
            });
            score = score.max(s);
            nodes[slot] = Some((ip, depth));
            floor = floor.max(f);
        }
        (score, nodes, floor)
    }

    fn add_reg(&mut self, node: &Enr) -> [Option<(IpAddr, u8)>; 2] {
        ad_ips(node).map(|ip| {
            ip.map(|ip| {
                let levels = self.with_tree_mut(ip, |tree, octets| tree.insert(octets).1);
                (ip, levels)
            })
        })
    }

    fn remove_reg(&mut self, ips: &[Option<(IpAddr, u8)>; 2]) {
        for &(ip, levels) in ips.iter().flatten() {
            self.with_tree_mut(ip, |tree, octets| tree.remove(octets, levels));
        }
    }

    fn bump_bound(&mut self, ip: IpAddr, depth: usize, value: Duration, now: Instant) {
        self.with_tree_mut(ip, |tree, octets| {
            tree.bump_bound(octets, depth, value, now)
        });
    }

    fn with_tree<R>(&self, ip: IpAddr, f: impl FnOnce(&IpTree, &[u8]) -> R) -> R {
        match ip {
            IpAddr::V4(a) => f(&self.ipv4, &a.octets()),
            IpAddr::V6(a) => f(&self.ipv6, &a.octets()),
        }
    }

    fn with_tree_mut<R>(&mut self, ip: IpAddr, f: impl FnOnce(&mut IpTree, &[u8]) -> R) -> R {
        match ip {
            IpAddr::V4(a) => f(&mut self.ipv4, &a.octets()),
            IpAddr::V6(a) => f(&mut self.ipv6, &a.octets()),
        }
    }
}

/// The IP-tree addresses of a record: `ip4`, and `ip6` with an IPv4-mapped address
/// taken as the IPv4 address it contains (dropped when it equals `ip4`).
fn ad_ips(node: &Enr) -> [Option<IpAddr>; 2] {
    let ip4 = node.ip4().map(IpAddr::V4);
    let ip6 = node
        .ip6()
        .map(|ip| netutil::unmap(IpAddr::V6(ip)))
        .filter(|ip| Some(*ip) != ip4);
    [ip4, ip6]
}

/// `base * factor`, or 0 when the factor is 0, so a full cache (infinite base) does
/// not give NaN.
fn scale_base(base: f64, factor: f64) -> f64 {
    if factor > 0.0 { base * factor } else { 0.0 }
}

/// Converts a wait time in fractional seconds to a duration, rounding up so a stored
/// floor is never below the value it represents. Capped at `MAX_WAIT_TIME`; NaN and
/// non-positive values are 0.
fn seconds_to_duration(secs: f64) -> Duration {
    if secs.is_nan() || secs <= 0.0 {
        return Duration::ZERO;
    }
    if secs >= MAX_WAIT_TIME.as_secs_f64() {
        return MAX_WAIT_TIME;
    }
    Duration::from_nanos((secs * 1e9).ceil() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::params::{E, REG_ATTEMPT_BUDGET};
    use enr::CombinedKey;
    use std::net::{Ipv4Addr, Ipv6Addr};

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

    fn node_with_ip6(ip: Ipv6Addr) -> Enr {
        let key = CombinedKey::generate_secp256k1();
        Enr::builder().ip6(ip).udp6(30303).build(&key).unwrap()
    }

    fn sized(ad_cache_size: usize) -> TopicTable {
        TopicTable::new(TopicConfig {
            ad_cache_size,
            ..Default::default()
        })
    }

    /// Adds `n` ads for `TOPIC2` from nodes without an IP.
    fn fill_other(tab: &mut TopicTable, n: usize, now: Instant) -> Vec<Enr> {
        (0..n)
            .map(|_| {
                let n = node();
                assert!(tab.add(&n, TOPIC2, now));
                n
            })
            .collect()
    }

    #[test]
    fn wait_then_admit() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        let n = node();
        let wt = tab.register(&n, TOPIC1, Duration::ZERO, now);
        // An empty cache has no service or IP component: admitted at once.
        assert_eq!(wt, Duration::ZERO);
        assert!(tab.is_registered(&n.node_id(), &TOPIC1, now));

        // A crowded topic requires waiting; presenting the required wait admits.
        for _ in 0..16 {
            assert!(tab.add(&node(), TOPIC1, now));
        }
        let m = node();
        let wt = tab.register(&m, TOPIC1, Duration::ZERO, now);
        assert!(wt > W_MIN, "got {wt:?}");
        assert!(!tab.is_registered(&m.node_id(), &TOPIC1, now));
        assert_eq!(tab.register(&m, TOPIC1, wt, now), Duration::ZERO);
        assert!(tab.is_registered(&m.node_id(), &TOPIC1, now));
        assert_eq!(tab.topic_size(&TOPIC1), 18);
    }

    #[test]
    fn wait_time_formula() {
        // 17 ads in a 200-slot cache, all on one topic, no public IPs:
        // base = 0.1 * 900 / (1 - 17/200)^10, service = base * 17/18, ip = 0,
        // safety = base * G.
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        for _ in 0..17 {
            assert!(tab.add(&node(), TOPIC1, now));
        }
        let base = 0.1 * 900.0 / (1.0 - 17.0 / 200.0f64).powi(10);
        let want = base * 17.0 / 18.0 + base * G;
        let got = tab.wait_time(&node(), &TOPIC1, now).as_secs_f64();
        assert!((got - want).abs() < 1e-6, "got {got}, want {want}");
        assert_eq!(
            tab.wait_time(&node(), &TOPIC2, now),
            seconds_to_duration(base * G)
        );
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
    fn add_keeps_held_ad() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        let n = node_with_ip(Ipv4Addr::new(1, 2, 3, 4));
        assert!(tab.add(&n, TOPIC1, now));
        assert!(tab.add(&n, TOPIC1, now + Duration::from_secs(10)));
        assert_eq!(tab.occupancy().0, 1);
        assert_eq!(tab.wt.ipv4.count(), 1);
        assert_eq!(tab.next_expiry(), Some(now + tab.ad_lifetime()));
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
            let result = tab.random_nodes(&TOPIC1, n, now, |_| true);
            let mut ids: Vec<[u8; 32]> = result.iter().map(|enr| enr.node_id().raw()).collect();
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), result.len(), "results are not unique");
            assert_eq!(ids.len(), want);
        }
        assert!(tab.random_nodes(&TOPIC1, N, now, |_| false).is_empty());
        assert!(tab.random_nodes(&TOPIC2, N, now, |_| true).is_empty());
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
        assert!(!tab.is_registered(&a.node_id(), &TOPIC1, t0 + lifetime));
        assert!(tab.is_registered(&b.node_id(), &TOPIC1, t0 + lifetime));
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
    fn expired_ad_not_served_or_counted() {
        // Ads past expiry are neither served nor reported registered before a sweep.
        let t0 = Instant::now();
        let mut tab = TopicTable::new(config());
        let lifetime = tab.ad_lifetime();
        let old: Vec<Enr> = (0..5).map(|_| node()).collect();
        for n in &old {
            assert!(tab.add(n, TOPIC1, t0));
        }
        let live: Vec<NodeId> = (0..5)
            .map(|_| {
                let n = node();
                assert!(tab.add(&n, TOPIC1, t0 + Duration::from_secs(60)));
                n.node_id()
            })
            .collect();
        let now = t0 + lifetime;
        for n in [3, 5, 10] {
            let got = tab.random_nodes(&TOPIC1, n, now, |_| true);
            assert_eq!(got.len(), n.min(live.len()));
            assert!(got.iter().all(|enr| live.contains(&enr.node_id())));
        }
        assert!(
            old.iter()
                .all(|n| !tab.is_registered(&n.node_id(), &TOPIC1, now))
        );
        assert_eq!(tab.nodes(&TOPIC1, now).len(), live.len());

        // Expired ads do not count toward the waiting time.
        let expired = || {
            let mut tab = sized(100);
            fill_other(&mut tab, 92, t0);
            assert!(tab.add(&node_with_ip(Ipv4Addr::new(11, 2, 3, 4)), TOPIC1, t0));
            tab
        };
        let now = t0 + lifetime + Duration::from_secs(1);
        let n = node_with_ip(Ipv4Addr::new(11, 2, 3, 5));
        let empty = sized(100).wait_time(&n, &TOPIC1, now);
        assert_eq!(expired().wait_time(&n, &TOPIC1, now), empty);
        let mut tab = expired();
        assert_eq!(
            tab.register(&n, TOPIC1, Duration::ZERO, now),
            Duration::ZERO
        );
        assert_eq!(tab.occupancy().0, 1);

        // A node whose ad expired gets a new ad, not the old one.
        let mut tab = TopicTable::new(config());
        let n = node();
        assert!(tab.add(&n, TOPIC1, t0));
        assert_eq!(tab.register(&n, TOPIC1, MAX_WAIT_TIME, now), Duration::ZERO);
        assert_eq!(tab.occupancy().0, 1);
        assert_eq!(tab.next_expiry(), Some(now + lifetime));
    }

    #[test]
    fn wait_safety_term() {
        let now = Instant::now();
        // At low occupancy the safety term stays below W_MIN: a fresh topic is admitted.
        for fill in [10, 50] {
            let mut tab = sized(100);
            fill_other(&mut tab, fill, now);
            let n = node();
            assert_eq!(
                tab.register(&n, TOPIC1, Duration::ZERO, now),
                Duration::ZERO,
                "{fill}% fill"
            );
            assert!(tab.is_registered(&n.node_id(), &TOPIC1, now));
        }
        // At high occupancy it alone quotes a fresh topic from a score-0 address above
        // the attempt budget.
        for fill in [86, 93] {
            let mut tab = sized(100);
            fill_other(&mut tab, fill, now);
            let n = node();
            let wt = tab.register(&n, TOPIC1, Duration::ZERO, now);
            assert!(
                wt > REG_ATTEMPT_BUDGET && wt <= MAX_WAIT_TIME,
                "{fill}% fill: {wt:?}"
            );
            assert!(!tab.is_registered(&n.node_id(), &TOPIC1, now));
        }
        // It is not recorded as a bound: the quote drops with occupancy.
        let mut tab = sized(100);
        let ads = fill_other(&mut tab, 93, now);
        let n = node();
        assert!(tab.register(&n, TOPIC1, Duration::ZERO, now) > E);
        assert!(!tab.wt.topic_bounds.contains_key(&TOPIC1));
        for ad in &ads[50..] {
            tab.remove_node(&ad.node_id());
        }
        assert_eq!(
            tab.register(&n, TOPIC1, Duration::ZERO, now),
            Duration::ZERO
        );
    }

    #[test]
    fn wait_capped_24h() {
        let now = Instant::now();
        // The wait grows with occupancy and stays within the cap where the uncapped
        // value overflows a duration.
        let mut tab = sized(100);
        let mut prev = Duration::ZERO;
        for _ in 0..90 {
            assert!(tab.add(&node(), TOPIC1, now));
            let wt = tab.wait_time(&node(), &TOPIC1, now);
            assert!(wt >= prev, "{wt:?} < {prev:?}");
            prev = wt;
        }
        assert_eq!(prev, MAX_WAIT_TIME);
        let n = node();
        let wt = tab.register(&n, TOPIC1, Duration::ZERO, now);
        assert!(wt > Duration::ZERO && wt <= MAX_WAIT_TIME);
        assert!(!tab.is_registered(&n.node_id(), &TOPIC1, now));

        // A full cache quotes the cap to a fresh topic from a score-0 address and to
        // a held topic from a similar address (not 0 * inf), and records the cap as the
        // bound of an infinite component.
        let mut tab = sized(20);
        for i in 1..=20 {
            assert!(tab.add(&node_with_ip(Ipv4Addr::new(11, 2, 3, i)), TOPIC1, now));
        }
        assert_eq!(
            tab.register(&node(), TOPIC2, Duration::ZERO, now),
            MAX_WAIT_TIME
        );
        let similar = node_with_ip(Ipv4Addr::new(11, 2, 3, 200));
        assert_eq!(
            tab.register(&similar, TOPIC1, Duration::ZERO, now),
            MAX_WAIT_TIME
        );
        assert_eq!(tab.wt.topic_bounds[&TOPIC1].remaining(now), MAX_WAIT_TIME);
        assert_eq!(tab.wt.ipv4.path_floor(&[11, 2, 3, 200], now), MAX_WAIT_TIME);

        assert_eq!(seconds_to_duration(f64::NAN), Duration::ZERO);
        assert_eq!(seconds_to_duration(-1.0), Duration::ZERO);
        assert_eq!(seconds_to_duration(f64::INFINITY), MAX_WAIT_TIME);
        assert_eq!(seconds_to_duration(1e9), MAX_WAIT_TIME);
        assert_eq!(seconds_to_duration(1.5e-9), Duration::from_nanos(2));
    }

    #[test]
    fn full_cache_quote() {
        // A full cache quotes maxWaitTime minus the credit, as a ticket that raises
        // the bounds.
        let now = Instant::now();
        let mut tab = sized(20);
        for _ in 0..20 {
            assert!(tab.add(&node(), TOPIC1, now));
        }
        let n = node();
        let hour = Duration::from_secs(3600);
        assert_eq!(tab.register(&n, TOPIC2, Duration::ZERO, now), MAX_WAIT_TIME);
        assert_eq!(tab.register(&n, TOPIC2, hour, now), MAX_WAIT_TIME - hour);
        let two = Duration::from_secs(2);
        assert_eq!(tab.register(&n, TOPIC2, MAX_WAIT_TIME - two, now), two);
        assert!(!tab.wt.topic_bounds.contains_key(&TOPIC1));
        assert_eq!(tab.register(&n, TOPIC1, hour, now), MAX_WAIT_TIME - hour);
        assert_eq!(tab.wt.topic_bounds[&TOPIC1].remaining(now), MAX_WAIT_TIME);
        assert!(!tab.is_registered(&n.node_id(), &TOPIC1, now));
        assert!(!tab.is_registered(&n.node_id(), &TOPIC2, now));
        assert_eq!(tab.occupancy().0, 20);
    }

    #[test]
    fn full_cache_next_expiry() {
        // Only a credit within W_MIN of maxWaitTime gets the time to the next expiry,
        // which leaves the bounds unchanged.
        let t0 = Instant::now();
        let mut tab = sized(20);
        assert!(tab.add(&node(), TOPIC1, t0));
        for _ in 1..20 {
            assert!(tab.add(&node(), TOPIC1, t0 + Duration::from_secs(60)));
        }
        let first_expiry = t0 + tab.ad_lifetime();
        let n = node();
        for now in [t0 + Duration::from_secs(120), t0 + Duration::from_secs(420)] {
            for credit in [MAX_WAIT_TIME, MAX_WAIT_TIME - W_MIN] {
                assert_eq!(tab.register(&n, TOPIC1, credit, now), first_expiry - now);
            }
        }
        assert!(tab.wt.topic_bounds.is_empty());
        assert!(!tab.is_registered(&n.node_id(), &TOPIC1, first_expiry));
        assert_eq!(tab.occupancy().0, 20);

        // The expired ad frees its slot for the waiting node.
        assert_eq!(
            tab.register(&n, TOPIC1, MAX_WAIT_TIME, first_expiry),
            Duration::ZERO
        );
        assert!(tab.is_registered(&n.node_id(), &TOPIC1, first_expiry));
        assert_eq!(tab.occupancy().0, 20);
    }

    #[test]
    fn no_admit_without_store() {
        // A cache without capacity quotes the cap instead of admitting.
        let now = Instant::now();
        let mut tab = sized(0);
        let n = node();
        assert_eq!(tab.register(&n, TOPIC1, MAX_WAIT_TIME, now), MAX_WAIT_TIME);
        assert_eq!(tab.occupancy().0, 0);

        // A zero quote always means the ad is held, and the cache never exceeds C.
        let t0 = Instant::now();
        let mut tab = sized(5);
        let nodes: Vec<Enr> = (0..8).map(|_| node()).collect();
        let credits = [
            Duration::ZERO,
            W_MIN,
            E,
            MAX_WAIT_TIME - W_MIN,
            MAX_WAIT_TIME,
        ];
        for i in 0..400u64 {
            let now = t0 + Duration::from_secs(i * 30);
            let n = &nodes[(i % 8) as usize];
            let credit = credits[(i % 5) as usize];
            let topic = if i % 3 == 0 { TOPIC2 } else { TOPIC1 };
            if tab.register(n, topic, credit, now) == Duration::ZERO {
                assert!(tab.is_registered(&n.node_id(), &topic, now), "step {i}");
            }
            assert!(tab.occupancy().0 <= 5);
        }
    }

    #[test]
    fn ipv4_mapped_scored_as_v4() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        for i in 1..=60u8 {
            assert!(tab.add(&node_with_ip(Ipv4Addr::new(203, 0, 113, i)), TOPIC1, now));
        }
        let ip = Ipv4Addr::new(203, 0, 113, 200);
        let v4 = node_with_ip(ip);
        let mapped = node_with_ip6(ip.to_ipv6_mapped());
        let wt = tab.wait_time(&v4, &TOPIC2, now);
        assert!(wt > W_MIN);
        assert_eq!(tab.wait_time(&mapped, &TOPIC2, now), wt);

        // Stored in the IPv4 tree, and once when `ip6` maps the `ip4` address.
        assert!(tab.add(&mapped, TOPIC2, now));
        let key = CombinedKey::generate_secp256k1();
        let both = Enr::builder()
            .ip4(Ipv4Addr::new(1, 2, 3, 4))
            .ip6(Ipv4Addr::new(1, 2, 3, 4).to_ipv6_mapped())
            .build(&key)
            .unwrap();
        assert!(tab.add(&both, TOPIC2, now));
        assert_eq!((tab.wt.ipv4.count(), tab.wt.ipv6.count()), (62, 0));
        tab.remove_node(&mapped.node_id());
        tab.remove_node(&both.node_id());
        assert_eq!((tab.wt.ipv4.count(), tab.wt.ipv6.count()), (60, 0));
    }

    #[test]
    fn lan_scores_zero() {
        let now = Instant::now();
        let mut tab = TopicTable::new(config());
        for i in 1..=60u8 {
            assert!(tab.add(&node_with_ip(Ipv4Addr::new(10, 0, 0, i)), TOPIC1, now));
        }
        // LAN ads are in the tree, but a LAN address in their prefix scores 0.
        assert_eq!(tab.wt.ipv4.count(), 60);
        assert!(tab.wt.ipv4.score_node(&[10, 0, 0, 200]).0 > 0.0);
        for (lan, topic) in [
            (node_with_ip(Ipv4Addr::new(10, 0, 0, 200)), TOPIC2),
            (
                node_with_ip6(Ipv4Addr::new(10, 0, 0, 200).to_ipv6_mapped()),
                [9u8; 32],
            ),
        ] {
            let no_ip = tab.wait_time(&node(), &topic, now);
            assert_eq!(tab.wait_time(&lan, &topic, now), no_ip);
            assert_eq!(
                tab.register(&lan, topic, Duration::ZERO, now),
                Duration::ZERO
            );
        }
        assert_eq!(tab.wt.ipv4.count(), 62);
        assert_eq!(
            tab.wt.ipv4.path_floor(&[10, 0, 0, 200], now),
            Duration::ZERO
        );
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
        let no_ip = tab.wait_time(&node(), &TOPIC2, now);
        assert_eq!(
            tab.wait_time(&node_with_ip(Ipv4Addr::new(10, 0, 0, 1)), &TOPIC2, now),
            no_ip
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

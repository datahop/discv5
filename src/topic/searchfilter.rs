//! The result filter and mixer of a search. The filter remembers the nodes a search
//! delivered for one ad lifetime, so that later passes do not deliver them again; a
//! node with a newer record is delivered once more, and only once. The mixer queues the
//! results of a pass per registrar and hands them out one registrar at a time, so that
//! no registrar's reply reaches the caller in a burst.

use super::{
    TopicConfig, TopicNode,
    params::{SEARCH_FILTER_LIMIT, SEARCH_MIX_REGISTRARS, SEARCH_MIX_WAIT},
};
use crate::Enr;
use enr::NodeId;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

struct Entry {
    seq: u64,
    expiry: Instant,
    /// Position in `order`.
    tick: u64,
    /// A newer record was delivered again.
    returned: bool,
}

/// The results of one registrar that wait to be handed out. A node several registrars
/// return waits in each of their queues and is handed out once.
struct RegistrarQueue<N> {
    nodes: VecDeque<N>,
    /// When the queue last went from empty to waiting.
    queued_at: Instant,
}

pub struct SearchFilter<N: TopicNode = Enr> {
    local_id: NodeId,
    ttl: Duration,
    limit: usize,
    seen: HashMap<NodeId, Entry>,
    /// Oldest expiry first: the lifetime is constant and a refreshed entry moves back.
    order: BTreeMap<u64, NodeId>,
    next_tick: u64,

    mix_registrars: usize,
    mix_wait: Duration,
    /// In the order the registrars first replied with results in the pass.
    queues: Vec<RegistrarQueue<N>>,
    registrars: HashMap<NodeId, usize>,
    /// The newest record of every waiting node.
    newest: HashMap<NodeId, N>,
    /// Handed out in this pass.
    handed: HashSet<NodeId>,
    first_queued: Option<Instant>,
}

impl<N: TopicNode> SearchFilter<N> {
    /// Creates an empty filter that keeps entries for the ad lifetime.
    pub fn new(local_id: NodeId, config: &TopicConfig) -> Self {
        SearchFilter {
            local_id,
            ttl: config.ad_lifetime,
            limit: SEARCH_FILTER_LIMIT,
            seen: HashMap::new(),
            order: BTreeMap::new(),
            next_tick: 0,
            mix_registrars: SEARCH_MIX_REGISTRARS,
            mix_wait: SEARCH_MIX_WAIT,
            queues: Vec::new(),
            registrars: HashMap::new(),
            newest: HashMap::new(),
            handed: HashSet::new(),
            first_queued: None,
        }
    }

    fn reset(&mut self) {
        self.queues.clear();
        self.registrars.clear();
        self.newest.clear();
        self.handed.clear();
        self.first_queued = None;
    }

    /// Queues the results of a registrar's reply and returns the results that can be
    /// handed out now. A failed query passes no results; it still hands out what is due.
    /// A registrar counts toward the mix when its reply held results that passed the
    /// filter, even if every one of them waits in other queues as well.
    pub fn take(
        &mut self,
        registrar: NodeId,
        results: impl IntoIterator<Item = N>,
        now: Instant,
    ) -> Vec<N> {
        for n in results {
            let id = n.node_id();
            if id == self.local_id || self.seen(&n, now) || self.handed.contains(&id) {
                continue;
            }
            let qi = match self.registrars.get(&registrar) {
                Some(&qi) => qi,
                None => {
                    self.queues.push(RegistrarQueue {
                        nodes: VecDeque::new(),
                        queued_at: now,
                    });
                    self.registrars.insert(registrar, self.queues.len() - 1);
                    self.queues.len() - 1
                }
            };
            if self.newest.get(&id).is_none_or(|cur| n.seq() > cur.seq()) {
                self.newest.insert(id, n.clone());
            }
            let q = &mut self.queues[qi];
            if q.nodes.is_empty() {
                q.queued_at = now;
            }
            self.first_queued.get_or_insert(now);
            q.nodes.push_back(n);
        }
        self.mix(false, now)
    }

    /// Returns the results that still wait, one registrar at a time, and starts a new
    /// pass. It is called at the end of a search pass.
    pub fn release(&mut self, now: Instant) -> Vec<N> {
        let out = self.mix(true, now);
        self.reset();
        out
    }

    /// Hands out results in rounds of one per registrar with results waiting, once
    /// enough registrars have replied or the first result has waited long enough. A
    /// round with a single registrar waits as well, so that a late reply is interleaved
    /// with whatever comes after it, unless it has waited long enough on its own. With
    /// `all` set, every waiting result goes out.
    fn mix(&mut self, all: bool, now: Instant) -> Vec<N> {
        let wait = self.mix_wait;
        let waited = |since: Instant| now.saturating_duration_since(since) >= wait;
        if !all && self.queues.len() < self.mix_registrars && !self.first_queued.is_some_and(waited)
        {
            return Vec::new();
        }
        let mut out = Vec::new();
        loop {
            let round: Vec<usize> = (0..self.queues.len())
                .filter(|&i| !self.queues[i].nodes.is_empty())
                .collect();
            if round.is_empty()
                || (!all && round.len() == 1 && !waited(self.queues[round[0]].queued_at))
            {
                return out;
            }
            for i in round {
                let n = self.queues[i].nodes.pop_front().expect("queue in round");
                let id = n.node_id();
                if !self.handed.insert(id) {
                    continue;
                }
                out.push(self.newest.remove(&id).unwrap_or(n));
            }
        }
    }

    /// Reports whether the node was delivered recently with the same or a newer record,
    /// or whether its one re-delivery with a newer record was already used.
    pub fn seen(&mut self, n: &N, now: Instant) -> bool {
        self.expire(now);
        self.seen
            .get(&n.node_id())
            .is_some_and(|e| n.seq() <= e.seq || e.returned)
    }

    /// Records that the node was delivered. Every id keeps a single entry, so the filter
    /// never holds more than its limit; when full, the entry that expires first goes.
    pub fn add(&mut self, n: &N, now: Instant) {
        self.expire(now);
        let id = n.node_id();
        let tick = self.next_tick;
        self.next_tick += 1;
        let mut entry = Entry {
            seq: n.seq(),
            expiry: now + self.ttl,
            tick,
            returned: false,
        };
        if let Some(old) = self.seen.remove(&id) {
            self.order.remove(&old.tick);
            entry.returned = old.returned || n.seq() > old.seq;
        } else {
            while self.seen.len() >= self.limit {
                let Some((_, oldest)) = self.order.pop_first() else {
                    break;
                };
                self.seen.remove(&oldest);
            }
        }
        self.seen.insert(id, entry);
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

    #[cfg(test)]
    pub(crate) fn set_mix_wait(&mut self, wait: Duration) {
        self.mix_wait = wait;
    }
}

#[cfg(test)]
mod tests {
    use super::{super::test_util::*, *};

    const MINUTE: Duration = Duration::from_secs(60);
    const SEC: Duration = Duration::from_secs(1);

    fn filter(ttl: Duration) -> SearchFilter<TestNode> {
        filter_for(NodeId::random(), ttl)
    }

    fn filter_for(local_id: NodeId, ttl: Duration) -> SearchFilter<TestNode> {
        SearchFilter::new(
            local_id,
            &TopicConfig {
                ad_lifetime: ttl,
                ..Default::default()
            },
        )
    }

    fn node(seq: u64) -> TestNode {
        TestNode {
            id: NodeId::random(),
            seq,
            ip: Some(int_ip(1)),
        }
    }

    fn nodes(n: usize) -> Vec<TestNode> {
        (0..n).map(|_| node(1)).collect()
    }

    fn with_seq(n: &TestNode, seq: u64) -> TestNode {
        TestNode { seq, ..n.clone() }
    }

    fn ids(nodes: &[TestNode]) -> Vec<NodeId> {
        nodes.iter().map(|n| n.id).collect()
    }

    #[test]
    fn expiry() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        let n = node(1);

        assert!(!f.seen(&n, now));
        f.add(&n, now);
        assert!(f.seen(&n, now + MINUTE - Duration::from_nanos(1)));
        assert!(!f.seen(&n, now + MINUTE));
        assert!(f.is_empty());
        assert!(f.order.is_empty());
    }

    #[test]
    fn newer_record() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        let old = node(1);
        let updated = with_seq(&old, 2);

        f.add(&old, now);
        assert!(!f.seen(&updated, now), "newer record filtered");
        let now = now + 30 * SEC;
        f.add(&updated, now);
        assert!(f.seen(&old, now) && f.seen(&updated, now));
        assert_eq!(f.len(), 1);
        // The first expiry must not drop the entry refreshed by the second add.
        let now = now + 30 * SEC;
        assert!(f.seen(&updated, now));
        let now = now + 30 * SEC;
        assert!(!f.seen(&updated, now));
    }

    #[test]
    fn limit() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        f.limit = 3;
        let nodes = nodes(4);
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

    // A newer record of a delivered node is delivered once more within the ttl, and not
    // again however often its record is bumped.
    #[test]
    fn newer_record_once() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        let r1 = node(1);
        let (r2, r3) = (with_seq(&r1, 2), with_seq(&r1, 3));
        f.add(&r1, now);
        assert!(!f.seen(&r2, now), "newer record filtered");
        f.add(&r2, now);
        assert!(f.seen(&r3, now), "a second newer record was not filtered");
        assert!(!f.seen(&r3, now + 2 * MINUTE), "record seen after expiry");
    }

    // Results are handed out one registrar at a time, once `SEARCH_MIX_REGISTRARS`
    // registrars have results waiting.
    #[test]
    fn mix_releases_at_ten_registrars() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        let regs: Vec<NodeId> = (0..SEARCH_MIX_REGISTRARS)
            .map(|_| NodeId::random())
            .collect();
        let ads: Vec<Vec<TestNode>> = regs.iter().map(|_| nodes(16)).collect();
        for (i, reg) in regs[..regs.len() - 1].iter().enumerate() {
            assert!(f.take(*reg, ads[i].clone(), now).is_empty(), "reply {i}");
        }
        let last = regs.len() - 1;
        let got = f.take(regs[last], ads[last].clone(), now);
        assert_eq!(got.len(), 16 * regs.len());
        for (i, n) in got.iter().enumerate() {
            assert_eq!(n.id, ads[i % regs.len()][i / regs.len()].id, "result {i}");
        }
        assert!(f.release(now).is_empty());
    }

    // Results of fewer registrars wait until the release. Once enough registrars have
    // replied, everything waiting is handed out one registrar at a time.
    #[test]
    fn mix_release() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        f.mix_registrars = 3;
        let (reg_a, reg_b, reg_c) = (NodeId::random(), NodeId::random(), NodeId::random());
        let (ads_a, ads_b, ads_c) = (nodes(3), nodes(3), nodes(1));

        assert!(f.take(reg_a, ads_a.clone(), now).is_empty());
        assert!(f.take(reg_b, ads_b.clone(), now).is_empty());
        let got = f.take(reg_c, ads_c.clone(), now);
        let want = [
            &ads_a[0], &ads_b[0], &ads_c[0], &ads_a[1], &ads_b[1], &ads_a[2], &ads_b[2],
        ];
        assert_eq!(ids(&got), want.iter().map(|n| n.id).collect::<Vec<_>>());
        assert!(f.release(now).is_empty());
        // A lone registrar's results wait for the pass to end.
        assert!(f.take(reg_a, nodes(2), now).is_empty());
        assert_eq!(f.release(now).len(), 2);
    }

    // A result that two registrars return is handed out once. Results delivered
    // recently are dropped.
    #[test]
    fn mix_shared() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        let (reg_a, reg_b) = (NodeId::random(), NodeId::random());
        let ads = nodes(3);

        f.add(&ads[0], now);
        f.take(reg_a, [ads[0].clone(), ads[1].clone()], now);
        f.take(reg_b, [ads[1].clone(), ads[2].clone()], now);
        assert_eq!(ids(&f.release(now)), ids(&ads[1..]));
    }

    // A newer record of a node that already waits in a queue replaces the waiting one;
    // an older one does not.
    #[test]
    fn mix_newer_record() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        let (reg_a, reg_b) = (NodeId::random(), NodeId::random());
        let old = node(1);
        let updated = with_seq(&old, 2);

        f.take(reg_a, [old.clone()], now);
        f.take(reg_b, [updated.clone()], now);
        assert_eq!(f.release(now), vec![updated.clone()]);
        f.take(reg_a, [updated.clone()], now);
        f.take(reg_b, [old], now);
        assert_eq!(
            f.release(now),
            vec![updated],
            "an older record replaced the waiting one"
        );
    }

    // Registrars holding the same ads count toward the mix: the first ones to reply
    // queue the ads, the later ones confirm them, and the results flow once enough
    // registrars have replied, instead of waiting for the pass.
    #[test]
    fn mix_overlap() {
        let now = Instant::now();
        let mut f = filter(MINUTE);
        let regs: Vec<NodeId> = (0..SEARCH_MIX_REGISTRARS)
            .map(|_| NodeId::random())
            .collect();
        let ads = nodes(16);
        for (i, reg) in regs[..regs.len() - 1].iter().enumerate() {
            assert!(f.take(*reg, ads.clone(), now).is_empty(), "reply {i}");
        }
        let got = f.take(regs[regs.len() - 1], ads.clone(), now);
        assert_eq!(got.len(), ads.len());
        assert!(f.release(now).is_empty());
    }

    // Results do not wait longer than `SEARCH_MIX_WAIT` for more registrars. A failed
    // query, which brings no results, hands out what is due.
    #[test]
    fn mix_releases_after_wait() {
        let start = Instant::now();
        let mut f = filter(MINUTE);
        let (reg_a, reg_b) = (NodeId::random(), NodeId::random());
        let (ads_a, ads_b) = (nodes(2), nodes(2));
        assert!(f.take(reg_a, ads_a.clone(), start).is_empty());
        let now = start + SEARCH_MIX_WAIT - SEC;
        assert!(f.take(reg_b, ads_b.clone(), now).is_empty());
        let got = f.take(NodeId::random(), [], start + SEARCH_MIX_WAIT);
        let want = [&ads_a[0], &ads_b[0], &ads_a[1], &ads_b[1]];
        assert_eq!(ids(&got), want.iter().map(|n| n.id).collect::<Vec<_>>());
    }

    // The local node and nodes already handed out in the pass are dropped, and do not
    // count as a registrar's results.
    #[test]
    fn mix_drops() {
        let now = Instant::now();
        let local = node(1);
        let mut f = filter_for(local.id, MINUTE);
        f.mix_registrars = 2;
        let (reg_a, reg_b, reg_c) = (NodeId::random(), NodeId::random(), NodeId::random());
        let ads = nodes(2);
        assert!(f.take(reg_a, [local], now).is_empty());
        assert!(f.queues.is_empty(), "the local node was queued");
        f.take(reg_a, [ads[0].clone()], now);
        assert_eq!(f.take(reg_b, [ads[1].clone()], now).len(), 2);
        // ads[0] was handed out and waits for the caller: a third registrar returning it
        // must not queue it again.
        assert!(f.take(reg_c, [ads[0].clone()], now).is_empty());
        assert!(f.release(now).is_empty());
    }

    // Mixing holds for the whole pass: a registrar that replies after the first results
    // went out gets one result per round, interleaved with the replies that follow, and
    // a lone late reply waits for company or for the wait.
    #[test]
    fn mix_continuous() {
        let start = Instant::now();
        let mut f = filter(MINUTE);
        f.mix_registrars = 2;
        let regs: Vec<NodeId> = (0..4).map(|_| NodeId::random()).collect();
        let (ads_a, ads_b, ads_s, ads_d) = (nodes(1), nodes(1), nodes(4), nodes(2));
        f.take(regs[0], ads_a, start);
        assert_eq!(f.take(regs[1], ads_b, start).len(), 2);
        // A late reply of four results is held: nothing to mix it with yet.
        assert!(f.take(regs[2], ads_s.clone(), start).is_empty());
        let now = start + SEC;
        let got = f.take(regs[3], ads_d.clone(), now);
        let want = [&ads_s[0], &ads_d[0], &ads_s[1], &ads_d[1]];
        assert_eq!(ids(&got), want.iter().map(|n| n.id).collect::<Vec<_>>());
        // The rest of the late reply waits again, then goes out after the wait.
        assert!(f.take(NodeId::random(), [], now).is_empty());
        let got = f.take(NodeId::random(), [], now + SEARCH_MIX_WAIT);
        assert_eq!(ids(&got), ids(&ads_s[2..]));
    }

    // Past the registrar threshold, a lone registrar's queue still waits
    // `SEARCH_MIX_WAIT` from when it last became non-empty.
    #[test]
    fn lone_registrar_waits() {
        let start = Instant::now();
        let mut f = filter(MINUTE);
        f.mix_registrars = 1;
        let reg = NodeId::random();
        let ads = nodes(3);
        assert!(f.take(reg, ads.clone(), start).is_empty());
        let late = start + SEARCH_MIX_WAIT - SEC;
        assert!(f.take(NodeId::random(), [], late).is_empty());
        let got = f.take(NodeId::random(), [], start + SEARCH_MIX_WAIT);
        assert_eq!(ids(&got), ids(&ads));
        // The queue emptied: new results wait from their own arrival.
        let now = start + 2 * SEARCH_MIX_WAIT;
        let more = nodes(1);
        assert!(f.take(reg, more.clone(), now).is_empty());
        assert!(f.take(reg, [], now + SEARCH_MIX_WAIT - SEC).is_empty());
        assert_eq!(ids(&f.take(reg, [], now + SEARCH_MIX_WAIT)), ids(&more));
    }
}

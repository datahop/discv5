use super::bound::LowerBound;
use std::time::{Duration, Instant};

/// A binary prefix tree over IP addresses held in the ad cache, used to score how
/// clustered a new address is relative to the cache and to store per-prefix lower
/// bounds on the IP component of the waiting time.
pub(super) struct IpTree {
    root: Node,
    bits: u8,
}

#[derive(Default)]
struct Node {
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
    counter: usize,
    bound: LowerBound,
}

impl IpTree {
    /// `bits` is 32 for IPv4 and 128 for IPv6. Addresses are passed as their raw
    /// big-endian octets of that width.
    pub(super) fn new(bits: u8) -> Self {
        assert!(bits == 32 || bits == 128, "invalid ipTree bits {bits}");
        IpTree {
            root: Node::default(),
            bits,
        }
    }

    /// Adds an address and returns its similarity score.
    pub(super) fn insert(&mut self, ip: &[u8]) -> f64 {
        debug_assert_eq!(ip.len() * 8, self.bits as usize);
        let root_counter = self.root.counter as f64;
        self.root.counter += 1;
        let mut node = &mut self.root;
        let mut sum = 0usize;
        let mut effective_depth = 0u32;
        for depth in 0..self.bits {
            // Beyond this depth the tree has no statistical power to flag a prefix as
            // overloaded.
            let balanced = root_counter / 2f64.powi(depth as i32 + 1);
            if balanced < 1.0 {
                break;
            }
            effective_depth += 1;
            let child = if ip_bit(ip, depth) {
                &mut node.left
            } else {
                &mut node.right
            };
            let n = child.get_or_insert_with(Default::default);
            if n.counter as f64 > balanced {
                sum += 1;
            }
            n.counter += 1;
            node = n;
        }
        self.compute_score(sum, effective_depth)
    }

    /// Returns the score that inserting `ip` would produce, and the depth of the
    /// longest-prefix-match node (0 is the root): the node the IP-component lower
    /// bound for `ip` is recorded on.
    pub(super) fn score_node(&self, ip: &[u8]) -> (f64, usize) {
        debug_assert_eq!(ip.len() * 8, self.bits as usize);
        let root_counter = self.root.counter as f64;
        let mut node = &self.root;
        let mut lpm_depth = 0usize;
        let mut sum = 0usize;
        let mut effective_depth = 0u32;
        let mut hit_missing = false;
        for depth in 0..self.bits {
            let balanced = root_counter / 2f64.powi(depth as i32 + 1);
            if balanced < 1.0 {
                break;
            }
            // insert() would create nodes from here on and keep counting depth.
            effective_depth += 1;
            if hit_missing {
                continue;
            }
            let child = if ip_bit(ip, depth) {
                &node.left
            } else {
                &node.right
            };
            match child {
                None => hit_missing = true,
                Some(n) => {
                    lpm_depth = depth as usize + 1;
                    if n.counter as f64 > balanced {
                        sum += 1;
                    }
                    node = n;
                }
            }
        }
        (self.compute_score(sum, effective_depth), lpm_depth)
    }

    /// The largest still-effective lower bound on any existing node along the path of
    /// `ip`. A bound on an ancestor covers every address under its prefix, and later
    /// inserts can create nodes deeper than the one a bound was recorded on, so the
    /// whole path is walked. The root is not consulted, as in the Go implementation.
    pub(super) fn path_floor(&self, ip: &[u8], now: Instant) -> Duration {
        debug_assert_eq!(ip.len() * 8, self.bits as usize);
        let mut floor = Duration::ZERO;
        let mut node = &self.root;
        for depth in 0..self.bits {
            let child = if ip_bit(ip, depth) {
                &node.left
            } else {
                &node.right
            };
            match child {
                None => break,
                Some(n) => {
                    floor = floor.max(n.bound.remaining(now));
                    node = n;
                }
            }
        }
        floor
    }

    /// Raises the lower bound stored on the node at `depth` along the path of `ip`.
    pub(super) fn bump_bound(&mut self, ip: &[u8], depth: usize, value: Duration, now: Instant) {
        let mut node = &mut self.root;
        for d in 0..depth {
            let child = if ip_bit(ip, d as u8) {
                &mut node.left
            } else {
                &mut node.right
            };
            match child {
                None => return,
                Some(n) => node = n,
            }
        }
        node.bound.bump(value, now);
    }

    /// Removes an address. The branch is freed, together with the bounds stored on it,
    /// once no address is left under it.
    pub(super) fn remove(&mut self, ip: &[u8]) {
        debug_assert_eq!(ip.len() * 8, self.bits as usize);
        self.root.counter = self.root.counter.saturating_sub(1);
        let mut node = &mut self.root;
        for depth in 0..self.bits {
            let child = if ip_bit(ip, depth) {
                &mut node.left
            } else {
                &mut node.right
            };
            match child.as_deref() {
                None => return,
                Some(n) if n.counter <= 1 => {
                    *child = None;
                    return;
                }
                Some(_) => {}
            }
            let n = child.as_deref_mut().expect("checked above");
            n.counter -= 1;
            node = n;
        }
    }

    /// Number of addresses in the tree.
    #[cfg(test)]
    pub(super) fn count(&self) -> usize {
        self.root.counter
    }

    #[cfg(test)]
    pub(super) fn is_empty_tree(&self) -> bool {
        self.root.left.is_none() && self.root.right.is_none()
    }

    #[cfg(test)]
    pub(super) fn bound_at(&self, ip: &[u8], depth: usize, now: Instant) -> Duration {
        let mut node = &self.root;
        for d in 0..depth {
            let child = if ip_bit(ip, d as u8) {
                &node.left
            } else {
                &node.right
            };
            match child {
                None => return Duration::ZERO,
                Some(n) => node = n,
            }
        }
        node.bound.remaining(now)
    }

    fn compute_score(&self, sum: usize, effective_depth: u32) -> f64 {
        if self.root.counter == 0 || effective_depth == 0 {
            return 0.0;
        }
        sum as f64 / effective_depth as f64
    }
}

fn ip_bit(ip: &[u8], i: u8) -> bool {
    ip[(i / 8) as usize] & (1 << (7 - i % 8)) != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    // Expected scores taken from the Go implementation's test table.
    const IPV4_OPS: &[(&str, f64)] = &[
        ("1.2.3.4", 0.0),
        ("1.2.3.5", 0.0),
        ("1.2.3.6", 0.0),
        ("1.2.3.4", 0.0),
        ("1.2.3.4", 0.0),
        ("1.2.3.4", 0.5),
        ("1.1.1.1", 1.0),
        ("8.8.8.8", 1.0),
    ];

    const IPV6_OPS: &[(&str, f64)] = &[
        ("fe80::716b:dafc:774e:a826", 0.0),
        ("fe80::716b:dafc:774e:a827", 0.0),
        ("fe80::716b:dafc:774e:a828", 0.0),
        ("abc2:2345:7a7b:774e:a828:f184:ee25:4545", 0.0),
    ];

    fn v4(s: &str) -> [u8; 4] {
        s.parse::<Ipv4Addr>().unwrap().octets()
    }

    fn v6(s: &str) -> [u8; 16] {
        s.parse::<Ipv6Addr>().unwrap().octets()
    }

    fn almost_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-8
    }

    #[test]
    fn insert_ipv4() {
        let mut tree = IpTree::new(32);
        for (i, (ip, want)) in IPV4_OPS.iter().enumerate() {
            let score = tree.insert(&v4(ip));
            assert!(almost_eq(score, *want), "{ip}: score {score}, want {want}");
            assert_eq!(tree.count(), i + 1);
        }
    }

    #[test]
    fn insert_ipv6() {
        let mut tree = IpTree::new(128);
        for (i, (ip, want)) in IPV6_OPS.iter().enumerate() {
            let score = tree.insert(&v6(ip));
            assert!(almost_eq(score, *want), "{ip}: score {score}, want {want}");
            assert_eq!(tree.count(), i + 1);
        }
    }

    #[test]
    fn score_matches_insert() {
        for bits in [32u8, 128] {
            let mut tree = IpTree::new(bits);
            for _ in 0..1000 {
                let ip: [u8; 16] = rand::random();
                let ip = &ip[..bits as usize / 8];
                let (before, _) = tree.score_node(ip);
                let inserted = tree.insert(ip);
                assert_eq!(before, inserted, "score() != insert() for {ip:?}");
            }
        }
    }

    #[test]
    fn remove_ipv4() {
        let mut tree = IpTree::new(32);
        for (ip, _) in IPV4_OPS {
            tree.insert(&v4(ip));
        }
        for i in (0..IPV4_OPS.len()).rev() {
            let (ip, want) = IPV4_OPS[i];
            tree.remove(&v4(ip));
            let (score, _) = tree.score_node(&v4(ip));
            assert!(almost_eq(score, want), "{ip}: score {score}, want {want}");
            assert_eq!(tree.count(), i);
        }
        assert!(tree.is_empty_tree());
    }

    #[test]
    fn bounds_on_path() {
        let now = Instant::now();
        let mut tree = IpTree::new(32);
        for i in 1..=60u8 {
            tree.insert(&[203, 0, 113, i]);
        }
        let target = [203u8, 0, 113, 200];
        let (score, depth) = tree.score_node(&target);
        assert!(score > 0.0);
        assert!(depth > 0);
        tree.bump_bound(&target, depth, Duration::from_secs(30), now);
        assert_eq!(tree.path_floor(&target, now), Duration::from_secs(30));
        assert_eq!(
            tree.path_floor(&target, now + Duration::from_secs(10)),
            Duration::from_secs(20)
        );
        // An address under a different prefix does not see the floor.
        assert_eq!(tree.path_floor(&[8, 8, 8, 8], now), Duration::ZERO);
        // Freeing the branch drops the bound.
        for i in 1..=60u8 {
            tree.remove(&[203, 0, 113, i]);
        }
        assert!(tree.is_empty_tree());
        assert_eq!(tree.path_floor(&target, now), Duration::ZERO);
    }
}

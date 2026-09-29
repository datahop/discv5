//! Address classification mirroring go-ethereum's `p2p/netutil`, used by the registrar
//! to decide which records may be relayed to a requester and whether a REGTOPIC record
//! is consistent with the packet source, and by the registration and search tables to
//! limit the nodes per subnet.

use crate::Enr;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

// Special-use IPv4 ranges (RFC 5735, IANA special registry): never valid targets.
const SPECIAL_V4: &[(Ipv4Addr, u8)] = &[
    (Ipv4Addr::new(0, 0, 0, 0), 8),
    (Ipv4Addr::new(192, 0, 0, 0), 29),
    (Ipv4Addr::new(192, 0, 0, 9), 32),
    (Ipv4Addr::new(192, 0, 0, 170), 32),
    (Ipv4Addr::new(192, 0, 0, 171), 32),
    (Ipv4Addr::new(192, 0, 2, 0), 24),
    (Ipv4Addr::new(192, 31, 196, 0), 24),
    (Ipv4Addr::new(192, 52, 193, 0), 24),
    (Ipv4Addr::new(192, 88, 99, 0), 24),
    (Ipv4Addr::new(192, 175, 48, 0), 24),
    (Ipv4Addr::new(198, 18, 0, 0), 15),
    (Ipv4Addr::new(198, 51, 100, 0), 24),
    (Ipv4Addr::new(203, 0, 113, 0), 24),
    (Ipv4Addr::new(255, 255, 255, 255), 32),
];

const SPECIAL_V6: &[(Ipv6Addr, u8)] = &[
    (Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 0), 64),
    (Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2001, 1, 0, 0, 0, 0, 0, 1), 128),
    (Ipv6Addr::new(0x2001, 2, 0, 0, 0, 0, 0, 0), 48),
    (Ipv6Addr::new(0x2001, 3, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2001, 4, 0x112, 0, 0, 0, 0, 0), 48),
    (Ipv6Addr::new(0x2001, 5, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2001, 0x10, 0, 0, 0, 0, 0, 0), 28),
    (Ipv6Addr::new(0x2001, 0x20, 0, 0, 0, 0, 0, 0), 28),
    (Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16),
];

/// Converts an IPv4-mapped IPv6 address to IPv4, as Go's `netip.Addr.Unmap` does.
pub(crate) fn unmap(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

fn in_v4(ip: Ipv4Addr, net: Ipv4Addr, bits: u8) -> bool {
    (u32::from(ip) ^ u32::from(net)) >> (32 - bits) == 0
}

fn in_v6(ip: Ipv6Addr, net: Ipv6Addr, bits: u8) -> bool {
    (u128::from(ip) ^ u128::from(net)) >> (128 - bits) == 0
}

/// Loopback, private or link-local unicast.
pub(crate) fn is_lan(ip: IpAddr) -> bool {
    match unmap(ip) {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unique_local() || v6.is_unicast_link_local(),
    }
}

/// Multicast, broadcast, documentation and other special-use ranges.
pub(crate) fn is_special_network(ip: IpAddr) -> bool {
    match unmap(ip) {
        IpAddr::V4(v4) => {
            v4.is_multicast() || SPECIAL_V4.iter().any(|&(net, bits)| in_v4(v4, net, bits))
        }
        IpAddr::V6(v6) => {
            v6.is_multicast() || SPECIAL_V6.iter().any(|&(net, bits)| in_v6(v6, net, bits))
        }
    }
}

fn is_loopback(ip: IpAddr) -> bool {
    unmap(ip).is_loopback()
}

/// Reports whether `addr`, relayed from `sender`, is a valid connection target: special
/// networks never are, loopback only from a loopback sender, LAN only from a LAN sender.
pub(crate) fn check_relay_addr(sender: IpAddr, addr: IpAddr) -> Result<(), &'static str> {
    if unmap(addr).is_unspecified() {
        return Err("zero address");
    }
    if is_special_network(addr) {
        return Err("special network");
    }
    if is_loopback(addr) && !is_loopback(sender) {
        return Err("loopback address from non-loopback host");
    }
    if is_lan(addr) && !is_lan(sender) {
        return Err("LAN address from WAN host");
    }
    Ok(())
}

fn is_routable(ip: IpAddr) -> bool {
    !unmap(ip).is_unspecified() && !is_special_network(ip) && !is_lan(ip)
}

/// Validates a REGTOPIC record against the packet source. Searchers dial the stored
/// record, so its address for the source family must equal the source, and an address
/// of the other family, which cannot be verified, must at least be publicly routable.
pub(crate) fn check_regtopic_record_ip(enr: &Enr, src: IpAddr) -> Result<(), &'static str> {
    let src = unmap(src);
    let (claimed, other) = match src {
        IpAddr::V4(_) => (enr.ip4().map(IpAddr::V4), enr.ip6().map(IpAddr::V6)),
        IpAddr::V6(_) => (enr.ip6().map(IpAddr::V6), enr.ip4().map(IpAddr::V4)),
    };
    let Some(claimed) = claimed else {
        return Err("no address for the source address family");
    };
    if unmap(claimed) != src {
        return Err("address does not match packet source");
    }
    if other.is_some_and(|ip| !is_routable(ip)) {
        return Err("unverifiable address is not publicly routable");
    }
    Ok(())
}

/// The address a record is dialled at: the "most global" of its IPv4 and IPv6 entries,
/// IPv4 winning ties, as go-ethereum's `enode.Node` selects it.
pub(crate) fn preferred_ip(enr: &Enr) -> Option<IpAddr> {
    fn locality(ip: IpAddr) -> u8 {
        match unmap(ip) {
            ip if ip.is_unspecified() => 0,
            ip if ip.is_loopback() => 1,
            IpAddr::V4(v4) if v4.is_link_local() => 2,
            IpAddr::V6(v6) if v6.is_unicast_link_local() => 2,
            IpAddr::V4(v4) if v4.is_private() => 3,
            IpAddr::V6(v6) if v6.is_unique_local() => 3,
            _ => 4,
        }
    }
    let v4 = enr.ip4().map(IpAddr::V4).filter(|ip| !ip.is_multicast());
    let v6 = enr
        .ip6()
        .map(|ip| unmap(IpAddr::V6(ip)))
        .filter(|ip| !ip.is_multicast());
    match (v4, v6) {
        (Some(a), Some(b)) => Some(if locality(a) >= locality(b) { a } else { b }),
        (a, b) => a.or(b),
    }
}

/// The UDP port that goes with `preferred_ip`: an IPv6 endpoint falls back to the
/// IPv4 port entry when the record has no IPv6 one.
pub(crate) fn preferred_udp_port(enr: &Enr) -> Option<u16> {
    match preferred_ip(enr)? {
        IpAddr::V4(_) => enr.udp4(),
        IpAddr::V6(_) => enr.udp6().or(enr.udp4()),
    }
}

/// The first `bits` bits of an address, tagged with its family.
fn prefix(ip: IpAddr, bits: u8) -> (bool, u128) {
    match unmap(ip) {
        IpAddr::V4(v4) => {
            let bits = u32::from(bits.min(32));
            let mask = u32::MAX.checked_shl(32 - bits).unwrap_or(0);
            (false, u128::from(u32::from(v4) & mask))
        }
        IpAddr::V6(v6) => {
            let bits = u32::from(bits.min(128));
            let mask = u128::MAX.checked_shl(128 - bits).unwrap_or(0);
            (true, u128::from(v6) & mask)
        }
    }
}

/// Reports whether two addresses of the same family share a prefix of `bits` bits.
pub(crate) fn same_net(bits: u8, a: IpAddr, b: IpAddr) -> bool {
    prefix(a, bits) == prefix(b, bits)
}

/// Tracks addresses, allowing at most `limit` of them in the same `subnet`-bit prefix.
#[derive(Debug, Clone)]
pub(crate) struct DistinctNetSet {
    subnet: u8,
    pub(crate) limit: usize,
    members: HashMap<(bool, u128), usize>,
}

impl DistinctNetSet {
    pub(crate) fn new(subnet: u8, limit: usize) -> Self {
        DistinctNetSet {
            subnet,
            limit,
            members: HashMap::new(),
        }
    }

    /// Adds an address. Returns false, and adds nothing, when its subnet is full.
    pub(crate) fn add(&mut self, ip: IpAddr) -> bool {
        let count = self.members.entry(prefix(ip, self.subnet)).or_insert(0);
        if *count < self.limit {
            *count += 1;
            return true;
        }
        if *count == 0 {
            self.members.remove(&prefix(ip, self.subnet));
        }
        false
    }

    pub(crate) fn remove(&mut self, ip: IpAddr) {
        let key = prefix(ip, self.subnet);
        if let Some(count) = self.members.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                self.members.remove(&key);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.members.values().sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enr::CombinedKey;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn lan_and_special() {
        assert!(is_lan(ip("127.0.0.1")));
        assert!(is_lan(ip("10.1.2.3")));
        assert!(is_lan(ip("192.168.0.1")));
        assert!(is_lan(ip("169.254.1.1")));
        assert!(is_lan(ip("::1")));
        assert!(is_lan(ip("fe80::1")));
        assert!(is_lan(ip("fd00::1")));
        assert!(is_lan(ip("::ffff:10.0.0.1")));
        assert!(!is_lan(ip("8.8.8.8")));
        assert!(!is_lan(ip("2001:4860::8888")));

        assert!(is_special_network(ip("203.0.113.5")));
        assert!(is_special_network(ip("198.18.1.1")));
        assert!(is_special_network(ip("224.0.0.1")));
        assert!(is_special_network(ip("2001:db8::1")));
        assert!(is_special_network(ip("2002::1")));
        assert!(!is_special_network(ip("2001:4860::8888")));
        assert!(!is_special_network(ip("8.8.8.8")));
    }

    #[test]
    fn relay_rules() {
        assert!(check_relay_addr(ip("1.2.3.4"), ip("8.8.8.8")).is_ok());
        assert!(check_relay_addr(ip("1.2.3.4"), ip("0.0.0.0")).is_err());
        assert!(check_relay_addr(ip("1.2.3.4"), ip("203.0.113.1")).is_err());
        assert!(check_relay_addr(ip("1.2.3.4"), ip("127.0.0.1")).is_err());
        assert!(check_relay_addr(ip("127.0.0.1"), ip("127.0.0.1")).is_ok());
        assert!(check_relay_addr(ip("1.2.3.4"), ip("10.0.0.1")).is_err());
        assert!(check_relay_addr(ip("10.0.0.2"), ip("10.0.0.1")).is_ok());
        assert!(check_relay_addr(ip("::ffff:10.0.0.2"), ip("10.0.0.1")).is_ok());
    }

    #[test]
    fn regtopic_record_ip() {
        let key = CombinedKey::generate_secp256k1();
        let enr = Enr::builder()
            .ip4(Ipv4Addr::new(1, 2, 3, 4))
            .udp4(30303)
            .build(&key)
            .unwrap();
        assert!(check_regtopic_record_ip(&enr, ip("1.2.3.4")).is_ok());
        assert!(check_regtopic_record_ip(&enr, ip("::ffff:1.2.3.4")).is_ok());
        assert!(check_regtopic_record_ip(&enr, ip("1.2.3.5")).is_err());
        assert!(check_regtopic_record_ip(&enr, ip("2001:4860::1")).is_err());

        let both = Enr::builder()
            .ip4(Ipv4Addr::new(1, 2, 3, 4))
            .ip6(Ipv6Addr::LOCALHOST)
            .udp4(30303)
            .build(&key)
            .unwrap();
        // The unverifiable IPv6 entry is not routable.
        assert!(check_regtopic_record_ip(&both, ip("1.2.3.4")).is_err());
        assert!(check_regtopic_record_ip(&both, ip("::1")).is_ok());

        assert_eq!(preferred_ip(&both), Some(ip("1.2.3.4")));
        assert_eq!(preferred_ip(&enr), Some(ip("1.2.3.4")));
        assert_eq!(preferred_ip(&Enr::builder().build(&key).unwrap()), None);
        assert_eq!(preferred_udp_port(&enr), Some(30303));
        assert_eq!(preferred_udp_port(&both), Some(30303));
    }

    #[test]
    fn distinct_net_set() {
        let mut set = DistinctNetSet::new(24, 2);
        assert!(set.add(ip("1.2.3.4")));
        assert!(set.add(ip("1.2.3.5")));
        assert!(!set.add(ip("1.2.3.6")));
        assert!(set.add(ip("1.2.4.6")));
        // A mapped address counts in the IPv4 subnet.
        assert!(!set.add(ip("::ffff:1.2.3.7")));
        assert!(set.add(ip("2001:4860::1")));
        assert!(set.add(ip("2001:48ff::1")));
        assert!(!set.add(ip("2001:4800::1")));
        assert_eq!(set.len(), 5);

        set.remove(ip("1.2.3.9"));
        assert!(set.add(ip("1.2.3.6")));
        set.remove(ip("9.9.9.9"));
        assert_eq!(set.len(), 5);

        assert!(same_net(24, ip("1.2.3.4"), ip("1.2.3.200")));
        assert!(!same_net(24, ip("1.2.3.4"), ip("1.2.4.4")));
        assert!(!same_net(24, ip("1.2.3.4"), ip("102:300::")));
    }
}

//! LAN classification mirroring go-ethereum's `p2p/netutil.AddrIsLAN`, shared by the
//! kbucket IP filter and topic discovery.

use std::net::IpAddr;

/// Converts an IPv4-mapped IPv6 address to IPv4, as Go's `netip.Addr.Unmap` does.
pub(crate) fn unmap(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

/// Loopback, private or link-local unicast.
pub(crate) fn is_lan(ip: IpAddr) -> bool {
    match unmap(ip) {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unique_local() || v6.is_unicast_link_local(),
    }
}

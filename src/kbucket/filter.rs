//! Provides a trait that can be implemented to apply a filter to a table or bucket.

use crate::{Enr, netutil::is_lan};

pub trait Filter<TVal: Eq>: FilterClone<TVal> + Send + Sync {
    fn filter(
        &self,
        value_to_be_inserted: &TVal,
        other_vals: &mut dyn Iterator<Item = &TVal>,
    ) -> bool;
}

/// Allow the trait objects to be cloneable.
pub trait FilterClone<TVal: Eq> {
    fn clone_box(&self) -> Box<dyn Filter<TVal>>;
}

impl<T, TVal: Eq> FilterClone<TVal> for T
where
    T: 'static + Filter<TVal> + Clone,
{
    fn clone_box(&self) -> Box<dyn Filter<TVal>> {
        Box::new(self.clone())
    }
}

impl<TVal: Eq> Clone for Box<dyn Filter<TVal>> {
    fn clone(&self) -> Box<dyn Filter<TVal>> {
        self.clone_box()
    }
}

// Implementation of an IP filter for buckets and for tables

/// Number of permitted nodes in the same /24 subnet per table.
const MAX_NODES_PER_SUBNET_TABLE: usize = 10;
/// The number of nodes permitted in the same /24 subnet per bucket.
const MAX_NODES_PER_SUBNET_BUCKET: usize = 2;

#[derive(Clone)]
pub(crate) struct IpTableFilter;

impl Filter<Enr> for IpTableFilter {
    fn filter(
        &self,
        value_to_be_inserted: &Enr,
        other_vals: &mut dyn Iterator<Item = &Enr>,
    ) -> bool {
        ip_filter(value_to_be_inserted, other_vals, MAX_NODES_PER_SUBNET_TABLE)
    }
}

#[derive(Clone)]
pub(crate) struct IpBucketFilter;

impl Filter<Enr> for IpBucketFilter {
    fn filter(
        &self,
        value_to_be_inserted: &Enr,
        other_vals: &mut dyn Iterator<Item = &Enr>,
    ) -> bool {
        ip_filter(
            value_to_be_inserted,
            other_vals,
            MAX_NODES_PER_SUBNET_BUCKET,
        )
    }
}

fn ip_filter(
    value_to_be_inserted: &Enr,
    other_vals: &mut dyn Iterator<Item = &Enr>,
    limit: usize,
) -> bool {
    if let Some(ip) = value_to_be_inserted.ip4() {
        // LAN nodes are exempt, as in go-ethereum's table. A /24 lies entirely inside or
        // outside the LAN ranges, so LAN nodes never count against a non-LAN one.
        if is_lan(ip.into()) {
            return true;
        }
        let mut count = 0;
        for enr in other_vals {
            // Ignore duplicates
            if enr == value_to_be_inserted {
                continue;
            }

            // Count the same /24 subnet
            if let Some(other_ip) = enr.ip4()
                && other_ip.octets()[0..3] == ip.octets()[0..3]
            {
                count += 1;
            }
            if count >= limit {
                return false;
            }
        }
    }
    // No IP, so no restrictions
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use enr::CombinedKey;
    use std::net::Ipv4Addr;

    fn enr(ip: Ipv4Addr) -> Enr {
        Enr::builder()
            .ip4(ip)
            .udp4(9000)
            .build(&CombinedKey::generate_secp256k1())
            .unwrap()
    }

    fn subnet(a: u8, b: u8, c: u8, n: u8) -> Vec<Enr> {
        (1..=n).map(|d| enr(Ipv4Addr::new(a, b, c, d))).collect()
    }

    /// Returns how many of `nodes`, inserted in order, `filter` admits.
    fn admitted(filter: &dyn Filter<Enr>, nodes: &[Enr]) -> usize {
        let mut table: Vec<Enr> = Vec::new();
        for node in nodes {
            if filter.filter(node, &mut table.iter()) {
                table.push(node.clone());
            }
        }
        table.len()
    }

    #[test]
    fn lan_subnet_is_exempt() {
        let nodes = subnet(172, 16, 0, 12);
        assert_eq!(admitted(&IpTableFilter, &nodes), 12);
        assert_eq!(admitted(&IpBucketFilter, &nodes[..3]), 3);
    }

    #[test]
    fn global_subnet_is_limited() {
        let nodes = subnet(203, 0, 113, 11);
        let table: Vec<Enr> = nodes[..10].to_vec();
        assert!(!IpTableFilter.filter(&nodes[10], &mut table.iter()));
        assert_eq!(admitted(&IpTableFilter, &nodes), MAX_NODES_PER_SUBNET_TABLE);

        let bucket: Vec<Enr> = nodes[..2].to_vec();
        assert!(!IpBucketFilter.filter(&nodes[2], &mut bucket.iter()));
        assert_eq!(
            admitted(&IpBucketFilter, &nodes[..3]),
            MAX_NODES_PER_SUBNET_BUCKET
        );
    }

    #[test]
    fn lan_nodes_do_not_count_against_global() {
        let mut table = subnet(10, 0, 0, 12);
        table.extend(subnet(203, 0, 113, 9));
        let new = enr(Ipv4Addr::new(203, 0, 113, 200));
        assert!(IpTableFilter.filter(&new, &mut table.iter()));
        table.push(new);
        let refused = enr(Ipv4Addr::new(203, 0, 113, 201));
        assert!(!IpTableFilter.filter(&refused, &mut table.iter()));
    }
}

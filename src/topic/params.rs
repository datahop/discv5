//! The TopDisc parameters and named constants of the spec (discv5-theory.md), named as
//! there.

use std::time::Duration;

/// Advertisement lifetime. Theory: Parameters, Ad Cache.
pub const E: Duration = Duration::from_secs(15 * 60);
/// Registrar ad cache capacity, a hard limit. Theory: Parameters, Ad Cache.
pub const C: usize = 1000;
/// Registration window after a ticket's waiting time. Theory: Parameters, Tickets.
pub const DELTA: Duration = Duration::from_secs(5);
/// Late grace after the window in which a ticket is still credited. Theory: Parameters, Tickets.
pub const EPSILON: Duration = Duration::from_secs(10);
/// Admission slack and minimum quote. Theory: Parameters, Admission Control.
pub const W_MIN: Duration = Duration::from_secs(1);
/// Base waiting-time factor, `base(0) = W_BASE * E`. Theory: Parameters, Waiting-Time Function.
pub const W_BASE: f64 = 0.1;
/// Occupancy exponent of the waiting-time function. Theory: Parameters, Waiting-Time Function.
pub const P_OCC: i32 = 10;
/// Safety constant of the waiting-time function. Theory: Parameters, Waiting-Time Function.
pub const G: f64 = 1e-7;
/// Cap on a computed waiting time and on recorded lower bounds. Theory: Parameters, Waiting-Time Function.
pub const MAX_WAIT_TIME: Duration = Duration::from_secs(24 * 3600);
/// Maximum number of advertisements returned for one topic query. Theory: Parameters, Advertisement Yield.
pub const F_RETURN: usize = 16;
/// Target number of waiting or registered registrars per registration bucket. Theory: Parameters, Advertisement Placement.
pub const K_REGISTER: usize = 10;
/// Number of registration-table buckets. Theory: Parameters, Service Tables.
pub const REG_TABLE_DEPTH: usize = 18;
/// Standby candidates per registration bucket. Theory: Service Tables.
pub const REG_STANDBY_LIMIT: usize = 20;
/// Largest single or total quoted wait before an attempt is abandoned. Theory: Parameters, Registration Procedure.
pub const REG_ATTEMPT_BUDGET: Duration = Duration::from_secs(E.as_secs() * 3 / 2);
/// TTL of the advertiser's failed set. Theory: Registration Procedure.
pub const ASKED_FAILED_TTL: Duration = E;
/// TTL of the advertiser's overloaded set. Theory: Registration Procedure.
pub const OVERLOADED_TTL: Duration = E;
/// Number of search-table buckets. Theory: Parameters, Service Tables.
pub const SEARCH_TABLE_DEPTH: usize = 18;
/// Candidates per search bucket. Theory: Service Tables.
pub const SEARCH_BUCKET_SIZE: usize = 8;
/// Distinct registrars queried per `/24` in a lookup, each counted for `E`. Theory: Parameters, Registrar Selection.
pub const SEARCH_SUBNET_LIMIT: usize = 10;
/// Yield threshold for staying at the responding bucket. Theory: Parameters, Advertisement Yield.
pub const SEARCH_YIELD_FLOOR: usize = 8;
/// Samples retained per bucket during a pass. Theory: Advertisement Yield.
pub const SEARCH_YIELD_WINDOW: usize = 4;
/// Samples a bucket needs before a response moves the active bucket. Theory: Advertisement Yield.
pub const SEARCH_MIN_SAMPLES: usize = 2;
/// Largest move of the active bucket toward the topic. Theory: Advertisement Yield.
pub const SEARCH_MAX_JUMP: usize = 4;
/// Range around the active bucket for aux requests and fallback replies. Theory: Parameters, Advertisement Yield.
pub const SEARCH_AUX_RADIUS: usize = 1;
/// Completed queries after which a pass ends. Theory: Pass Transitions.
pub const SEARCH_PASS_QUERIES: usize = 32;
/// Least spacing between the starts of two passes. Theory: Pass Transitions.
pub const SEARCH_PASS_GAP: Duration = Duration::from_secs(2);
/// Cap on the spacing between passes after idle passes. Theory: Pass Transitions.
pub const SEARCH_PASS_BACKOFF: Duration = Duration::from_secs(60);
/// Release threshold: registrars that replied with results. Theory: Parameters, Result Delivery.
pub const SEARCH_MIX_REGISTRARS: usize = 10;
/// Release threshold: wait of the first queued result. Theory: Parameters, Result Delivery.
pub const SEARCH_MIX_WAIT: Duration = Duration::from_secs(5);
/// Topic-distances a registrar serves with auxiliary ENRs. Theory: Auxiliary ENR Selection.
pub const AUX_ENR_LIMIT: usize = 8;
/// How long a registrar remembers a credited ticket, `DELTA + EPSILON`. Theory: Tickets.
pub const USED_TICKET_TTL: Duration = Duration::from_secs(DELTA.as_secs() + EPSILON.as_secs());
/// Credited tickets a registrar remembers. Theory: Tickets.
pub const USED_TICKET_LIMIT: usize = 16_384;
/// Response packets an advertiser waits for per REGTOPIC. Theory: Response Splitting.
pub const REG_RESP_LIMIT: usize = 8;
/// Response packets a discoverer waits for per TOPICQUERY. Theory: Response Splitting.
pub const QUERY_RESP_LIMIT: usize = 16;
/// ENRs per NODES or TOPICNODES response. Theory: Response Splitting.
pub const ENRS_PER_PACKET: usize = 3;
/// ENRs above which a TOPICNODES is not decoded. Theory: Response Splitting.
pub const MAX_TOPIC_NODES: usize = 16;
/// Topic-distances above which a request is invalid. Theory: Response Splitting.
pub const MAX_TOPIC_DISTANCES: usize = 256;
/// Wait per response packet. Theory: Response Splitting.
pub const RESP_TIMEOUT: Duration = Duration::from_millis(700);

#[cfg(test)]
mod tests {
    use super::*;

    // The Parameters table of the spec and the named constants, as literals.
    #[test]
    fn spec_parameters() {
        let min = |m: u64| Duration::from_secs(m * 60);
        let sec = Duration::from_secs;
        assert_eq!(E, min(15));
        assert_eq!(C, 1_000);
        assert_eq!(DELTA, sec(5));
        assert_eq!(EPSILON, sec(10));
        assert_eq!(W_MIN, sec(1));
        assert_eq!(W_BASE, 0.1);
        assert_eq!(P_OCC, 10);
        assert_eq!(G, 1e-7);
        assert_eq!(MAX_WAIT_TIME, sec(24 * 3600));
        assert_eq!(F_RETURN, 16);
        assert_eq!(K_REGISTER, 10);
        assert_eq!(REG_TABLE_DEPTH, 18);
        assert_eq!(REG_STANDBY_LIMIT, 20);
        assert_eq!(REG_ATTEMPT_BUDGET, E.mul_f64(1.5));
        assert_eq!(ASKED_FAILED_TTL, E);
        assert_eq!(OVERLOADED_TTL, E);
        assert_eq!(SEARCH_TABLE_DEPTH, 18);
        assert_eq!(SEARCH_BUCKET_SIZE, 8);
        assert_eq!(SEARCH_SUBNET_LIMIT, 10);
        assert_eq!(SEARCH_YIELD_FLOOR, 8);
        assert_eq!(SEARCH_YIELD_WINDOW, 4);
        assert_eq!(SEARCH_MIN_SAMPLES, 2);
        assert_eq!(SEARCH_MAX_JUMP, 4);
        assert_eq!(SEARCH_AUX_RADIUS, 1);
        assert_eq!(SEARCH_PASS_QUERIES, 32);
        assert_eq!(SEARCH_PASS_GAP, sec(2));
        assert_eq!(SEARCH_PASS_BACKOFF, sec(60));
        assert_eq!(SEARCH_MIX_REGISTRARS, 10);
        assert_eq!(SEARCH_MIX_WAIT, sec(5));
        assert_eq!(AUX_ENR_LIMIT, 8);
        assert_eq!(USED_TICKET_TTL, DELTA + EPSILON);
        assert_eq!(USED_TICKET_LIMIT, 16_384);
        assert_eq!(REG_RESP_LIMIT, 8);
        assert_eq!(QUERY_RESP_LIMIT, 16);
        assert_eq!(ENRS_PER_PACKET, 3);
        assert_eq!(MAX_TOPIC_NODES, 16);
        assert_eq!(MAX_TOPIC_DISTANCES, 256);
        assert_eq!(RESP_TIMEOUT, Duration::from_millis(700));
    }
}

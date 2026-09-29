# TopDisc in sigp/discv5, phase 2: wire messages and registrar

Everything lives behind the Cargo feature `topic-discovery` (default off). The reference is
the Go implementation in the datahop/go-ethereum fork (`p2p/discover`: `v5wire/msg.go`,
`topicindex/{common,topictable,ticket,iptree}.go`, `v5_udp.go`, `table.go`); where the
devp2p spec and the Go code disagree, the Go code is followed (list below).

## Module map

| Go | Rust |
|---|---|
| `v5wire/msg.go`: `Regtopic`, `Regconfirmation`, `TopicQuery`, `TopicNodes`, ids 0x07..0x0a, decode bounds | `src/rpc.rs`: `RequestBody::{RegTopic, TopicQuery}`, `ResponseBody::{RegConfirmation, TopicNodes}`, encode/decode arms 7/8/9/10, `ResponseBody::total()`, shared `encode_enr_list` / `decode_enr_list` (also used by NODES) |
| `v5_udp.go` `handle()` dispatch | `src/service.rs` `handle_rpc_request` arms; unsolicited REGCONFIRMATION/TOPICNODES dropped in `handle_rpc_response` |
| `handleRegtopic`, `handleTopicQuery`, `collectTopicAuxNodes`, `packNodeRecords`, `waitTimeToMs` | `src/service/topic.rs` |
| `table.go` `collectOnePerDist` | `Service::collect_topic_aux_nodes` (one pass over `kbuckets.iter_ref()`, reservoir pick per distance) |
| `checkRegtopicRecordIP`, `netutil.CheckRelayAddr` / `IsLAN` / `IsSpecialNetwork`, `enode` preferred-IP rule | `src/topic/netutil.rs` |
| `dispatch()` topic expiry alarm | main loop arm `Service::topic_expiry_poll` (sleep until `TopicTable::next_expiry`) |
| `topicindex/common.go`: `Config` + defaults, `TopicID`, `TopicDiscovery` ENR entry, `SupportsTopicDiscovery` | `src/topic/config.rs` `TopicConfig` (`Default` = Go defaults); `src/topic/mod.rs` `TopicId`, `TOPIC_DISCOVERY_ENR_KEY = "ng"`, `TOPIC_DISCOVERY_VERSION = 1`, `supports_topic_discovery` |
| `topictable.go`: `TopicTable`, `waitTime`, `Register`, `Expire`, `RandomNodes`, `RemoveNode`, `waitTimeState`, `lowerBound` | `src/topic/table.rs` `TopicTable`; `src/topic/bound.rs` `LowerBound` |
| `iptree.go` | `src/topic/iptree.rs` |
| `ticket.go`: `Ticket`, `TicketSealer` | `src/topic/ticket.rs` |
| `UDPv5.topicTable` / `ticketSealer` | `Service::topic_table` / `ticket_sealer` |
| `ln.Set(topicindex.TopicDiscoveryVersion)` in `ListenV5` | `Discv5::new` inserts `ng = 1` into the local ENR when missing |
| topic config | `Config::topic`, `ConfigBuilder::topic_config` |
| handler multi-response accounting (NODES only) | `handler/mod.rs` uses `ResponseBody::total()`, so REGCONFIRMATION / TOPICNODES totals are honoured (phase 3 needs it) |

Clock: Go's `mclock` is replaced by `std::time::Instant` passed explicitly as `now` to the
table and sealer methods (testable without a clock trait). Tickets encode instants as
nanoseconds since the sealer's epoch.

## Wire format (Go layout)

- REGTOPIC 0x07: `[request-id, topic (32 bytes), ticket (bytes, empty on first attempt), ENR, [distance, ...]]`
- REGCONFIRMATION 0x08: `[request-id, total (uint8), ticket (empty = admitted), wait-time (ms; ad lifetime when admitted)]`
- TOPICQUERY 0x09: `[request-id, topic, [distance, ...]]`
- TOPICNODES 0x0a: `[request-id, total (uint8), [ENR, ...]]`
- Aux routing records ride in ordinary NODES 0x04 with the same request id and total.

Decode bounds from Go: at most 256 distances, at most 16 records in TOPICNODES, `total`
must fit a uint8, request id at most 8 bytes, no trailing data. The test vectors in
`src/rpc.rs` (`topic_tests`) were produced by running the Go encoder on the same inputs.

## Registrar behaviour

- REGTOPIC: unpack ticket (any ticket error: no reply), verify record signature, record id
  must equal the sender, record address for the source family must equal the packet source
  and an address of the other family must be routable; `waited = now - FirstIssued` (0
  without ticket); aux nodes: first 8 requested distances, one random `ng` record per
  distance from the routing table measured from the topic hash, passing the relay-address
  check against the requester; REGCONFIRMATION first, then NODES in chunks of 3,
  `total = 1 + chunks`. Ticket wait time in ms rounded up; on admission wait-time carries
  the ad lifetime.
- TOPICQUERY: NODES (aux) then TOPICNODES chunks of 3 with up to 16 random advertisers
  passing the relay check; a single empty TOPICNODES with `total = 1` when nothing is sent.
- Waiting time as in Go: `base = 0.1 * AdLifetime / (1 - c/C)^10`,
  `service = base * c(topic) / (c + 1)`, `ip = base * ipScore` (LAN addresses exempt), each
  floored by its lower bound (per topic, per IP prefix on the path), summed and rounded up
  to ns; admitted when the remaining wait is at most 1 s; bounds recorded when a ticket is
  issued; per-topic bound dropped with the topic's last ad, prefix bounds freed with their
  tree branch.
- Ticket: 32-byte HMAC-SHA256 over a 58-byte big-endian body (key id u16, topic, wait ns,
  last used ns, first issued ns) = 90 bytes; valid in `[last_used + wait, + 5 s)`; keys
  rotate after 100000 uses and are dropped after 6 h unused.
- Defaults: ad lifetime 15 min, ad cache 5000, table depths 10, reg bucket 5 (+20 standby),
  reg attempt timeout 22.5 min, search bucket 16, topic nodes limit 16, aux nodes limit 8.

## Spec vs Go (Go followed)

1. REGTOPIC field order: spec `[…, topic, ENR, ticket, …]`, Go `[…, topic, ticket, ENR, …]`.
2. ENR key: spec `topic-discovery`, Go `ng`.
3. Ticket binding: spec binds an advertisement digest (service, ENR, payload); Go binds
   the topic only.
4. Waiting time: spec adds a safety constant `G`; the Go fork dropped it.
5. `total`: spec unbounded, Go `uint8`.

## Interpretations and places where Go could not be matched exactly

1. Full cache (`topictable.go` `waitTime` / `Register`): occupancy is 0, `baseTime` is
   `+Inf` and the components `Inf` or `NaN`; the Go float-to-`Duration` conversion is
   platform dependent (amd64: `MinInt64`, so the wait gate never fires and `Register`
   returns `NextExpiryTime - now`; arm64 would saturate). Implemented the amd64 outcome
   explicitly: with a full cache the node is quoted the next expiry, no bounds recorded.
2. `TopicTable.Nodes` / `RandomNodes` iterate Go's per-topic list newest-first and
   reservoir-sample; Rust keeps a map per topic and samples uniformly. Same distribution,
   different order; not wire-visible.
3. `Add` in Go can hold duplicate (node, topic) entries; Rust `add` is idempotent per
   (node, topic). `Register` never produces duplicates in either.
4. The IP used for the relay check follows Go's `enode.Node` preferred endpoint rule
   (validity, locality score, IPv4 on ties, 4-in-6 unmapped); hostname handling is not
   applicable.
5. `iptree.go` can record a bound on the root (when the longest-prefix-match node is the
   root) that `pathFloor` never reads; reproduced as is.
6. `Discv5::new` adds `ng = 1` to the local ENR without bumping the sequence number
   (consistent with `Discv5::enr_insert`); Go's `LocalNode` bumps it. Records passed with
   `ng` already set are left alone.
7. Ticket instants are ns since the sealer's epoch rather than `mclock.AbsTime`; opaque
   to peers, same size and field order.
8. `match_request` does not yet accept NODES for REGTOPIC / TOPICQUERY: the service's
   NODES response path assumes FINDNODE (`unreachable!` otherwise). Phase 3 changes it.

## Public API proposed for phase 3

```rust
impl Discv5 {
    /// Keep an advertisement for `topic` alive: runs the registration walk against
    /// `ng` registrars around the topic hash until stopped.
    pub fn register_topic(&self, topic: TopicId) -> Result<(), Error>;
    pub fn stop_topic_registration(&self, topic: TopicId) -> Result<(), Error>;
    /// Look up advertisers of `topic`; resolves with up to `count` distinct records.
    pub fn topic_search(&self, topic: TopicId, count: usize)
        -> impl Future<Output = Result<Vec<Enr>, QueryError>> + 'static;
    /// Designated-peer primitives, used by the walk and the search, and by tests.
    pub fn send_regtopic(&self, contact: NodeContact, topic: TopicId, ticket: Vec<u8>, distances: Vec<u64>)
        -> impl Future<Output = Result<RegConfirmation, RequestError>> + 'static;
    pub fn send_topic_query(&self, contact: NodeContact, topic: TopicId, distances: Vec<u64>)
        -> impl Future<Output = Result<TopicQueryResult, RequestError>> + 'static;
    /// Advertisers this node holds for `topic` (registrar side).
    pub fn local_topic_nodes(&self, topic: TopicId) -> Vec<Enr>;
}
pub struct RegConfirmation { pub admitted: bool, pub ticket: Vec<u8>, pub wait_time: Duration, pub aux: Vec<Enr> }
pub struct TopicQueryResult { pub advertisers: Vec<Enr>, pub aux: Vec<Enr> }
// Event additions: TopicRegistered { topic, registrar }, TopicNodesFound { topic, nodes }.
```

Service side: `ServiceRequest::{RegTopic, TopicQuery, RegisterTopic, StopTopic, TopicSearch}`,
`CallbackResponse::{RegConfirmation, TopicNodes}`, `active_nodes_responses` generalised to
collect NODES + TOPICNODES per request id, `match_request` extended so NODES answers
REGTOPIC / TOPICQUERY, new modules `topic/registration.rs` (registration table, walk,
attempt timeout, eviction on routing-table removal via `TopicTable::remove_node`) and
`topic/search.rs` (search table, adaptive distance, result filters).

## Left out deliberately

- Registration walk (`registration.go`) and search (`search.go`, `searchfilter.go`),
  including the registration and search tables: phase 3. The related `TopicConfig`
  fields are present with Go defaults but unused.
- Testbed instrumentation: `wireStats`, `WaitTimeStats`, `AdmissionEvent`,
  `TopicSearchStats`, `TrackAdmissions` / `WasAdmitted`, `TopicQueryRcvCount`, metrics.
- Wiring `TopicTable::remove_node` to routing-table removals (Go `onDispatch` eviction).
- `Discv5`-level accessors for the ad cache.

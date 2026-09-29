# TopDisc in sigp/discv5, phases 2 and 3

Phase 2: wire messages and the registrar. Phase 3: the advertiser (registration walk), the
searcher (search table, result filter, adaptive search distance), the request and response
plumbing, the public API.

Everything lives behind the Cargo feature `topic-discovery` (default off). The reference is
the Go implementation in the datahop/go-ethereum fork (`p2p/discover`: `v5wire/msg.go`,
`topicindex/*.go`, `v5_topic.go`, `v5_udp.go`, `table.go`). Phase 3 was ported from the
worktree `geth-adaptive-wt` (head `0c906e4fd`). Where the devp2p spec and the Go code
disagree, the Go code is followed (list below).

## Module map

| Go | Rust |
|---|---|
| `v5wire/msg.go`: `Regtopic`, `Regconfirmation`, `TopicQuery`, `TopicNodes`, ids 0x07..0x0a, decode bounds | `src/rpc.rs`: `RequestBody::{RegTopic, TopicQuery}`, `ResponseBody::{RegConfirmation, TopicNodes}`, encode/decode arms 7/8/9/10, `ResponseBody::total()`, `RequestBody::is_topic_request()`, shared `encode_enr_list` / `decode_enr_list` (also used by NODES) |
| `v5_udp.go` `handle()` dispatch | `src/service.rs` `handle_rpc_request` arms |
| `handleRegtopic`, `handleTopicQuery`, `collectTopicAuxNodes`, `packNodeRecords`, `waitTimeToMs` | `src/service/topic.rs` |
| `table.go` `collectOnePerDist` | `Service::collect_topic_aux_nodes` (one pass over `kbuckets.iter_ref()`, reservoir pick per distance) |
| `checkRegtopicRecordIP`, `netutil.CheckRelayAddr` / `IsLAN` / `IsSpecialNetwork`, `enode` preferred endpoint rule | `src/topic/netutil.rs` (`preferred_ip`, `preferred_udp_port`) |
| `netutil.DistinctNetSet`, `netutil.SameNet` | `src/topic/netutil.rs` `DistinctNetSet`, `same_net` |
| `dispatch()` topic expiry alarm | main loop arm `Service::topic_wakeup_poll`: sleeps until the earliest of ad expiry, registration attempt, search pass, search deadline; then `Service::topic_tick` |
| `topicindex/common.go`: `Config` + defaults, `TopicID`, `TopicDiscovery` ENR entry, `SupportsTopicDiscovery` | `src/topic/config.rs` `TopicConfig` (with `search_yield_floor`); `src/topic/mod.rs` `TopicId`, `TOPIC_DISCOVERY_ENR_KEY = "ng"`, `TOPIC_DISCOVERY_VERSION = 1`, `supports_topic_discovery` |
| `topictable.go`, `iptree.go`, `ticket.go` | `src/topic/table.rs` `TopicTable`, `src/topic/bound.rs` `LowerBound`, `src/topic/iptree.rs`, `src/topic/ticket.rs` |
| `UDPv5.topicTable` / `ticketSealer` | `Service::topic_table` (`Arc<RwLock<TopicTable>>`, shared with `Discv5`) / `ticket_sealer` |
| `ln.Set(topicindex.TopicDiscoveryVersion)` in `ListenV5` | `Discv5::new` inserts `ng = 1` into the local ENR when missing |
| topic config | `Config::topic`, `ConfigBuilder::topic_config` |
| `topicindex/registration.go`: `Registration`, `RegAttempt`, `RegAttemptState`, `regBucket`, `regHeap` | `src/topic/registration.rs`: `Registration<N>`, `RegAttempt<N>`, `RegAttemptState`; the heap is a `BTreeMap<(Instant, seq), NodeId>` |
| `topicindex/search.go`: `Search`, `searchBucket`, `QueryTarget`, `adaptiveTarget`, `observe`, `IsDone`, `BucketsWithFreeSpace` | `src/topic/search.rs`: `Search<N>` |
| `topicindex/searchfilter.go` | `src/topic/searchfilter.rs` `SearchFilter` |
| `enode.Node` as seen by the tables (`ID`, `Seq`, `IP`) | trait `topic::TopicNode`, implemented for `Enr`; tests use a node type with a chosen id |
| `v5_topic.go` `topicSystem` | `service::topic::TopicSystem` (field `Service::topics`) |
| `topicReg`, `registrationLoop`, `runRegistration`, `sendRequestsLoop`, `pause` | `TopicReg`, `Service::registration_step`, `Service::registration_response` |
| `topicSearch`, `runLoop`, `run`, `runRequests`, `topicSearchIterator` | `TopicSearch`, `Service::search_step`, `Service::search_response` |
| `filterTopicDiscovery`, `shuffleNodes`, `tab.allNodes()` | `capable`, `Service::topic_capable_nodes` |
| `UDPv5.regtopic`, `UDPv5.topicQuery`, `verifyResponseNode` | `Service::handle_topic_response`, `topic_request_failed`, `topic_request_done`, `verify_response_node`; responses in progress in `TopicSystem::responses` |
| `handleCallResponse` with `responseType = 0` | `Response::match_request`: NODES also answers REGTOPIC and TOPICQUERY; the service routes every response of a topic request to `handle_topic_response` before the FINDNODE path |
| `table.go` `nodeFeed` / `removedFeed`, `subscribeNodes`, `subscribeRemovedNodes`; `evictRemovedNodes`, `evictNode`, `evictTopicTableNode` | `kbucket::TableChange`, `KBucket::take_changes`, `KBucketsTable::take_changes`; `Service::topic_table_changes` at the start of every main loop iteration |
| `trackTopicRequest` | `Service::connection_updated(.., Disconnected)` on a failed request of a loop |
| `RegisterTopic`, `StopRegisterTopic`, `TopicSearch`, `LocalTopicNodes` | `Discv5::{register_topic, stop_topic_registration, topic_search, local_topic_nodes}` |

Clock: Go's `mclock` is replaced by `std::time::Instant` passed explicitly as `now` to the
table, sealer, registration, search and filter methods (testable without a clock trait).
Tickets encode instants as nanoseconds since the sealer's epoch.

## Wire format (Go layout)

- REGTOPIC 0x07: `[request-id, topic (32 bytes), ticket (bytes, empty on first attempt), ENR, [distance, ...]]`
- REGCONFIRMATION 0x08: `[request-id, total (uint8), ticket (empty = admitted), wait-time (ms; ad lifetime when admitted)]`
- TOPICQUERY 0x09: `[request-id, topic, [distance, ...]]`
- TOPICNODES 0x0a: `[request-id, total (uint8), [ENR, ...]]`
- Aux routing records ride in ordinary NODES 0x04 with the same request id and total.

Decode bounds from Go: at most 256 distances, at most 16 records in TOPICNODES, `total`
must fit a uint8, request id at most 8 bytes, no trailing data. The test vectors in
`src/rpc.rs` (`topic_tests`) were produced by running the Go encoder on the same inputs.
The Go field `OpID` is `rlp:"-"`: it is not on the wire.

## Registrar behaviour (phase 2)

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
- A node that leaves the routing table loses its ads (phase 3, see below).

## Advertiser behaviour (phase 3)

- Registration table per topic: `reg_table_depth` buckets, far to close,
  `index = min(256 - logdist(topic, id), depth - 1)`. Per bucket: at most
  `reg_bucket_size` attempts in state `Waiting` or `Registered`, at most
  `reg_bucket_standby_limit` in `Standby`, one non-LAN address per /24.
- `add_nodes`: skip the local node; a known node gets its record replaced when the
  sequence number is higher (the subnet accounting moves with the address, and the attempt
  is dropped when the new subnet is full); one node per source per bucket, for the life of
  the attempt of the source; then the standby limit and the subnet limit.
- An attempt goes `Standby` -> `Waiting` (promoted when the bucket has an active slot
  free) -> request -> ticket (wait `wait-time`, send the ticket) or registered. A quote
  above `reg_attempt_timeout`, or waiting times that add up to more, end the attempt. A
  lifetime above the local `ad_lifetime` is cut to it.
- Expiry as in Go: when the ad lifetime has passed, the attempt is removed and a standby
  node of the bucket is promoted. There is no renewal before expiry. The registrar comes
  back into the table only as an auxiliary node, through the routing table feed, or when
  the table is seeded again.
- Loop per topic (`TopicReg`): seed the table with all `ng` nodes of the routing table in
  random order; when the table is empty, seed again, not sooner than 2 s after the last
  seeding. One REGTOPIC in flight at a time. The next attempt is chosen when it is due and
  keeps its node, ticket and distance list from that time. A response adds the `ng`
  auxiliary nodes (source = the registrar), then handles ticket, admission or error.
- Distances in REGTOPIC: `Registration::buckets_with_free_space` (buckets whose standby
  list is not full), with the labels of the Go table (see interpretation 9).

## Searcher behaviour (phase 3)

- Search table per pass: `search_table_depth` buckets, far to close, `search_bucket_size`
  nodes per bucket (unasked and asked together), one non-LAN address per /24, one node per
  bucket from one response.
- Default search (`search_yield_floor = 0`): the target is a random unasked node of a
  random bucket of the pool. The pool holds the buckets with unasked nodes, far to close,
  up to and including the first one that has not answered a request. The pass ends when no
  unasked node is left.
- Adaptive search (`search_yield_floor > 0`): the target is an unasked node of the active
  bucket, or of the nearest bucket that has one (farther side first). Each reply records
  its ad count for the bucket of the registrar (window 4). With two or more samples, the
  lower and upper median decide: lower >= `topic_nodes_limit` moves one bucket farther,
  upper >= floor stays, upper = 0 jumps 4 closer, else
  `ceil(log2(floor / upper))` closer, at most 4. An asked node goes into the asked filter
  and leaves the table. A node that the asked filter knows goes to the spare list; one
  spare node is asked per pass when nothing else is left. The pass ends when the active
  bucket has no unasked node and two samples, or when nothing is left to ask; in the
  second case the active bucket moves one step closer. The distance list starts with the
  buckets around the active one.
- Filters: limit 50000 entries, lifetime = `ad_lifetime`, a record with a higher sequence
  number counts as not seen. The result filter holds the advertisers a search returned,
  the asked filter the registrars the adaptive passes asked. Both live as long as the
  search; the active bucket and the asked filter go from pass to pass.
- Loop per search (`TopicSearch`): a pass is seeded with all `ng` nodes of the routing
  table in random order, not sooner than 2 s after the last seeding. One TOPICQUERY in
  flight at a time. A response adds the `ng` auxiliary nodes and the `ng` advertisers. A
  failed request that brought no node drops the registrar from the pass; one that brought
  nodes counts as an answer.

## Request and response plumbing (phase 3)

- A topic request is over when the announced number of messages has arrived, capped at 8
  (REGTOPIC, `aux_nodes_limit`) or 16 (TOPICQUERY, `topic_nodes_limit`), and, for
  REGTOPIC, the REGCONFIRMATION is one of them. Records are capped at the same numbers per
  kind (aux, advertisers).
- A record of a response is dropped when it has no address, fails the relay check against
  the sender, has a UDP port <= 1024, or repeats an id of the same kind in the response.
- Timeouts come from the handler (`request_timeout`, `request_retries`). The messages that
  arrived before a failure are given to the loops with the error.

## Final public API

```rust
impl Discv5 {
    pub fn register_topic(&self, topic: TopicId) -> Result<(), Error>;
    pub fn stop_topic_registration(&self, topic: TopicId) -> Result<(), Error>;
    pub fn topic_search(&self, topic: TopicId, count: usize)
        -> impl Future<Output = Result<Vec<Enr>, QueryError>> + 'static + use<>;
    pub fn send_regtopic(&self, contact: NodeContact, topic: TopicId, ticket: Vec<u8>, distances: Vec<u64>)
        -> impl Future<Output = Result<RegConfirmation, RequestError>> + 'static + use<>;
    pub fn send_topic_query(&self, contact: NodeContact, topic: TopicId, distances: Vec<u64>)
        -> impl Future<Output = Result<TopicQueryResult, RequestError>> + 'static + use<>;
    pub fn local_topic_nodes(&self, topic: TopicId) -> Vec<Enr>;
}

// discv5::topic
pub type TopicId = [u8; 32];
pub struct RegConfirmation { pub admitted: bool, pub ticket: Vec<u8>, pub wait_time: Duration, pub aux: Vec<Enr> }
pub struct TopicQueryResult { pub advertisers: Vec<Enr>, pub aux: Vec<Enr> }
pub struct TopicConfig { /* phase 2 fields */ pub search_yield_floor: usize }
pub trait TopicNode: Clone { fn node_id(&self) -> NodeId; fn seq(&self) -> u64; fn ip(&self) -> Option<IpAddr>; }
pub struct Registration<N: TopicNode = Enr>;  pub struct RegAttempt<N>;  pub enum RegAttemptState { Standby, Waiting, Registered }
pub struct Search<N: TopicNode = Enr>;  pub struct SearchFilter;

// discv5::Event (non_exhaustive)
Event::TopicRegistered { topic: TopicId, registrar: NodeId }
Event::TopicNodesFound { topic: TopicId, nodes: Vec<Enr> }

// discv5::kbucket
pub enum TableChange<TNodeId> { Inserted(Key<TNodeId>), Removed(Key<TNodeId>) }
impl KBucketsTable { pub fn take_changes(&mut self) -> Vec<TableChange<TNodeId>>; }
```

Changes against the phase 2 proposal: none in the signatures of `Discv5` (the futures
carry `use<>` like the others of the crate). `active_nodes_responses` was not generalised:
topic responses have their own map, so the FINDNODE path is untouched. The internal
`CallbackResponse` has two more variants than proposed (`TopicRegistration`,
`TopicSearch`), which tie a request to its loop.

- `register_topic` and `stop_topic_registration` do not wait: they use `try_send` on the
  service channel (30 slots) and return `Error::Custom("Service channel full")` or
  `Error::ServiceChannelClosed` when that fails.
- `topic_search` ends with `count` distinct records, or at `Config::query_timeout` (60 s
  default) with the records found until then. `count = 0` ends at once. A search whose
  future is dropped ends at its next step.
- `send_regtopic` and `send_topic_query` do not check the `ng` entry of the contact and
  return the records of the response without the `ng` filter. A failure gives `Err`; the
  messages that arrived before it are dropped.
- Only the loops filter by `ng`: routing table nodes, auxiliary nodes, advertisers.

## Spec vs Go (Go followed)

1. REGTOPIC field order: spec `[…, topic, ENR, ticket, …]`, Go `[…, topic, ticket, ENR, …]`.
2. ENR key: spec `topic-discovery`, Go `ng`.
3. Ticket binding: spec binds an advertisement digest (service, ENR, payload); Go binds
   the topic only.
4. Waiting time: spec adds a safety constant `G`; the Go fork dropped it.
5. `total`: spec unbounded, Go `uint8`.
6. Service table: spec has 256 buckets `bᵢ(s)`; Go keeps a fixed number of buckets, far to
   close, and puts all closer nodes into the last one.
7. Seeding: spec drops nodes whose liveness is not verified; Go takes every entry of the
   routing table that carries `ng`.
8. Renewal: spec says advertisers should renew before expiry; Go does not (see above).
9. Waiting: spec says one waiting interval should not exceed `E`; Go ends the attempt when
   one quote, or the sum, exceeds `RegAttemptTimeout` (1.5 `E`).
10. Lookup: spec walks bucket by bucket with at most `K_lookup = 5` queries per bucket and
    ends at `F_lookup` or when no registrar is left; Go asks every node of the table (pool
    rule above) and starts a new pass after the last one; the adaptive search is not in
    the spec.
11. Go rules that the spec does not have: one node per source per bucket, one address per
    /24 per bucket, the standby list, UDP port > 1024 for records of a response.
12. Search results: Go drops advertisers whose record has no `ng` entry; the spec asks for
    a valid ad only.
13. Parameters: spec `F_return = 10`, `C = 1000`; Go 16 and 5000.

## Interpretations and places where Go could not be matched exactly

Phase 2:

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

Phase 3:

8. Table sizes. The Go worktree used for phase 3 has `regTableDepth = 18`,
   `searchTableDepth = 18`, `RegBucketSize = 10`, `SearchBucketSize = 8`. The defaults of
   `TopicConfig` stay as phase 2 set them (depth 10, 5, 16: the spec sizes of the Go
   commit `1e72b0066`). The depths are configuration in Rust, constants in Go. The ported
   unit tests use the sizes of the worktree. `Search::observe` uses
   `TopicConfig::topic_nodes_limit` where Go uses the constant `TopicNodesLimit`.
9. Labels of the registration buckets (`registration.go:137`). Go gives bucket `i` the
   label `256 - (depth - 1) + i`, while `bucketIndex` puts the nodes at distance 256 into
   bucket 0. `BucketsWithFreeSpace` thus lists the distances in ascending order and drops
   the mirror distance of a full bucket. Ported as is (test `free_space_labels`). Likely a
   bug, see the report of phase 3.
10. Attempt handles. Go passes `*RegAttempt`; Rust passes the node id. A response for an
    attempt that is not in flight (removed in the meantime) changes nothing. Go panics in
    `removeAttempt` or puts a removed attempt back on the heap in this case
    (`AddNodes` with a record update that fails the subnet limit removes an attempt that
    is in flight). `start_request` returns `false` and `pop_result` returns `None` where
    Go panics.
11. Order. The Go heap has no order among equal times; Rust is first in, first out. Go
    picks "random" map entries by iteration order; Rust picks uniformly with `rand`.
12. `IsDone` of the adaptive search moves the active bucket when nothing is left to ask
    (`search.go:152`). It is a side effect of a query function; the Rust loop calls
    `is_done` once per state, as the Go loop does.
13. Search results and the end of a pass. Go hands results to the consumer through a
    channel while it goes on with queries; the order of the two is decided by `select`. A
    target can be chosen while results are buffered, and the pass can then end with that
    request in flight; Go applies its response to the next pass and drops the target that
    the next pass had chosen (`v5_topic.go:485-536`). Rust takes the results of a response
    at once and chooses the next target after that. A pass never ends with a request in
    flight.
14. End of a search. The Go API is an iterator without an end. `topic_search` needs one:
    `count` records or `Config::query_timeout`.
15. Seeding takes every `ng` entry of the routing table, as `tab.allNodes()` does. The
    Rust table also keeps nodes in state `Disconnected` (Go removes dead nodes), so a
    dead node can be asked once per pass or per seeding.
16. Failure tracking. Go counts the timeouts of topic requests in the failure counter of
    the routing table, which removes the node after `maxFindnodeFailures`, and resets the
    counter on an answer. The Rust table has no counter: a failed request of a loop (any
    error) marks the node `Disconnected`, as the crate does for its other requests. A
    search request that failed after it brought nodes does not. Requests of the user
    (`send_regtopic`, `send_topic_query`) do not change the routing table.
17. Routing table hook. Go has feeds in the table. Rust records insertions and removals
    in each bucket (`TableChange`, at most 64 per bucket, oldest dropped) and the service
    takes them at the start of every iteration of its main loop. A change made outside the
    service (`Discv5::add_enr`, `remove_node`, `ban_node`) is seen when the service next
    runs. A node that moves within a bucket (status or value update) is no change. The
    Rust table removes nodes less often than Go: a disconnected node stays until a pending
    node replaces it, so ads of dead advertisers can stay until they expire.
18. New routing table nodes reach the running registrations only (Go: the search loop
    reads the feed only while it pauses, and drops what it reads).
19. Incomplete responses. The handler counts the messages of a response with the number
    that the first message announced, without the cap of the service, and drops the
    request after that number, or after a message that announces one. When the handler is
    done and the response is not complete (no REGCONFIRMATION, or fewer messages than
    announced), the service fails the request with `InvalidRemotePacket` at once. A
    response of the wrong kind (for example TOPICNODES for REGTOPIC) fails it the same
    way. Go ignores such messages and runs into the timeout. When a registrar announces
    more messages than the cap, the service is done at the cap and the handler waits for
    the rest until its timeout; that timeout fails the other requests to the same node
    (handler behaviour of the crate).
20. A REGTOPIC whose REGCONFIRMATION arrived and whose NODES did not all arrive fails, and
    the attempt ends, as in Go (`regtopic` returns the error, `runRegistration` calls
    `HandleErrorResponse`).
21. `topicQuery` in Go caps the auxiliary records at `topicNodesResultLimit` (16), not at
    `regtopicNodesLimit` (8). Ported: `topic_nodes_limit` caps both kinds for TOPICQUERY.
22. Timeouts: the crate's `request_timeout` (1 s) and `request_retries` (1), Go 700 ms.
23. The ad cache is behind `Arc<RwLock<..>>` and shared with `Discv5`, so
    `local_topic_nodes` does not go through the service (Go runs it on the dispatch loop).
24. The registrar can name the requester as an auxiliary node (Go and Rust); the tables
    skip the local node.

## Tests

- Unit tests next to the code, ported from `registration_test.go`, `search_test.go`,
  `searchfilter_test.go`, plus tests for the Rust-only parts (labels, LAN exemption,
  responses without a request, asked filter expiry, change log of the routing table).
- Service tests with a fake handler (`src/service/topic.rs`): response collection, caps,
  incomplete responses, registration walk with ticket and waiting time, new routing table
  nodes, eviction on removal, default and adaptive search passes, partial failures.
- Integration tests with UDP sockets on 127.0.0.1 (`src/discv5/test.rs`, module `topic`):
  `register_and_search` (two advertisers, the second through a ticket),
  `register_and_search_adaptive`, `search_ends_at_timeout`, `table_removal_evicts_ad`,
  `designated_requests`.
- Loopback: `is_lan` is true for 127.0.0.0/8, the relay check accepts a loopback record
  from a loopback sender, the IP score and the subnet limits of the tables skip LAN
  addresses. The ports of the test records are above 1024.

## Left out

- Testbed instrumentation: `wireStats`, `WaitTimeStats`, `AdmissionEvent`,
  `TopicSearchStats` (`queriesByBucket`, `activeTrace`), `TrackAdmissions` /
  `WasAdmitted`, `TopicQueryRcvCount`, `opid`, metrics.
- `netrestrict` in `verifyResponseNode`: the crate has no such list.
- A `Stream` API for searches without an end.

## Phase 4: interop against the Go node

- Run a Go node (`geth-adaptive-wt`, devp2p or simnet binary) and a Rust node on
  loopback: Rust registers on Go, Go registers on Rust, each side finds the other with a
  search; with and without `SearchYieldFloor`.
- Check on the wire: REGTOPIC field order, ticket round trip through a Go registrar (the
  ticket is opaque, the waiting time in ms), `total` of the mixed NODES / REGCONFIRMATION /
  TOPICNODES responses, the request id length (the crate sends 8 bytes).
- Handshake: the Go node answers a REGTOPIC from an unknown node with WHOAREYOU; check
  that the crate resends the REGTOPIC and that the record in the handshake carries `ng`.
- ENR sequence number: `Discv5::new` adds `ng` without a new sequence number
  (interpretation 6). A Go node that holds the older record of the same sequence number
  does not fetch the new one. Decide if the sequence number must go up.
- Timing: request timeout 1 s against 700 ms, ticket window of 5 s.
- Registrar limits against the Go advertiser: more than 8 distances in a request, low
  ports, records without address.
- Decide on interpretations 9 and 13 with the Go side, then port the fix.

## Phase 5: Lighthouse and Reth on this crate

- Lighthouse: map subnets (attestation, sync committee, data column) to topics; replace
  or complement the predicate queries of the subnet discovery with `register_topic` and
  `topic_search`; feed `TopicNodesFound` into the peer manager. The ENR must carry `ng`
  and keep its sequence number rule. The feature must be turned on in the `discv5`
  dependency of Lighthouse.
- Reth: the discv5 wrapper crate of Reth uses this crate; expose the topic calls there,
  topics from the fork id or the network name.
- For both: a search API that fits long runs (stream, or repeated `topic_search` with the
  filters kept between calls), limits for the number of registrations and searches at the
  same time, metrics, and the behaviour behind NAT (the REGTOPIC record must match the
  packet source, so a node without a known external address cannot register).
- Mixed networks: nodes without `ng` are never asked; measure how many `ng` nodes a
  routing table needs for the walk to work (the Go evaluation gives the numbers for geth).

# TopDisc in this crate

This crate implements the topic-based service discovery (TopDisc) part of the Discovery v5
spec: the wire messages of `discv5-wire.md` (REGTOPIC, REGCONFIRMATION, TOPICQUERY,
TOPICNODES) and the behaviour of `discv5-theory.md`, section "Topic-based Service
Discovery" (datahop/devp2p, `master`). The spec is the reference. This file says where each
part lives, and lists the few points the spec leaves to the implementation, with the choice
made here. Those choices match the geth implementation (datahop/go-ethereum, `topdisc`)
except where noted.

Everything is behind the Cargo feature `topic-discovery` (default off). With the feature
off the crate is upstream discv5. With it on, topic discovery is always on: there is no
runtime switch.

## Modules

| Spec | Code |
|---|---|
| Wire messages 0x07..0x0a, `total`, decode bounds | `src/rpc.rs` (`RequestBody::{RegTopic, TopicQuery}`, `ResponseBody::{RegConfirmation, TopicNodes}`) |
| Parameters and named constants | `src/topic/params.rs`, named as in the spec; test `spec_parameters` repeats the table |
| ENR capability `td`, service identifiers | `src/topic/mod.rs` (`TOPIC_DISCOVERY_ENR_KEY`, `supports_topic_discovery`, `topic_id`) |
| Ad cache, waiting-time function, lower bounds | `src/topic/table.rs`, `src/topic/bound.rs` |
| IP similarity tree | `src/topic/iptree.rs` |
| Tickets, used-ticket memory | `src/topic/ticket.rs` |
| Registration table | `src/topic/registration.rs` |
| Search table, yield, pass transitions | `src/topic/search.rs` |
| Failed, overloaded and asked sets; `/24` lookup limit | `src/topic/askedset.rs`, `src/topic/admittedset.rs` |
| Result filter and mixer | `src/topic/searchfilter.rs` |
| Endpoint address, relay rule, LAN addresses, `/24` sets | `src/topic/netutil.rs` |
| Request handlers, registration and lookup loops | `src/service/topic.rs` |
| Response timing and packet counting | `src/handler` (topic requests only) |
| Public API | `src/discv5.rs` (`Discv5`, `TopicSearchStream`), `topic::TopicConfig` |

The spec's test vectors ("TopDisc Message Encodings") are kept verbatim in
`src/topic/testdata/topdisc-message-encodings.txt`; the `topic_tests` module of `src/rpc.rs`
decodes all 8, checks the fields and re-encodes them byte for byte.

## Capability and identifiers

- `Discv5::new` inserts `td = 1` into the local ENR when it is missing, through
  `Enr::insert` (the sequence number goes up). A record whose `td` is missing, undecodable or
  another version is not TopDisc-capable.
- `topic::topic_id(name)` is SHA-256 of the UTF-8 bytes of the name. `TopicId` is the 32-byte
  hash; the API takes hashes.
- TopDisc requests go only to `td` nodes. The service tables take only `td` nodes, from the
  routing table and from auxiliary ENRs.

## Wire

As in the spec: REGTOPIC `[request-id, topic, ticket, ENR, [topic-distance, ...]]`,
REGCONFIRMATION `[request-id, total, ticket, wait-time]`, TOPICQUERY
`[request-id, topic, [topic-distance, ...]]`, TOPICNODES `[request-id, total, [ENR, ...]]`.
Auxiliary ENRs travel in NODES (0x04) with the request id and `total` of the topic request.
More than `MAX_TOPIC_DISTANCES` distances, or a TOPICNODES with more than `MAX_TOPIC_NODES`
records, does not decode.

The REGTOPIC ENR is kept as raw RLP next to the decoded record. A list that is not a record
makes the message undecodable (not answered); a record whose signature fails is answered
with a rejection. In responses to topic requests a record that fails verification is dropped
on its own and the rest of the packet is kept. NODES answering FINDNODE keep the upstream
strict decoding.

## Registrar

- Every decoded REGTOPIC is answered, in the order of the spec's Admission Control: ENR
  checks, ticket authentication and timing, credit of a ticket not yet remembered, an
  advertisement already held is admitted unchanged, then the waiting time. One clock reading
  serves the whole request.
- Rejection: REGCONFIRMATION with no ticket and wait-time 0, `total = 1`, no NODES. Early
  ticket: the same ticket bytes and the rest of its wait, no NODES. Otherwise
  REGCONFIRMATION first, then the auxiliary ENRs in NODES of `ENRS_PER_PACKET`,
  `total = 1 + NODES count`. Admission carries the ad lifetime in `wait-time`.
- Waiting time: the spec's function with `W_BASE`, `P_OCC`, `G`, capped at `MAX_WAIT_TIME`,
  computed in seconds and rounded up to ns, non-positive or NaN gives 0; `wait-time` on the
  wire is rounded up to ms. A full cache quotes `MAX_WAIT_TIME` minus the credit, or the time
  to the next expiry when the credit is within `W_MIN` of it. A cache of capacity 0 quotes
  `MAX_WAIT_TIME`.
- Lower bounds per service and per IP-tree vertex, as in the spec. The IP tree keeps IPv4
  and IPv6 apart; an IPv4-mapped address goes into the IPv4 tree. LAN addresses score 0.
- The ad cache holds at most `C` ads, never two for the same node and service, and drops an
  advertiser's ads when the routing table drops the node. Expired ads are removed before
  any count, lookup or registration.
- TOPICQUERY: NODES with the auxiliary ENRs, then TOPICNODES with up to `F_RETURN` random
  advertisers that pass the relay rule for the requester. With nothing to send, one empty
  TOPICNODES with `total = 1`.
- Auxiliary ENRs: the first `AUX_ENR_LIMIT` listed distances, one random `td` node per
  distance from the routing table (one pass, reservoir sampling), relay rule against the
  requester.

## Advertiser

- `Discv5::register_topic` starts a registration loop for the topic. Its table has
  `REG_TABLE_DEPTH` buckets with the spec's labels, `K_REGISTER` active registrars,
  `REG_STANDBY_LIMIT` standby candidates and one registrar per `/24` per bucket.
- Candidates come from the routing table (all `td` entries, random order) and from
  auxiliary ENRs, under the five rules of Advertisement Placement. The failed and overloaded
  sets keep a registrar out for `ASKED_FAILED_TTL` / `OVERLOADED_TTL` from its first entry.
- Scheduling: due attempts are served one per bucket in turn from the farthest bucket, a due
  ticket retry before a first contact. A ticket retry does not wait for other requests.
- Quotes below `W_MIN` are waited as `W_MIN` and count toward `REG_ATTEMPT_BUDGET`; a single
  or total quote above the budget abandons the attempt into the overloaded set. A rejection,
  or a timeout before the REGCONFIRMATION, puts the registrar into the failed set. A timeout
  after it keeps the confirmation. Each removal refills the slot from standby.
- Renewal follows the spec: no re-registration before expiry; at expiry the registrar leaves
  the bucket and a standby candidate takes the slot.
- `Event::TopicRegistered` is emitted for each admission.

## Discoverer

- `Discv5::topic_search_stream` runs one lookup until the stream is dropped. Each pass
  fills a fresh search table (`SEARCH_TABLE_DEPTH` buckets, `SEARCH_BUCKET_SIZE` candidates,
  two per `/24` per bucket) from the routing table and sends one TOPICQUERY at a time.
- Registrar selection, the asked and failed sets (`ASKED_FAILED_TTL`), spares, and the
  `SEARCH_SUBNET_LIMIT` per `/24` follow Registrar Selection. A registrar that answered keeps
  its `/24` slot in the pass; a failed one releases it. One candidate per bucket per reply is
  admitted.
- Yield samples, the lower/upper median rule, `SEARCH_AUX_RADIUS` and the move conditions
  follow Advertisement Yield. A partial reply is sampled only when it reaches
  `SEARCH_YIELD_FLOOR`.
- Pass ends and the start bucket of the next pass follow Pass Transitions, including the cap
  of `SEARCH_PASS_QUERIES`, the one retry of a bucket whose queries all failed, the restart at
  bucket 0, and the back-off between `SEARCH_PASS_GAP` and `SEARCH_PASS_BACKOFF`.
- Result Delivery: validation, per-query de-duplication, `td` check, the result filter (one
  delivery per `E`, once more with a higher sequence number) and the mixer
  (`SEARCH_MIX_REGISTRARS`, `SEARCH_MIX_WAIT`, rounds in first-reply order, the lone-registrar
  rule, highest sequence number wins). A pass releases all queued results when it ends and the
  next pass starts after they are read. While `SEARCH_UNREAD_LIMIT` results wait unread, no
  query is sent.

## Requests and responses

- A topic request waits `RESP_TIMEOUT` per response packet: the timer restarts on each
  packet. Topic requests are never retransmitted and are not re-sent when a session is
  re-established; a request lost that way ends as a timeout.
- The first `total` counts, capped at `REG_RESP_LIMIT` (REGTOPIC, which also needs the
  REGCONFIRMATION) or `QUERY_RESP_LIMIT` (TOPICQUERY). A TOPICQUERY reply cut short by the
  cap, a timeout or an error after a valid record is partial (`TopicQueryResult::complete`
  is false), also when no record is valid at the cap.
- Returned records pass the relay rule against the registrar's address and are
  de-duplicated per kind; at most `F_RETURN` advertisers and the cap of the request kind for
  auxiliary ENRs are kept.
- Liveness: the routing table has no failure counter. A REGTOPIC timeout before the
  REGCONFIRMATION, or a TOPICQUERY timeout before any valid record, marks the node
  `Disconnected` in the routing table. Nothing else does.

## Where the spec is silent

1. Ticket encoding: `HMAC-SHA256 (32) || key id u16 | topic 32 | node id 32 | twait u64 |
   tmod u64 | tinit u64`, big-endian, times in ns since the sealer's epoch, 122 bytes. Keys
   rotate after 100,000 uses and are dropped after 6 h unused. The used-ticket memory is
   keyed by the MAC.
2. A reply with several REGCONFIRMATIONs: the last one counts.
3. Up to 4 REGTOPIC requests in flight per topic, each to a different registrar.
4. With no usable candidates the advertiser re-reads the routing table every 2 s.
5. Pass transitions: after the one retry pass the next pass moves one bucket closer (or
   stays at the closest bucket) and a retry is possible again from there; after 30 idle
   passes the gap stays at `SEARCH_PASS_BACKOFF`.
6. Result delivery: filter capacity `SEARCH_FILTER_LIMIT` = 50,000 (earliest expiry dropped);
   unread limit `SEARCH_UNREAD_LIMIT` = 16, counting the results in the stream channel and in
   the pass buffer, with queries stopped at 16 or more.
7. Mixer: a registrar counts toward `SEARCH_MIX_REGISTRARS` when at least one of its results
   passed the filter; a node queued by several registrars waits in each queue and is skipped
   once delivered; the filter records a node when it is handed to the stream; a failed query
   also checks the mixer; a newer record of a node already delivered in the pass waits for the
   next pass.
8. Seeding takes every `td` entry of the routing table, also entries in state `Disconnected`
   (the crate keeps them until replaced); the failed sets bound how often a dead node is asked.
   Rust-only, not in geth: a target the service cannot send to counts as a failed query.

## Public API

```rust
impl Discv5 {
    pub fn register_topic(&self, topic: TopicId) -> Result<(), Error>;
    pub fn stop_topic_registration(&self, topic: TopicId) -> Result<(), Error>;
    pub fn topic_search_stream(&self, topic: TopicId) -> Result<TopicSearchStream, Error>;
    pub fn topic_search(&self, topic: TopicId, count: usize)
        -> impl Future<Output = Result<Vec<Enr>, QueryError>>;
    pub fn send_regtopic(&self, contact: NodeContact, topic: TopicId, ticket: Vec<u8>, distances: Vec<u64>)
        -> impl Future<Output = Result<RegTopicResult, RequestError>>;
    pub fn send_topic_query(&self, contact: NodeContact, topic: TopicId, distances: Vec<u64>)
        -> impl Future<Output = Result<TopicQueryResult, RequestError>>;
    pub fn local_topic_nodes(&self, topic: TopicId) -> Vec<Enr>;
}

// discv5::topic
pub fn topic_id(service: &str) -> TopicId;
pub enum RegConfirmation { Admitted { lifetime }, Ticket { ticket, wait }, Rejected }
pub struct RegTopicResult { pub confirmation: RegConfirmation, pub aux: Vec<Enr> }
pub struct TopicQueryResult { pub advertisers: Vec<Enr>, pub aux: Vec<Enr>, pub complete: bool }
pub struct TopicConfig { ad_lifetime, ad_cache_size, reg_bucket_size,
                         reg_bucket_standby_limit, reg_attempt_timeout, search_bucket_size }

// discv5::Event
Event::TopicRegistered { topic: TopicId, registrar: NodeId }
```

- `register_topic`, `stop_topic_registration` and `topic_search_stream` do not wait: they
  fail with `Error::Custom("Service channel full")` when the service channel is full.
  Stopping a registration leaves the placed ads to expire.
- `TopicSearchStream` is a `Stream<Item = Enr>`; reading it resumes a lookup held by the
  unread limit, dropping it ends the lookup.
- `topic_search` reads a stream until `count` distinct advertisers or `Config::query_timeout`.
- `send_regtopic` and `send_topic_query` are single requests to a chosen node, without the
  `td` filter, for tools and tests.
- `TopicConfig` defaults are the spec's parameters. `Discv5::metrics().topic` counts topic
  messages, tickets issued and received, rejections sent and received, and ticket outcomes.

## Tests

- Unit tests next to the code (tables, tickets, IP tree, filter, search, registration).
- Service tests with a fake handler in `src/service/topic.rs`.
- UDP tests on 127.0.0.1 in `src/discv5/test.rs`.
- Interop smoke against geth: `interop/geth-smoke.sh`, see `interop/README.md`.

Run tests with `--lib --bins --tests --examples`: the crate doctest binds UDP 9000.

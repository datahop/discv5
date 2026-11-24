# TopDisc parity with geth

This file records how the TopDisc code of this crate matches the spec and the geth
implementation, and the evidence for it. The spec is the reference. geth is the second
implementation: where it agrees with the spec, the Rust code matches it; where geth and the spec
differ, section 3 says so and the Rust code follows the spec.

| Reference | Commit |
|---|---|
| geth, datahop/go-ethereum `topdisc` | `3b8bef9e3` (v1.17.10 + TopDisc). `p2p/discover` and `p2p/enode` are identical to `08faf4f26` (#188): `git diff --stat 08faf4f26 3b8bef9e3 -- p2p/discover p2p/enode` is empty. |
| Spec, datahop/devp2p `master` | `ff1e30ab0` (`discv5/discv5-theory.md`, `discv5/discv5-wire.md`) |

The evidence has three layers: the geth PRs mapped to the spec and to the Rust code (section 1),
every geth TopDisc test mapped to the Rust test that checks the same behaviour (section 4), and
live runs against geth and on one `/24` (section 5). Section 2 lists where the Rust code differs
from geth, section 3 where geth differs from the spec.

## 1. geth PRs

The TopDisc PRs of geth's `topdisc` branch from #140 (the first after the rebase of 2026-10-20)
to #188. Earlier PRs (#20 to #107) were ported before #140 and their tests are in section 4.
The Rust tests column lists the Rust tests mapped to the geth tests the PR added or changed;
paths are relative to `src/`. Smoke cases are those of `interop/geth-smoke.sh` (see
`interop/README.md`).

| geth PR | spec section (theory unless noted) | Rust code | Rust tests (from the geth tests of the PR) | smoke case |
|---|---|---|---|---|
| #140 | Result Delivery; Lookup Responses | `searchfilter.rs` `SearchFilter::{take,release,add}`; `search.rs` `Search::add_results`; `service/topic.rs` `search_step` | `service/topic::regtopic_rejections_answered`, `service/test::regtopic_admits_and_topicquery_serves`, `topic/search::buckets_with_free_space`, `topic/searchfilter::expiry`, `topic/searchfilter::limit`, `topic/searchfilter::mix_releases_at_ten_registrars`, `topic/searchfilter::mix_continuous`, `topic/searchfilter::mix_drops`, `topic/searchfilter::mix_newer_record`, `topic/searchfilter::mix_overlap`, `topic/searchfilter::mix_release`, `topic/searchfilter::mix_shared`, `topic/searchfilter::mix_releases_after_wait`, `topic/searchfilter::newer_record`, `topic/searchfilter::newer_record_once`, `topic/search::result_newer_record`, `discv5/test::register_and_search`, `discv5/test::register_and_search_eight_nodes`, `service/topic::search_passes`, `discv5/test::search_stream_finds_advertisers` | 1, 2 |
| #141 | — (test fix) | `discv5/test.rs` UDP tests | `discv5/test::register_and_search`, `discv5/test::register_and_search_eight_nodes` | — |
| #144 | Advertisement Yield; Pass Transitions; Registrar Selection (asked set, spares) | `search.rs` `Search::{add_reply,add_partial_reply,query_target,next_pass_bucket}`; `askedset.rs`; `service/topic.rs` `TopicSearch::{next_pass,pass_gap}` | `topic/askedset::keeps_first_expiry`, `topic/search::adaptive_advance`, `topic/search::asked_by_id`, `topic/search::confirmed_bucket_holds`, `topic/search::empty_closest_restarts_wide`, `topic/search::empty_topic_restarts_wide`, `topic/search::adaptive_explores`, `topic/search::failures_keep_bucket`, `topic/search::far_helper_pulls_out`, `topic/search::adaptive_helper`, `topic/search::helper_keeps_bucket`, `topic/search::adaptive_is_done`, `topic/search::adaptive_is_done_once`, `topic/search::adaptive_liar`, `topic/search::move_survives_pass_end`, `topic/search::no_reask_in_pass`, `topic/search::partial_reply`, `topic/search::pass_budget`, `topic/search::replied_node_keeps_subnet`, `topic/search::adaptive_reply_size`, `topic/search::adaptive_retreat`, `topic/search::adaptive_spare`, `topic/search::spare_asked_from_table`, `topic/search::spare_no_reask_after_expiry`, `topic/search::spare_rotation`, `topic/search::adaptive_walked_dry`, `topic/search::handle_error_response`, `topic/search::is_done_lifecycle`, `topic/search::query_target`, `topic/search::result_newer_record`, `topic/search::results_tracking`, `service/test::aux_nodes_one_per_distance_capable_only`, `topic/searchfilter::mix_releases_at_ten_registrars`, `topic/searchfilter::mix_continuous`, `service/topic::retry_pass_once`, `topic/search::failed_excluded`, `service/topic::dead_frontier_kept_once`, `service/topic::restart_at_farthest`, `service/topic::backoff_idle_definition`, `topic/search::answered_or_placed_not_failed`, `service/topic::search_pass_gap`, `service/topic::search_partial_reply`, `service/topic::pass_waits_for_reads`, `service/topic::search_sample_before_td`, `service/topic::search_unsupported_ads_idle`, `service/topic::queries_stop_at_unread_limit`, `service/topic::stream_drives_running_search`, `service/topic::search_passes`, `discv5/test::register_and_search`, `service/topic::registration_walk`, `service/topic::stop_and_register_again` | 1 |
| #145 | Service Tables (bucket labels, topic-distances) | `registration.rs` `Registration::buckets_with_free_space` | `topic/registration::free_space_labels` | — |
| #151 | TopDisc Liveness and Temporary Exclusion; Registration Procedure; Registrar Selection | `registration.rs` failed/overloaded sets; `search.rs` failed set; `service/topic.rs` `topic_request_done` | `topic/mod::enr_capability_key`, `service/topic::registration_waits_for_registrars`, `service/topic::search_passes`, `topic/registration::expiry`, `topic/registration::failed_excluded`, `topic/search::pass_budget`, `topic/search::failed_excluded`, `topic/search::failed_spare`, `service/topic::answer_restores_liveness`, `service/topic::two_timeouts_then_answer`, `service/topic::rejection_is_not_liveness_failure`, `service/topic::registration_walk`, `service/topic::regtopic_timeout_keeps_ad`, `service/topic::retry_pass_once`, `topic/search::failures_keep_bucket`, `service/topic::dead_frontier_kept_once`, `topic/search::empty_topic_restarts_wide`, `topic/search::empty_closest_restarts_wide`, `topic/search::adaptive_walked_dry`, `topic/search::adaptive_is_done_once`, `service/topic::restart_at_farthest`, `service/topic::backoff_idle_definition`, `service/topic::search_timeout_not_reasked` | — |
| #153 | Admission Control; Tickets; Registration Procedure | `service/topic.rs` `handle_regtopic`, `regtopic_credit`; `ticket.rs`; `registration.rs` `Registration::update` | `service/topic::regtopic_rejections_answered`, `service/test::regtopic_admits_and_topicquery_serves`, `service/topic::replayed_ticket_new_attempt`, `service/topic::early_ticket_echoed`, `service/topic::late_ticket_credit`, `service/topic::foreign_ticket_not_consumed`, `service/topic::bad_signature_enr_rejected`, `topic/registration::add_nodes_keeps_in_flight`, `topic/registration::ticket_response_below_floor`, `topic/registration::ticket_response_drop_above_budget`, `topic/registration::remove_node`, `topic/registration::update_bucket_fairness`, `topic/registration::update_hostile_bucket`, `topic/registration::update_prefers_tickets`, `topic/ticket::ticket_timing`, `topic/ticket::used_tickets_ttl_and_limit`, `service/topic::replayed_ticket_after_ad_expiry`, `topic/ticket::wrong_topic_and_early_ticket`, `topic/ticket::key_reuse_and_rotation`, `service/topic::rejection_goes_to_failed_set`, `service/topic::rejection_is_not_liveness_failure`, `service/topic::ticket_retry_on_time_with_slow_registrar`, `service/topic::four_regtopics_in_flight`, `service/topic::retries_not_blocked_by_dead_registrars`, `service/topic::registration_walk`, `discv5/test::register_with_ticket_round_trip` | 3, 4 |
| #155 | Waiting-Time Function (`MAX_WAIT_TIME`) | `table.rs` `TopicTable::wait_time_components`, `scale_base` | `topic/table::expiry_order`, `topic/table::full_cache_admits_after_expiry`, `topic/table::wait_capped_24h` | — |
| #157 | Registration Procedure (`W_MIN`, budget); wire `wait-time` | `registration.rs` `handle_ticket_response`; `rpc.rs` `decode_wait_time` | `service/topic::replayed_ticket_new_attempt`, `topic/registration::ticket_response_drop_above_budget`, `topic/registration::update_hostile_bucket`, `rpc::wait_time_saturates`, `service/topic::registration_walk`, `service/topic::regtopic_huge_wait_time`, `topic/registration::ticket_response_below_floor`, `service/topic::rejection_is_not_liveness_failure` | 3 |
| #158 | — (test fix) | — | `rpc::wait_time_saturates`, `topic/registration::ticket_response_drop_above_budget`, `service/topic::registration_walk`, `service/topic::regtopic_huge_wait_time` | — |
| #159 | IP Similarity Tree | `iptree.rs` `IpTree::{insert,remove,score_node}` | `topic/iptree::insert_ipv4, topic/iptree::insert_ipv6`, `topic/iptree::iptree_remove_to_inserted_depth`, `topic/iptree::remove_ipv4`, `topic/iptree::score_matches_insert` | — |
| #160 | Tickets (node binding) | `ticket.rs` `TicketSealer::{pack,unpack}`; `service/topic.rs` `handle_regtopic` | `service/topic::foreign_ticket_not_consumed`, `service/topic::replayed_ticket_new_attempt`, `service/topic::regtopic_rejections_answered`, `service/topic::early_ticket_echoed`, `service/topic::late_ticket_credit`, `service/topic::bad_signature_enr_rejected`, `topic/ticket::ticket_timing`, `topic/ticket::used_tickets_ttl_and_limit`, `service/topic::replayed_ticket_after_ad_expiry`, `topic/ticket::seal_unseal_round_trip` | 3 |
| #164 | Service Tables (ads of nodes dropped from the table) | `service/topic.rs` `topic_table_changes`; `table.rs` `TopicTable::remove_node` | `service/topic::routing_table_removal_evicts`, `discv5/test::table_removal_evicts_ad` | — |
| #165 | Admission Control (one clock reading) | `service/topic.rs` `handle_regtopic(.., now)` | `service/topic::replayed_ticket_new_attempt` | — |
| #166 | Waiting-Time Lower Bound | `bound.rs`; `table.rs` `record_wait_time`, `remove_ad` | `topic/table::expiry_order`, `topic/table::full_cache_admits_after_expiry`, `topic/table::full_cache_next_expiry`, `topic/table::topic_bound_recorded_and_collected`, `topic/table::topic_bound_outliving_ads_collected`, `topic/table::topic_lower_bound_decays` | 3 |
| #168 | Registrar Selection (`SEARCH_SUBNET_LIMIT`, per-bucket /24) | `admittedset.rs`; `search.rs` `Search::{add_nodes,query_target}`; `service/topic.rs` `search_step` | `topic/admittedset::exempt`, `topic/admittedset::keeps_first_expiry`, `topic/admittedset::limit`, `topic/search::failures_keep_bucket`, `topic/search::spare_asked_from_table`, `topic/search::add_nodes_one_per_bucket_rule`, `topic/search::ip_limit`, `topic/search::subnet_limit_across_passes`, `topic/search::subnet_limit_drop_ends_pass`, `topic/search::subnet_limit_drops_held_node`, `topic/search::subnet_limit_spares`, `topic/search::subnet_limit_unqueried_seeds`, `service/topic::subnet_limit_ends_pass`, `service/topic::search_sample_before_td`, `service/topic::backoff_idle_definition`, `service/topic::search_unsupported_ads_idle` | — |
| #170 | Waiting-Time Function (`G`); Registration Procedure (budget) | `table.rs` `wait_time_components`; `registration.rs` `handle_ticket_response` | `topic/registration::ticket_response_drop_above_budget`, `topic/registration::over_budget_promotes_standby`, `topic/registration::overloaded_ring`, `rpc::wait_time_saturates`, `service/topic::registration_walk`, `service/topic::regtopic_huge_wait_time`, `topic/registration::ticket_response_below_floor`, `service/topic::rejection_is_not_liveness_failure`, `topic/table::wait_capped_24h`, `topic/table::wait_safety_term` | 3 |
| #173 | Ad Cache (expired ads) | `table.rs` `TopicTable::{expire,nodes,random_nodes,is_registered}`; `service/topic.rs` `handle_topic_query`, `handle_regtopic` | `service/topic::topic_query_expires_ads_first`, `topic/table::expired_ad_not_served_or_counted`, `topic/table::random_nodes` | — |
| #174 | — (shutdown) | `discv5.rs` `shutdown`, `TopicSearchStream`; `service/topic.rs` `search_step` | `discv5/test::shutdown_ends_searches`, `service/topic::dropped_search_ends`, `discv5::drop_closes_and_notifies`, `service/topic::stream_drives_running_search` | — |
| #177 | Waiting-Time Lower Bound (dropped with the last ad) | `table.rs` `remove_ad` | `topic/table::expiry_order`, `topic/table::full_cache_admits_after_expiry`, `topic/table::full_cache_next_expiry`, `topic/table::topic_lower_bound_decays`, `topic/table::expired_ad_not_served_or_counted` | — |
| #180 | Result Delivery (`SEARCH_MIX_REGISTRARS`, `SEARCH_MIX_WAIT`) | `searchfilter.rs` `SearchFilter::{mix,release}` | `topic/searchfilter::mix_releases_at_ten_registrars`, `topic/searchfilter::mix_continuous` | 1, 2 |
| #181 | ENR Capability (`td`) | `topic/mod.rs` `TOPIC_DISCOVERY_ENR_KEY`, `supports_topic_discovery`; `discv5.rs` `Discv5::new` | — | all |
| #182 | Parameters (`C` = 1000) | `params.rs` `C`; `config.rs` | — | — |
| #183 | Response Splitting; TopDisc Liveness | `handler/request_call.rs` `TopicRespCount::count`; `service/topic.rs` `handle_topic_response`, `topic_request_done`, `reg_outcome`, `query_outcome` | `service/topic::regtopic_closed_after_confirmation`, `service/topic::confirmation_kept_when_nodes_missing`, `service/topic::post_confirmation_timeout_keeps_registration`, `service/topic::rejection_is_not_liveness_failure`, `service/topic::confirmation_kept_when_nodes_missing (second half)`, `service/topic::failed_regtopic_keeps_aux_nodes`, `service/topic::reg_resp_limit_without_confirmation_fails`, `service/topic::topic_response_caps (REGTOPIC half)`, `service/topic::regtopic_packet_limit`, `service/topic::topic_response_caps (TOPICQUERY half)`, `service/topic::first_total_wins (TOPICQUERY half)`, `service/topic::topic_query_empty_and_failed`, `service/topic::query_partial_response`, `service/topic::query_cap_without_records_fails` | 3 |
| #186 | Services and Service Identifiers; wire test vectors | `topic/mod.rs` `topic_id`; `rpc.rs` codec; `examples/topdisc_node.rs` `--service`/`--topic` | `interop/geth-smoke.sh (mode basic, cases 1 and 2)`, `rpc::spec_vectors_decode_encode`, `rpc::nodes_vector_unchanged` | all |
| #188 | Service Tables (one per bucket per reply, /24 per bucket) | `search.rs` `Search::add_nodes` | `topic/search::ip_limit`, `topic/search::one_per_bucket_after_ip_limit` | — |

## 2. Rust vs geth

The defaults of every parameter equal geth's (`src/topic/params.rs` against geth's
`topicindex/common.go`, `search.go`, `searchfilter.go`, `ticket.go`, `topictable.go` and
`v5_topic.go`). The choices the spec leaves open follow geth; they are listed in `PLAN.md`,
"Where the spec is silent".

Values derived from the ad lifetime follow geth: the failed and overloaded sets of the
advertiser, and the asked, failed and `/24` sets of a lookup, keep their entries for the
configured `TopicConfig::ad_lifetime`. `reg_attempt_timeout` defaults to 1.5 times it when
unset or zero (`TopicConfig::attempt_timeout`, geth `withDefaults`). The constants
`ASKED_FAILED_TTL`, `OVERLOADED_TTL` and `REG_ATTEMPT_BUDGET` are the values at the default
lifetime `E`. Tests: `topic/config.rs::attempt_timeout_follows_lifetime`,
`topic/registration.rs::failed_excluded`, `topic/search.rs::sets_follow_ad_lifetime`.

Liveness follows the spec (theory, TopDisc Liveness and Temporary Exclusion): "A timeout of a
[REGTOPIC] before the [REGCONFIRMATION] arrives, or of a [TOPICQUERY] before any record that
passes validation has arrived, counts as one liveness failure towards the ordinary node table. A
request that the node answers, even with a rejection or a partial response, clears the
consecutive failures counted so far. No other TopDisc failure counts. Retention in the ordinary
node table otherwise follows Discovery v5 rules."

- **One failure:** the crate's routing table has no failure counter; one failed request is its
  unit of failure. An ordinary request (PING, FINDNODE, TALKREQ) that fails after its resends
  ends in `Service::rpc_failure`, which marks the entry `Disconnected` for every error
  (`src/service.rs:1667`, `:1764`; upstream sigp 8dc0696 `src/service.rs:1506`, `:1595`);
  the `Disconnected` branch of `connection_updated` takes the node off the ping schedule
  (`src/service.rs:1579-1600`; 8dc0696 `:1418-1439`); the handler keeps the session
  (`src/handler/mod.rs:481-487`; 8dc0696 `src/handler/mod.rs:470-477`). A qualifying topic
  timeout does the same, through the same `connection_updated(.., Disconnected)`
  (`src/service/topic.rs:674`); topic requests are not resent (wire, UDP Communication:
  "Implementations should not re-send packets if the recipient doesn't respond"). The entry
  stays in the table until a pending node replaces it, as for any disconnected entry. Tests:
  `service/topic.rs::rejection_is_not_liveness_failure`,
  `service/topic.rs::two_timeouts_then_answer`.
- **Clearing:** an answer connects a `Disconnected` entry again and puts it back on the ping
  schedule (`service/topic.rs` `topic_liveness_restored`): a REGCONFIRMATION of any kind, also
  when the request then ends in an error (S3); a complete TOPICQUERY response, also one without
  records; a partial one. A node outside the routing table is not inserted. A failed request
  that is not a timeout changes nothing. Tests: `service/topic.rs::answer_restores_liveness`,
  `service/topic.rs::failed_answer_keeps_liveness`.

TOPICQUERY outcomes follow the pending spec clarification (S1): complete when the whole
announced `total` arrived, with any number of records, so a `total` above `QUERY_RESP_LIMIT` (24
= `F_RETURN + AUX_ENR_LIMIT`, datahop/devp2p#52, pending; geth uses 16, S4) never completes;
partial when the response ended early, by a timeout, the `QUERY_RESP_LIMIT` cap or an error,
after a valid record; failed when it ended early without one. A failed query puts the registrar
in the lookup's failed set; at the cap it is reported as `RequestError::InvalidRemotePacket`.
The cap still ends receipt. Tests: `service/topic.rs::query_partial_response`,
`service/topic.rs::query_cap_without_records_fails`,
`service/topic.rs::query_cap_below_announced_total`.

The remaining differences from geth:

1. **Seeding from `Disconnected` entries:** lookups and registrations take every `td` entry of
   the routing table, also entries in state `Disconnected`, which the crate keeps until they are
   replaced (geth evicts after failed checks). The failed sets bound how often a dead node is
   asked. The spec requires no liveness check or table membership for `B(s)` (TopDisc Liveness
   and Temporary Exclusion). `PLAN.md`, item 8 of "Where the spec is silent".
2. **A target the service cannot send to** counts as a failed query (Rust only; geth has no
   such case). The spec counts an unreachable registrar as a failed query (Registrar
   Selection).
3. **Direct requests:** `Discv5::send_regtopic` returns an error without the ENRs that arrived
   when the request fails; geth's call returns both. The registration loop keeps them
   (`service/topic.rs::failed_regtopic_keeps_aux_nodes`), as geth's does.

## 3. Spec vs geth

Places where geth and the spec, datahop/devp2p `ff1e30ab0`, differ or where the spec allows
more than one reading. The Rust code follows the spec in each.

- **S1, TOPICQUERY ended early without a valid record (spec ambiguity, per pending spec
  clarification):** Response Splitting (text at `ff1e30ab0`; S4 has the amended cap): "A
  discoverer waits for at most `QueryRespLimit = 16` packets of any type for a [TOPICQUERY], even
  if `total` is higher, treats what arrived as a partial response if the capped `total` was not
  reached". Registrar Selection: "A query fails when the registrar is unreachable, times out, or
  its response ends in an error before any record (advertisement or auxiliary ENR) that passes
  validation has arrived." The spec does not say whether the cap is such an error. The pending
  clarification (datahop/devp2p#52): a response is complete when the whole announced `total`
  arrived, whatever it holds; reaching the cap short of a larger `total` is an early end; partial
  when it ended early (timeout, cap or error) with at least one valid record; failed when it ended
  early with none. A failed query goes to the failed set; for liveness a timeout is one failure,
  the cap or another error changes nothing. geth behaves so when packets of other kinds fill the
  cap: it returns `errTooManyResponses` (`v5_udp.go:668`), and a response with an error and no
  nodes goes to `HandleErrorResponse` with `trackTopicRequest(.., false)` only for `errTimeout`
  and no reset (`v5_topic.go:611-619`). Rust does the same
  (`service/topic.rs::query_cap_without_records_fails`,
  `service/topic.rs::failed_answer_keeps_liveness`). For a larger announced `total` geth differs,
  see S4.
- **S2, records with a UDP port up to 1024 (out of scope):** the spec has no port rule for
  returned records since datahop/devp2p #50 ("drop MinUDPPort from TopDisc"). geth drops such
  records in `verifyResponseNode` (`v5_udp.go:559`), the general check of every NODES-style
  response, which TOPICNODES and the auxiliary NODES share. This is ordinary discv5 record
  validation, outside the TopDisc spec. Rust applies the same check to topic responses
  (`service/topic.rs` `LOW_PORT`). No change.
- **S3, REGTOPIC that gets its REGCONFIRMATION and then ends in an error other than a timeout
  (geth deviation):** TopDisc Liveness: "A request that the node answers, even with a
  rejection or a partial response, clears the consecutive failures counted so far." geth keeps
  the confirmation only on a timeout; any other error is returned with it (`v5_udp.go:636-642`),
  and the registration loop handles the error and `continue`s before
  `trackTopicRequest(.., true)` (`v5_topic.go:329-338`), so the answer does not clear the
  failures. Rust clears them on any REGCONFIRMATION (`service/topic.rs:663-667`, test
  `service/topic.rs::answer_restores_liveness`, case "error after confirmation") and fails the
  attempt as geth does (`reg_outcome`, `service/topic.rs:1096`). No Rust change.
- **S4, TOPICQUERY packet and record caps (geth deviation, three parts):** per datahop/devp2p#52
  (pending) `QueryRespLimit` is 24, `F_return` + `AuxENRLimit`, so that an honest registrar may
  send one record per packet; the spec text at `ff1e30ab0` still says 16 (quoted in S1). A
  response is complete only when its whole announced `total` arrived; reaching the cap first is an
  early end: failed without a valid record (failed set, liveness unchanged), partial with one
  (sampled only when its count reaches `F`). A discoverer keeps at most `AuxENRLimit` = 8
  auxiliary ENRs per query, the most an honest registrar sends. geth differs in all three parts.
  (a) It stops at `topicNodesResultLimit` = 16 (`v5_udp.go:49`, `:667`). (b) It caps the announced
  `total` at 16 on the first answering packet (`v5_udp.go:679`, `:695`), so 16 NODES or TOPICNODES
  packets of an announced 40 end the call with no error (`v5_udp.go:666`), and the search takes it
  as a complete reply, `AddReply` and `trackTopicRequest(.., true)` (`v5_topic.go:620-635`): with
  16 empty TOPICNODES the registrar counts as answered and its failures are cleared, and with
  records the reply is a full yield sample. (c) It keeps up to 16 auxiliary ENRs per query
  (`v5_udp.go:684`). Rust follows the amended spec: `QUERY_RESP_LIMIT` = `F_RETURN +
  AUX_ENR_LIMIT` = 24 (`topic/params.rs`), and at most `AUX_ENR_LIMIT` auxiliary records are kept
  per TOPICQUERY (`service/topic.rs` `handle_topic_response`). Tests:
  `service/topic.rs::query_cap_below_announced_total` (announced 40 capped at 24 with and without
  records, announced 5, announced 24 at one record per packet),
  `service/topic.rs::topic_response_caps` (24 packets, 16 advertisers, 8 auxiliary ENRs kept). The
  interop smoke is not affected: no case sends a TOPICQUERY response of more than a few packets.

Checked with no other mismatch found: liveness apart from S3 and S4 (a REGTOPIC timeout before
the REGCONFIRMATION or a TOPICQUERY timeout before any valid record is one failure, other
answers clear it), a REGTOPIC timeout after the REGCONFIRMATION keeps the confirmation, the
packet caps of Response Splitting apart from S1 and S4, auxiliary ENR selection (first
`AuxENRLimit` distances, one random eligible node per distance), `total` of at most 255,
`wait-time` below `W_min` treated as `W_min`, the admitted lifetime capped at `E`, and the
REGTOPIC record address rules.

## 4. geth tests

All 171 tests that geth's `topdisc` adds to `p2p/` and `cmd/devp2p` (relative to v1.17.10).
Status: `mapped` (a Rust test checks the same behaviour, possibly as part of a larger test),
`smoke` (checked by the interop smoke only), `differs` (section 3), `n/a` (geth-only
structure with no Rust counterpart).

| Status | Tests |
|---|---|
| mapped | 161 |
| smoke | 1 |
| differs | 3 |
| n/a | 6 |

### `cmd/devp2p/discv5cmd_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestParseTopic` | — | n/a: geth devp2p CLI parser; the example takes `--service`/`--topic` |
| `TestTopicFlags` | — | n/a: geth devp2p CLI flags |
| `TestTopicRPC` | `interop/geth-smoke.sh (mode basic, cases 1 and 2)` | smoke: `interop/geth-smoke.sh` basic, cases 1 and 2 |

### `table_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestTable_collectOnePerDist` | `src/service/test.rs::aux_nodes_one_per_distance_capable_only` | mapped |
| `TestTable_collectOnePerDistRejects` | `src/service/test.rs::aux_nodes_one_per_distance_capable_only` | mapped |

### `topicindex/admittedset_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestAdmittedSetExempt` | `src/topic/admittedset.rs::exempt` | mapped |
| `TestAdmittedSetKeepsFirstExpiry` | `src/topic/admittedset.rs::keeps_first_expiry` | mapped |
| `TestAdmittedSetLimit` | `src/topic/admittedset.rs::limit` | mapped |

### `topicindex/askedset_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestAskedSetKeepsFirstExpiry` | `src/topic/askedset.rs::keeps_first_expiry` | mapped |

### `topicindex/iptree_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestIPTreeInsert` | `src/topic/iptree.rs::insert_ipv4, src/topic/iptree.rs::insert_ipv6` | mapped |
| `TestIPTreeRandomChurn` | `src/topic/iptree.rs::iptree_remove_to_inserted_depth` | mapped |
| `TestIPTreeRemove` | `src/topic/iptree.rs::remove_ipv4` | mapped |
| `TestIPTreeRemoveShallowInsert` | `src/topic/iptree.rs::iptree_remove_to_inserted_depth` | mapped |
| `TestIPTreeScore` | `src/topic/iptree.rs::score_matches_insert` | mapped |

### `topicindex/registration_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestRegistrationAddNodesKeepsInflight` | `src/topic/registration.rs::add_nodes_keeps_in_flight` | mapped |
| `TestRegistrationBucketDistance` | `src/topic/registration.rs::free_space_labels` | mapped |
| `TestRegistrationBuckets` | `src/topic/registration.rs::buckets` | mapped |
| `TestRegistrationExpiry` | `src/topic/registration.rs::expiry` | mapped |
| `TestRegistrationFailedExcluded` | `src/topic/registration.rs::failed_excluded` | mapped |
| `TestRegistrationHandleTicketResponseBelowFloor` | `src/topic/registration.rs::ticket_response_below_floor` | mapped |
| `TestRegistrationHandleTicketResponseDropAboveBudget` | `src/topic/registration.rs::ticket_response_drop_above_budget` | mapped |
| `TestRegistrationIPCheck` | `src/topic/registration.rs::ip_check` | mapped |
| `TestRegistrationOnePerBucketCheck` | `src/topic/registration.rs::one_per_bucket_check` | mapped |
| `TestRegistrationOverBudgetPromotesStandby` | `src/topic/registration.rs::over_budget_promotes_standby` | mapped |
| `TestRegistrationOverloadedRing` | `src/topic/registration.rs::overloaded_ring` | mapped |
| `TestRegistrationRecordUpdate` | `src/topic/registration.rs::record_update` | mapped |
| `TestRegistrationRemoveNode` | `src/topic/registration.rs::remove_node` | mapped |
| `TestRegistrationRequests` | `src/topic/registration.rs::requests` | mapped |
| `TestRegistrationSourceCapFreedOnEviction` | `src/topic/registration.rs::source_cap_freed_on_eviction` | mapped |
| `TestRegistrationSourcePersistentCap` | `src/topic/registration.rs::source_persistent_cap` | mapped |
| `TestRegistrationUpdateBucketFairness` | `src/topic/registration.rs::update_bucket_fairness` | mapped |
| `TestRegistrationUpdateHostileBucket` | `src/topic/registration.rs::update_hostile_bucket` | mapped |
| `TestRegistrationUpdatePrefersTickets` | `src/topic/registration.rs::update_prefers_tickets` | mapped |

### `topicindex/searchfilter_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestSearchFilterExpiry` | `src/topic/searchfilter.rs::expiry` | mapped |
| `TestSearchFilterLimit` | `src/topic/searchfilter.rs::limit` | mapped |
| `TestSearchFilterMix` | `src/topic/searchfilter.rs::mix_releases_at_ten_registrars` | mapped |
| `TestSearchFilterMixContinuous` | `src/topic/searchfilter.rs::mix_continuous` | mapped |
| `TestSearchFilterMixDrops` | `src/topic/searchfilter.rs::mix_drops` | mapped |
| `TestSearchFilterMixNewerRecord` | `src/topic/searchfilter.rs::mix_newer_record` | mapped |
| `TestSearchFilterMixOverlap` | `src/topic/searchfilter.rs::mix_overlap` | mapped |
| `TestSearchFilterMixRelease` | `src/topic/searchfilter.rs::mix_release` | mapped |
| `TestSearchFilterMixShared` | `src/topic/searchfilter.rs::mix_shared` | mapped |
| `TestSearchFilterMixWait` | `src/topic/searchfilter.rs::mix_releases_after_wait` | mapped |
| `TestSearchFilterNewerRecord` | `src/topic/searchfilter.rs::newer_record` | mapped |
| `TestSearchFilterNewerRecordOnce` | `src/topic/searchfilter.rs::newer_record_once` | mapped |

### `topicindex/search_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestSearchAdaptiveAdvance` | `src/topic/search.rs::adaptive_advance` | mapped |
| `TestSearchAdaptiveAskedByID` | `src/topic/search.rs::asked_by_id` | mapped |
| `TestSearchAdaptiveConfirmedBucketHolds` | `src/topic/search.rs::confirmed_bucket_holds` | mapped |
| `TestSearchAdaptiveEmptyClosestRestartsWide` | `src/topic/search.rs::empty_closest_restarts_wide` | mapped |
| `TestSearchAdaptiveEmptyTopicRestartsWide` | `src/topic/search.rs::empty_topic_restarts_wide` | mapped |
| `TestSearchAdaptiveExplores` | `src/topic/search.rs::adaptive_explores` | mapped |
| `TestSearchAdaptiveFailuresKeepBucket` | `src/topic/search.rs::failures_keep_bucket` | mapped |
| `TestSearchAdaptiveFarHelperPullsOut` | `src/topic/search.rs::far_helper_pulls_out` | mapped |
| `TestSearchAdaptiveHelper` | `src/topic/search.rs::adaptive_helper` | mapped |
| `TestSearchAdaptiveHelperKeepsBucket` | `src/topic/search.rs::helper_keeps_bucket` | mapped |
| `TestSearchAdaptiveIsDone` | `src/topic/search.rs::adaptive_is_done` | mapped |
| `TestSearchAdaptiveIsDoneOnce` | `src/topic/search.rs::adaptive_is_done_once` | mapped |
| `TestSearchAdaptiveLiar` | `src/topic/search.rs::adaptive_liar` | mapped |
| `TestSearchAdaptiveMoveSurvivesPassEnd` | `src/topic/search.rs::move_survives_pass_end` | mapped |
| `TestSearchAdaptiveNoReaskInPass` | `src/topic/search.rs::no_reask_in_pass` | mapped |
| `TestSearchAdaptivePartialReply` | `src/topic/search.rs::partial_reply` | mapped |
| `TestSearchAdaptivePassBudget` | `src/topic/search.rs::pass_budget` | mapped |
| `TestSearchAdaptiveRepliedNodeKeepsSubnet` | `src/topic/search.rs::replied_node_keeps_subnet` | mapped |
| `TestSearchAdaptiveReplySize` | `src/topic/search.rs::adaptive_reply_size` | mapped |
| `TestSearchAdaptiveRetreat` | `src/topic/search.rs::adaptive_retreat` | mapped |
| `TestSearchAdaptiveSpare` | `src/topic/search.rs::adaptive_spare` | mapped |
| `TestSearchAdaptiveSpareAskedFromTable` | `src/topic/search.rs::spare_asked_from_table` | mapped |
| `TestSearchAdaptiveSpareNoReaskAfterExpiry` | `src/topic/search.rs::spare_no_reask_after_expiry` | mapped |
| `TestSearchAdaptiveSpareRotation` | `src/topic/search.rs::spare_rotation` | mapped |
| `TestSearchAdaptiveWalkedDry` | `src/topic/search.rs::adaptive_walked_dry` | mapped |
| `TestSearchAddNodesOnePerBucketRule` | `src/topic/search.rs::add_nodes_one_per_bucket_rule` | mapped |
| `TestSearchBucketDistanceClamp` | `src/topic/search.rs::bucket_distance_clamp` | mapped |
| `TestSearchBucketIPLimit` | `src/topic/search.rs::ip_limit` | mapped |
| `TestSearchBucketsWithFreeSpace` | `src/topic/search.rs::buckets_with_free_space` | mapped |
| `TestSearchFailedExcluded` | `src/topic/search.rs::failed_excluded` | mapped |
| `TestSearchFailedSpare` | `src/topic/search.rs::failed_spare` | mapped |
| `TestSearchHandleErrorResponse` | `src/topic/search.rs::handle_error_response` | mapped |
| `TestSearchIsDone` | `src/topic/search.rs::is_done_lifecycle` | mapped |
| `TestSearchOnePerBucketAfterIPLimit` | `src/topic/search.rs::one_per_bucket_after_ip_limit` | mapped |
| `TestSearchQueryTarget` | `src/topic/search.rs::query_target` | mapped |
| `TestSearchResultNewerRecord` | `src/topic/search.rs::result_newer_record` | mapped |
| `TestSearchResultsTracking` | `src/topic/search.rs::results_tracking` | mapped |
| `TestSearchSubnetLimitAcrossPasses` | `src/topic/search.rs::subnet_limit_across_passes` | mapped |
| `TestSearchSubnetLimitDropEndsPass` | `src/topic/search.rs::subnet_limit_drop_ends_pass` | mapped |
| `TestSearchSubnetLimitDropsHeldNode` | `src/topic/search.rs::subnet_limit_drops_held_node` | mapped |
| `TestSearchSubnetLimitSpares` | `src/topic/search.rs::subnet_limit_spares` | mapped |
| `TestSearchSubnetLimitUnqueriedSeeds` | `src/topic/search.rs::subnet_limit_unqueried_seeds` | mapped |

### `topicindex/ticket_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestTicketSealerInvalid` | `src/topic/ticket.rs::wrong_topic_and_early_ticket` | mapped |
| `TestTicketSealerKeyReuse` | `src/topic/ticket.rs::key_reuse_and_rotation` | mapped |
| `TestTicketSealerKeyRotation` | `src/topic/ticket.rs::key_reuse_and_rotation` | mapped |
| `TestTicketSealerSimple` | `src/topic/ticket.rs::seal_unseal_round_trip` | mapped |
| `TestTicketTiming` | `src/topic/ticket.rs::ticket_timing` | mapped |
| `TestUsedTickets` | `src/topic/ticket.rs::used_tickets_ttl_and_limit` | mapped |

### `topicindex/topictable_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestLowerBoundDecay` | `src/topic/bound.rs::decay_and_max_aggregation` | mapped |
| `TestTopicTableEviction` | `src/topic/table.rs::expiry_order`<br>`src/topic/table.rs::full_cache_admits_after_expiry` | mapped |
| `TestTopicTableFullRegisterExpires` | `src/topic/table.rs::full_cache_next_expiry` | mapped |
| `TestTopicTableFullWait` | `src/topic/table.rs::full_cache_next_expiry` | mapped |
| `TestTopicTableIPBoundDeeperLPM` | `src/topic/table.rs::ip_bound_survives_deeper_lpm` | mapped |
| `TestTopicTableIPBoundGC` | `src/topic/table.rs::ip_bound_recorded_and_collected` | mapped |
| `TestTopicTableRandomNodes` | `src/topic/table.rs::random_nodes` | mapped |
| `TestTopicTableRandomNodesExpired` | `src/topic/table.rs::expired_ad_not_served_or_counted` | mapped |
| `TestTopicTableRecordsIPBound` | `src/topic/table.rs::ip_bound_recorded_and_collected` | mapped |
| `TestTopicTableRegisterTwice` | `src/topic/table.rs::register_twice` | mapped |
| `TestTopicTableReregisterExpired` | `src/topic/table.rs::expired_ad_not_served_or_counted` | mapped |
| `TestTopicTableWait` | `src/topic/table.rs::wait_then_admit` | mapped |
| `TestTopicTableWaitBoundGC` | `src/topic/table.rs::topic_bound_recorded_and_collected`<br>`src/topic/table.rs::topic_bound_outliving_ads_collected` | mapped |
| `TestTopicTableWaitLowerBound` | `src/topic/table.rs::topic_lower_bound_decays` | mapped |
| `TestTopicTableWaitSafetyFull` | `src/topic/table.rs::wait_capped_24h` | mapped |
| `TestTopicTableWaitSafetyHighOccupancy` | `src/topic/table.rs::wait_safety_term` | mapped |
| `TestTopicTableWaitSafetyLowOccupancy` | `src/topic/table.rs::wait_safety_term` | mapped |
| `TestTopicTableWaitSafetyNotBounded` | `src/topic/table.rs::wait_safety_term` | mapped |
| `TestTopicTableWaitTimeCap` | `src/topic/table.rs::wait_capped_24h` | mapped |
| `TestTopicTableWaitTimeExpired` | `src/topic/table.rs::expired_ad_not_served_or_counted` | mapped |

### `v5_topic_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestCheckRegtopicRecordIP` | `src/topic/netutil.rs::regtopic_record_ip`<br>`src/service/topic.rs::regtopic_rejections_answered`<br>`src/topic/netutil.rs::regtopic_record_ip_cases` | mapped |
| `TestFilterTopicDiscovery` | `src/topic/mod.rs::enr_capability_key`<br>`src/service/topic.rs::registration_waits_for_registrars`<br>`src/service/topic.rs::search_passes` | mapped |
| `TestHandleRegtopicForeignTicket` | `src/service/topic.rs::foreign_ticket_not_consumed` | mapped |
| `TestHandleRegtopicOneClock` | — | n/a: `handle_regtopic` takes one `now` for the whole request (by signature) |
| `TestHandleRegtopicRecordIP` | `src/service/topic.rs::regtopic_rejections_answered`<br>`src/service/test.rs::regtopic_admits_and_topicquery_serves` | mapped |
| `TestHandleRegtopicReplay` | `src/service/topic.rs::replayed_ticket_new_attempt` | mapped |
| `TestHandleRegtopicReplies` | `src/service/topic.rs::regtopic_rejections_answered`<br>`src/service/topic.rs::early_ticket_echoed`<br>`src/service/topic.rs::late_ticket_credit`<br>`src/service/topic.rs::replayed_ticket_new_attempt`<br>`src/service/topic.rs::foreign_ticket_not_consumed`<br>`src/service/topic.rs::bad_signature_enr_rejected` | mapped |
| `TestHandleTopicQueryExpiredAd` | `src/service/topic.rs::topic_query_expires_ads_first`<br>`src/topic/table.rs::expired_ad_not_served_or_counted` | mapped |
| `TestRegtopicTicketCredit` | `src/service/topic.rs::late_ticket_credit` | mapped |
| `TestRegtopicTicketReplay` | `src/service/topic.rs::replayed_ticket_new_attempt`<br>`src/topic/ticket.rs::ticket_timing`<br>`src/topic/ticket.rs::used_tickets_ttl_and_limit`<br>`src/service/topic.rs::replayed_ticket_after_ad_expiry` | mapped |
| `TestTopicDHTEvictionEvictsAd` | `src/service/topic.rs::routing_table_removal_evicts`<br>`src/discv5/test.rs::table_removal_evicts_ad` | mapped |
| `TestTopicDiscoveryENRFlag` | `src/discv5/test.rs::inserts_td_and_bumps_seq` | mapped |
| `TestTopicDiscoveryFilterNodes` | `src/topic/mod.rs::enr_capability_key`<br>`src/service/topic.rs::registration_walk`<br>`src/service/topic.rs::registration_waits_for_registrars`<br>`src/service/topic.rs::search_passes` | mapped |
| `TestTopicLocalTopicNodes` | `src/discv5/test.rs::register_and_search` | mapped |
| `TestTopicLocalTopicNodesTimeout` | — | n/a: guards geth's dispatch round trip; `local_topic_nodes` reads the table under a lock |
| `TestTopicReg` | `src/discv5/test.rs::register_and_search` | mapped |
| `TestTopicRegHugeWaitTime` | `src/rpc.rs::wait_time_saturates`<br>`src/topic/registration.rs::ticket_response_drop_above_budget`<br>`src/service/topic.rs::registration_walk`<br>`src/service/topic.rs::regtopic_huge_wait_time` | mapped |
| `TestTopicRegNodeTableUpdates` | `src/service/topic.rs::registration_walk`<br>`src/service/topic.rs::registration_waits_for_registrars` | mapped |
| `TestTopicRegRejectedNotPenalized` | `src/service/topic.rs::rejection_goes_to_failed_set`<br>`src/service/topic.rs::rejection_is_not_liveness_failure` | mapped |
| `TestTopicRegRetriesNotBlockedByDeadRegistrars` | `src/service/topic.rs::ticket_retry_on_time_with_slow_registrar`<br>`src/topic/registration.rs::update_prefers_tickets`<br>`src/service/topic.rs::four_regtopics_in_flight`<br>`src/service/topic.rs::retries_not_blocked_by_dead_registrars` | mapped |
| `TestTopicRegSuccessResetsFailures` | `src/service/topic.rs::answer_restores_liveness`<br>`src/service/topic.rs::two_timeouts_then_answer` | mapped |
| `TestTopicRegTicketRetry` | `src/service/topic.rs::ticket_retry_on_time_with_slow_registrar`<br>`src/service/topic.rs::registration_walk`<br>`src/discv5/test.rs::register_with_ticket_round_trip` | mapped |
| `TestTopicRegTimeoutTracksNotEvicts` | `src/service/topic.rs::rejection_is_not_liveness_failure`<br>`src/service/topic.rs::registration_walk`<br>`src/topic/registration.rs::failed_excluded`<br>`src/service/topic.rs::regtopic_timeout_keeps_ad` | mapped |
| `TestTopicRegZeroWaitTicketClamped` | `src/topic/registration.rs::ticket_response_below_floor`<br>`src/service/topic.rs::registration_walk`<br>`src/service/topic.rs::rejection_is_not_liveness_failure` | mapped |
| `TestTopicSearch` | `src/discv5/test.rs::register_and_search`<br>`src/discv5/test.rs::register_and_search_eight_nodes` | mapped |
| `TestTopicSearchCloseAfterClose` | `src/discv5/test.rs::shutdown_ends_searches` | mapped |
| `TestTopicSearchDrainsTableFeed` | — | n/a: geth's node feed; Rust reads a bounded change log (`table_changes_bounded`) |
| `TestTopicSearchIteratorClose` | `src/service/topic.rs::dropped_search_ends`<br>`src/discv5.rs::drop_closes_and_notifies`<br>`src/service/topic.rs::stream_drives_running_search` | mapped |
| `TestTopicSearchMixesRegistrars` | `src/topic/searchfilter.rs::mix_releases_at_ten_registrars`<br>`src/topic/searchfilter.rs::mix_continuous` | mapped |
| `TestTopicSearchNextPassDeadFrontier` | `src/service/topic.rs::retry_pass_once`<br>`src/topic/search.rs::failures_keep_bucket`<br>`src/topic/search.rs::failed_excluded`<br>`src/service/topic.rs::dead_frontier_kept_once` | mapped |
| `TestTopicSearchNextPassEmptyTopic` | `src/topic/search.rs::empty_topic_restarts_wide`<br>`src/topic/search.rs::empty_closest_restarts_wide`<br>`src/topic/search.rs::adaptive_walked_dry`<br>`src/topic/search.rs::adaptive_is_done_once`<br>`src/service/topic.rs::restart_at_farthest`<br>`src/service/topic.rs::backoff_idle_definition` | mapped |
| `TestTopicSearchNextPassPlacedFailures` | `src/topic/search.rs::answered_or_placed_not_failed`<br>`src/topic/search.rs::confirmed_bucket_holds` | mapped |
| `TestTopicSearchNoRepeats` | `src/service/topic.rs::search_passes`<br>`src/discv5/test.rs::search_stream_finds_advertisers`<br>`src/topic/searchfilter.rs::expiry` | mapped |
| `TestTopicSearchPassGap` | `src/service/topic.rs::search_pass_gap`<br>`src/service/topic.rs::backoff_idle_definition` | mapped |
| `TestTopicSearchRunPartialReply` | `src/service/topic.rs::search_partial_reply`<br>`src/topic/search.rs::partial_reply`<br>`src/service/topic.rs::pass_waits_for_reads` | mapped |
| `TestTopicSearchRunSubnetLimitEndsPass` | `src/topic/search.rs::subnet_limit_drop_ends_pass`<br>`src/service/topic.rs::subnet_limit_ends_pass` | mapped |
| `TestTopicSearchRunUnsupportedAds` | `src/service/topic.rs::search_sample_before_td`<br>`src/service/topic.rs::backoff_idle_definition`<br>`src/service/topic.rs::search_unsupported_ads_idle` | mapped |
| `TestTopicSearchStopsOnClose` | `src/discv5/test.rs::shutdown_ends_searches` | mapped |
| `TestTopicSearchStopsWhenUnread` | `src/service/topic.rs::queries_stop_at_unread_limit`<br>`src/service/topic.rs::stream_drives_running_search`<br>`src/service/topic.rs::search_passes` | mapped |
| `TestTopicSearchTimeoutNotReasked` | `src/topic/search.rs::failed_excluded`<br>`src/topic/search.rs::failed_spare`<br>`src/service/topic.rs::search_timeout_not_reasked` | mapped |
| `TestTopicStartAfterClose` | `src/discv5/test.rs::shutdown_ends_searches` | mapped |
| `TestTopicStopPendingEviction` | — | n/a: guards geth's eviction channel at shutdown; Rust evicts in the service task, no channel |
| `TestTopicStopRegister` | `src/discv5/test.rs::register_and_search`<br>`src/service/topic.rs::registration_walk`<br>`src/service/topic.rs::stop_and_register_again` | mapped |

### `v5_udp_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestUDPv5_RegtopicClosedAfterConfirmation` | `src/service/topic.rs::regtopic_closed_after_confirmation` | mapped |
| `TestUDPv5_RegtopicLostNodes` | `src/service/topic.rs::confirmation_kept_when_nodes_missing`<br>`src/service/topic.rs::post_confirmation_timeout_keeps_registration`<br>`src/service/topic.rs::rejection_is_not_liveness_failure` | mapped |
| `TestUDPv5_RegtopicNodesOnlyTimeout` | `src/service/topic.rs::confirmation_kept_when_nodes_missing (second half)`<br>`src/service/topic.rs::rejection_is_not_liveness_failure`<br>`src/service/topic.rs::failed_regtopic_keeps_aux_nodes` | mapped |
| `TestUDPv5_RegtopicPacketLimit` | `src/service/topic.rs::reg_resp_limit_without_confirmation_fails`<br>`src/service/topic.rs::topic_response_caps (REGTOPIC half)`<br>`src/service/topic.rs::regtopic_packet_limit` | mapped |
| `TestUDPv5_RegtopicTooManyNodes` | `src/service/topic.rs::reg_resp_limit_without_confirmation_fails`<br>`src/service/topic.rs::failed_regtopic_keeps_aux_nodes` | mapped |
| `TestUDPv5_TopicQueryAnnouncedTotal` | `src/service/topic.rs::topic_response_caps (TOPICQUERY half)`<br>`src/service/topic.rs::first_total_wins (TOPICQUERY half)`<br>`src/service/topic.rs::topic_query_empty_and_failed`<br>`src/service/topic.rs::query_cap_below_announced_total` | differs (S4): announced 200 ends complete at 16 packets in geth; Rust waits for 24 and calls it partial. Announced 2 and 16 are complete in both |
| `TestUDPv5_TopicQueryPartialAtLimit` | `src/service/topic.rs::query_partial_response` | differs (S4): same outcome, partial with the records, but at 16 packets in geth and 24 in Rust |
| `TestUDPv5_TopicQueryUnexpectedPackets` | `src/service/topic.rs::query_partial_response`<br>`src/service/topic.rs::query_cap_without_records_fails` | differs (S4): same outcome, failed, but at 16 packets in geth and 24 in Rust |

### `v5wire/topic_msg_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestTopicMessageDecodeErrors` | `src/rpc.rs::rejects_malformed`<br>`src/rpc.rs::reject_extra_data`<br>`src/rpc.rs::reject_oversized_request_id`<br>`src/rpc.rs::rejects_malformed_topic_kinds` | mapped |
| `TestTopicMessageFieldsAtCap` | `src/rpc.rs::rejects_malformed`<br>`src/rpc.rs::decodes_topic_fields_at_bounds` | mapped |
| `TestTopicMessageOpaqueTicket` | `src/rpc.rs::decodes_topic_fields_at_bounds` | mapped |
| `TestTopicMessageOversizedFields` | `src/rpc.rs::rejects_malformed`<br>`src/rpc.rs::rejects_malformed_topic_kinds` | mapped |
| `TestTopicMessageReqIDBoundary` | `src/rpc.rs::reject_oversized_request_id`<br>`src/rpc.rs::decodes_topic_fields_at_bounds`<br>`src/rpc.rs::rejects_malformed_topic_kinds` | mapped |
| `TestTopicMessageRLP` | `src/rpc.rs::spec_vectors_decode_encode` | mapped |

### `v5wire/topic_vectors_test.go`

| geth test | Rust test(s) | status |
|---|---|---|
| `TestVectorsTopicMessages` | `src/rpc.rs::spec_vectors_decode_encode`<br>`src/rpc.rs::nodes_vector_unchanged` | mapped |

## 5. Live evidence

- **geth interop smoke:** `interop/geth-smoke.sh all` against `devp2p` built from geth
  `3b8bef9e3` (`go build ./cmd/devp2p`), three runs on 2026-11-23: every run passed all five
  checks (cases 1, 2 and 4, case 3 in both directions, no case rerun), about 27 s per run. Case
  3 quoted the same 8,604 ms wait at the geth and at the Rust registrar (ad lifetime 60 s, one
  ad in a cache of 10).
- **One `/24`:** `discv5/test.rs::many_advertisers_one_subnet`: 12 advertisers, 3 registrars
  and a searcher on distinct addresses of 127.0.0.0/24, as on a docker network, each on a port
  free on its address. The registrars sit in the same registration bucket, so each advertiser
  registers with all three only through the LAN exemption of the one-per-`/24` rule; with the
  exemption removed the test fails (ads held per registrar 5, 2 and 5 of 12). The searcher knows
  only the registrars and finds all 12 advertisers. Which registrars it asks depends on how the
  yield moves the search, so the search side of the exemption is checked by unit tests:
  `topic/search.rs::ip_limit` and `topic/admittedset.rs::exempt`.

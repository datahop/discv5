# TopDisc (DISC-NG) in discv5

This fork adds topic-based service discovery (TopDisc) to discv5, as specified in
datahop/devp2p `discv5/discv5-theory.md` and `discv5/discv5-wire.md`. `PLAN.md` describes
what is implemented and where; this file is the usage guide. The code matches the spec at
datahop/devp2p `ff1e30ab0` and geth at datahop/go-ethereum `topdisc` `3b8bef9e3`;
`docs/geth-parity.md` has the evidence and the differences.

## Enable

```toml
discv5 = { git = "https://github.com/datahop/discv5", branch = "discng-clients/m1", features = ["topic-discovery"] }
```

With the feature on, the node always runs TopDisc: its ENR carries `td = 1`, it answers
REGTOPIC and TOPICQUERY as a registrar, and the API below is available. `ConfigBuilder::topic_config`
takes a `topic::TopicConfig`; its defaults are the spec's parameters.

## API

```rust
use discv5::topic::topic_id;
use futures::StreamExt;

let topic = topic_id("my-service"); // SHA-256 of the name

// Advertise: keeps ads for the local node alive until stopped.
discv5.register_topic(topic)?;
discv5.stop_topic_registration(topic)?;

// Search: one lookup that runs pass after pass until the stream is dropped.
let mut stream = discv5.topic_search_stream(topic)?;
while let Some(enr) = stream.next().await {
    // each advertiser once per ad lifetime, once more with a newer record
}

// Or collect `count` distinct advertisers, bounded by `Config::query_timeout`.
let found = discv5.topic_search(topic, 10).await?;

// Registrar side: the ads this node holds.
let ads = discv5.local_topic_nodes(topic);
```

- `register_topic`, `stop_topic_registration` and `topic_search_stream` return at once;
  they fail with `Error::Custom("Service channel full")` when the service is backlogged.
- A stream with 16 unread results pauses its lookup until it is read.
- `Event::TopicRegistered { topic, registrar }` reports each admission.
- `send_regtopic` and `send_topic_query` send one request to a chosen node, for tools and
  tests. `send_regtopic` returns a `RegTopicResult` whose `confirmation` is
  `Admitted`, `Ticket` or `Rejected`.
- `Discv5::metrics().topic` holds the TopDisc counters (messages, tickets issued and
  received, rejections sent and received, ticket outcomes).

## Example node

`examples/topdisc_node.rs` is a node for interop runs. It prints one JSON line per event on
stdout and serves its state as JSON on `--status-port`.

```sh
cargo run --features topic-discovery --example topdisc_node -- \
    --port 31400 --status-port 41400 --service my-service \
    --bootnode enr:... --register --search 10
```

| Flag | Use |
|---|---|
| `--service <name>` / `--topic <hex>` | the topic, by name (SHA-256) or as 32 bytes of hex |
| `--register` | keep an ad for the topic alive |
| `--search <count>` | run one lookup until shutdown; print `found` per result and `search_end` every `count` results |
| `--ad-cache-size`, `--ad-lifetime` | registrar ad cache capacity and ad lifetime (s) |
| `--probe-regtopic <enr>` | send one REGTOPIC to that node, print `admitted`, `ticket` or `rejected`, exit |
| `--enr-ip`, `--listen-ip`, `--port` | addresses; `--enr-ip` may differ from the socket address |
| `--bootnode`, `--bootnode-file` | bootnode ENRs |
| `--report-topic <hex>` | more topics whose held ads `/status` lists |
| `--lookup-interval <s>` | seconds between the FINDNODE lookups that fill the routing table (default 30) |
| `--register-after <s>` | seconds to wait after start before registering (default 0) |
| `--search-timeout <s>` | query timeout of the node (default 60) |
| `--key <hex>` | private key; a random one when absent |
| `--status-port <port>` | TCP port of the `/status` server; no server when absent |

`/status` fields include `ads_held_ids` (per topic), `registrars`, `advertiser_ids`,
`searches_done`, the message counters, and `tickets_issued`, `tickets_recv`,
`rejections_sent`, `rejections_recv`.

## Interop smoke

`interop/geth-smoke.sh` runs the example against geth's `devp2p discv5 listen` on
loopback. See `interop/README.md`.

```sh
GETH_DEVP2P=/path/to/devp2p interop/geth-smoke.sh all
```

## Tests

```sh
cargo test --features topic-discovery --lib --bins --tests --examples
```

Leave out doctests: the crate example binds UDP 9000.

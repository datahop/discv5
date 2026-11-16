# Interop smoke against geth

`geth-smoke.sh` runs geth `devp2p discv5 listen` nodes and `examples/topdisc_node` nodes
on 127.0.0.1 and checks TopDisc between them in both directions.

## Build geth devp2p

From a geth checkout at the `topdisc` branch (datahop fork), into a scratch directory:

```sh
go build -p 16 -o /tmp/<rundir>/devp2p ./cmd/devp2p
```

## Run

```sh
GETH_DEVP2P=/tmp/<rundir>/devp2p interop/geth-smoke.sh all
```

Modes: `basic`, `all` (cases 1 to 4), or one of `case1` .. `case4`. The script builds the
example with `cargo build --example topdisc_node --features topic-discovery` (set
`TOPDISC_NODE=<path>` to use a prebuilt binary). It prints one `PASS`/`FAIL` line per check
and exits non-zero on any `FAIL`. Each case starts its own nodes and stops them at the end.
Topics are SHA-256 of fixed names: the Rust node gets `--service <name>`, geth the same name
in its RPC calls.

## Mode `basic`

The Rust node registers `sha256("topdisc-smoke/rust")` with geth as its only registrar;
geth, with the Rust node as its only bootnode, registers `sha256("topdisc-smoke/geth")`.
Pass: geth `discv5_topicNodes` lists the Rust node, and the Rust `/status` `ads_held_ids`
lists geth.

## Cases of `all`

1. Rust -> geth. A Rust node registers; geth `discv5_topicNodes` on its registrar lists it,
   then a second geth node's `discv5_topicSearch(topic, 1)` returns it.
2. geth -> Rust. geth `discv5_registerTopic`; the Rust registrar's `/status` lists it, then
   a second Rust node with `--search 1` yields it (`advertiser_ids`).
3. Ticket round trip, both ways. A registrar with ad cache 10 and ad lifetime 60 s (geth
   `--config` with `adCacheSize`, `adLifetimeSeconds`; Rust `--ad-cache-size 10
   --ad-lifetime 60`). The first advertiser is admitted; the second starts once the first ad
   is stored, gets a ticket (about 8.6 s by the waiting-time function with one ad in the
   cache) and is admitted on retry. The registrar must hold the second ad, and the logs must
   tie the ticket to that pair: at the geth registrar, the second Rust advertiser logs a
   ticket from the registrar's id, then an admission from it; at the Rust registrar (trace
   log), a REGCONFIRMATION to the second geth advertiser with `admitted=false`, then one with
   `admitted=true` and `waited_ms` above 0. The PASS line shows the quoted wait. A direction
   that fails is run once more with fresh nodes.
4. Rejection. A Rust `--probe-regtopic` with `--enr-ip 127.0.0.9` (the record IP differs
   from the packet source) against geth exits 0 within 30 s, prints `rejected`, and geth logs
   `address does not match packet source`.

A geth RPC check passes only when curl succeeds and the body is exactly one JSON object
with a `result` and no `error`; ids must be members of the result. Rust `/status` checks
also need exactly one JSON document. A node that does not start
fails its case. On exit, also by SIGINT or SIGTERM, the script stops every node it started.

Searches get `SEARCH_WAIT` seconds (default 60): with fewer than 10 registrars the mixer
releases results after 5 s or at the end of a pass.

## Ports and environment

| Variable       | Default            | Use                                             |
|----------------|--------------------|-------------------------------------------------|
| `UDP_BASE`     | 31400              | node slot `i` uses UDP `UDP_BASE + i`, i = 0..9 |
| `TCP_BASE`     | 41400              | node slot `i` serves status or RPC on `TCP_BASE + i` |
| `WAIT`         | 60                 | seconds to wait for an ad                       |
| `SEARCH_WAIT`  | 60                 | seconds to wait for a search                    |
| `SMOKE_LOGDIR` | unset              | keep each run's logs in a subdirectory here     |
| `KEEP_RUNDIR`  | 0                  | 1 keeps the logs of a `/tmp` run                |
| `RUST_LOG`     | `info,discv5=debug`| Rust node logs                                  |

All 20 ports are checked free before the run, and 9000, 9001, 30303, 8545 and 5052 are
refused. geth always gets `--bootnodes` (possibly empty), so it never contacts the mainnet
bootnodes. Without `SMOKE_LOGDIR` the logs go to a `/tmp/topdisc-smoke.*` directory, removed
on exit unless a check fails or `KEEP_RUNDIR=1`. The script needs `curl`, `jq` and `ss`.

# Interop smoke against geth

`geth-smoke.sh` runs one geth `devp2p discv5 listen` node and one `examples/topdisc_node`
on 127.0.0.1, and checks that each holds an advertisement of the other.

## Build geth devp2p

From a geth checkout at the `topdisc` branch (datahop fork), into a scratch directory:

```sh
go build -p 16 -o /tmp/<rundir>/devp2p ./cmd/devp2p
```

## Run

```sh
GETH_DEVP2P=/tmp/<rundir>/devp2p interop/geth-smoke.sh basic
```

The script builds the example with `cargo build --example topdisc_node --features
topic-discovery` (set `TOPDISC_NODE=<path>` to use a prebuilt binary). It exits 0 when both
checks pass and prints one `PASS`/`FAIL` line per check.

## Mode `basic`

- Topics: `T1 = sha256("topdisc-smoke/rust")`, `T2 = sha256("topdisc-smoke/geth")`.
- The Rust node registers T1; geth is its only registrar (via `--bootnode-file`).
- geth starts with the Rust ENR as its only bootnode and registers T2 through
  `discv5_registerTopic`.
- Pass: geth `discv5_topicNodes(T1)` lists the Rust node id, and the Rust `/status`
  `ads_held_ids` for T2 lists the geth node id, within `WAIT` seconds (default 60).

## Ports and environment

| Variable      | Default | Use                    |
|---------------|---------|------------------------|
| `RUST_UDP`    | 30410   | Rust discv5 UDP        |
| `RUST_STATUS` | 30411   | Rust status (TCP)      |
| `GETH_UDP`    | 30420   | geth discv5 UDP        |
| `GETH_RPC`    | 30421   | geth JSON-RPC (TCP)    |
| `WAIT`        | 60      | seconds to wait        |
| `KEEP_RUNDIR` | 0       | 1 keeps the logs       |

The script refuses ports that are in use and the ports 9000, 9001, 30303, 8545 and 5052.
geth always gets `--bootnodes`, so it never contacts the mainnet bootnodes. Logs go to a
`/tmp/topdisc-smoke.*` directory, removed on exit unless a check fails or `KEEP_RUNDIR=1`.

#!/usr/bin/env bash
# Loopback interop smoke between geth `devp2p discv5 listen` and examples/topdisc_node.
# Usage: GETH_DEVP2P=<path to devp2p> interop/geth-smoke.sh basic
# See interop/README.md.
set -euo pipefail

MODE=${1:-basic}
[[ $MODE == basic ]] || { echo "unknown mode: $MODE (supported: basic)" >&2; exit 2; }
: "${GETH_DEVP2P:?set GETH_DEVP2P to a devp2p binary built from geth topdisc}"
[[ -x $GETH_DEVP2P ]] || { echo "not executable: $GETH_DEVP2P" >&2; exit 2; }

REPO=$(cd "$(dirname "$0")/.." && pwd)
RUST_UDP=${RUST_UDP:-30410}
RUST_STATUS=${RUST_STATUS:-30411}
GETH_UDP=${GETH_UDP:-30420}
GETH_RPC=${GETH_RPC:-30421}
WAIT=${WAIT:-60}
KEEP_RUNDIR=${KEEP_RUNDIR:-0}

for port in "$RUST_UDP" "$RUST_STATUS" "$GETH_UDP" "$GETH_RPC"; do
    case $port in 9000 | 9001 | 30303 | 8545 | 5052) echo "port $port is reserved" >&2; exit 2 ;; esac
    if ss -Hlun "sport = :$port" | grep -q . || ss -Hltn "sport = :$port" | grep -q .; then
        echo "port $port is in use" >&2
        exit 2
    fi
done

RUNDIR=$(mktemp -d /tmp/topdisc-smoke.XXXXXX)
PIDS=()
cleanup() {
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [[ $KEEP_RUNDIR == 1 ]]; then echo "run dir kept: $RUNDIR"; else rm -rf "$RUNDIR"; fi
}
trap cleanup EXIT

sha256hex() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
# T1: advertised by the Rust node at geth. T2: advertised by geth at the Rust node.
T1=$(sha256hex "topdisc-smoke/rust")
T2=$(sha256hex "topdisc-smoke/geth")

rpc() {
    curl -sf --connect-timeout 1 --max-time 2 -X POST -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" \
        "http://127.0.0.1:$GETH_RPC"
}
node_id() { "$GETH_DEVP2P" enrdump "$1" | awk '/^Node ID:/ {print $3}'; }

if [[ -n ${TOPDISC_NODE:-} ]]; then
    RUST_BIN=$TOPDISC_NODE
else
    (cd "$REPO" && cargo build -q --example topdisc_node --features topic-discovery)
    RUST_BIN=${CARGO_TARGET_DIR:-$REPO/target}/debug/examples/topdisc_node
fi

echo "run dir: $RUNDIR"
RUST_LOG=${RUST_LOG:-info} "$RUST_BIN" --port "$RUST_UDP" --status-port "$RUST_STATUS" \
    --topic "$T1" --report-topic "$T2" --register \
    --bootnode-file "$RUNDIR/geth.enr" --lookup-interval 5 \
    >"$RUNDIR/rust.out" 2>"$RUNDIR/rust.err" &
PIDS+=($!)

for _ in $(seq 50); do
    RUST_ENR=$(grep -m1 '"event":"start"' "$RUNDIR/rust.out" 2>/dev/null |
        sed 's/.*"enr":"\([^"]*\)".*/\1/') || true
    [[ -n ${RUST_ENR:-} ]] && break
    sleep 0.2
done
[[ -n ${RUST_ENR:-} ]] || {
    echo "FAIL: the Rust node did not start" >&2
    tail "$RUNDIR/rust.err" >&2
    KEEP_RUNDIR=1
    exit 1
}
RUST_ENR=enr:${RUST_ENR#enr:}
RUST_ID=$(node_id "$RUST_ENR")

"$GETH_DEVP2P" --verbosity 3 discv5 listen --addr "127.0.0.1:$GETH_UDP" \
    --rpc "127.0.0.1:$GETH_RPC" --bootnodes "$RUST_ENR" \
    >"$RUNDIR/geth.out" 2>"$RUNDIR/geth.err" &
PIDS+=($!)

for _ in $(seq 50); do
    GETH_ENR=$(rpc discv5_localNode '[]' 2>/dev/null | sed -n 's/.*"result":"\([^"]*\)".*/\1/p') || true
    [[ -n ${GETH_ENR:-} ]] && break
    sleep 0.2
done
[[ -n ${GETH_ENR:-} ]] || {
    echo "FAIL: geth did not start" >&2
    tail "$RUNDIR/geth.err" >&2
    KEEP_RUNDIR=1
    exit 1
}
GETH_ID=$(node_id "$GETH_ENR")
echo "$GETH_ENR" >"$RUNDIR/geth.enr"
echo "rust $RUST_ID udp $RUST_UDP"
echo "geth $GETH_ID udp $GETH_UDP"

rpc discv5_registerTopic "[\"0x$T2\"]" >/dev/null || {
    echo "FAIL: registerTopic" >&2
    KEEP_RUNDIR=1
    exit 1
}

rust_holds_geth=0
geth_holds_rust=0
deadline=$((SECONDS + WAIT))
while ((SECONDS < deadline)); do
    if [[ $geth_holds_rust == 0 ]]; then
        for enr in $(rpc discv5_topicNodes "[\"0x$T1\"]" | grep -o 'enr:[A-Za-z0-9_-]*' || true); do
            [[ $(node_id "$enr") == "$RUST_ID" ]] && geth_holds_rust=1
        done
    fi
    if [[ $rust_holds_geth == 0 ]] &&
        curl -sf --connect-timeout 1 --max-time 2 "http://127.0.0.1:$RUST_STATUS/" | grep -o "\"ads_held_ids\":{[^}]*}" |
        grep -q "\"$T2\":\[[^]]*\"$GETH_ID\""; then
        rust_holds_geth=1
    fi
    [[ $geth_holds_rust == 1 && $rust_holds_geth == 1 ]] && break
    sleep 1
done

status=0
if [[ $geth_holds_rust == 1 ]]; then
    echo "PASS rust->geth: geth discv5_topicNodes(T1) lists the Rust node"
else
    echo "FAIL rust->geth: geth does not hold the Rust node's ad for T1"
    status=1
fi
if [[ $rust_holds_geth == 1 ]]; then
    echo "PASS geth->rust: Rust /status ads_held_ids(T2) lists the geth node"
else
    echo "FAIL geth->rust: the Rust node does not hold geth's ad for T2"
    status=1
fi
if [[ $status != 0 ]]; then
    KEEP_RUNDIR=1
fi
exit $status

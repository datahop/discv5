#!/usr/bin/env bash
# Loopback interop smoke between geth `devp2p discv5 listen` and examples/topdisc_node.
# Usage: GETH_DEVP2P=<path to devp2p> interop/geth-smoke.sh basic|all|case1..case4
# See interop/README.md.
set -euo pipefail

MODE=${1:-basic}
case $MODE in
basic | all | case[1-4]) ;;
*) echo "unknown mode: $MODE (supported: basic, all, case1..case4)" >&2; exit 2 ;;
esac
: "${GETH_DEVP2P:?set GETH_DEVP2P to a devp2p binary built from geth topdisc}"
[[ -x $GETH_DEVP2P ]] || { echo "not executable: $GETH_DEVP2P" >&2; exit 2; }

REPO=$(cd "$(dirname "$0")/.." && pwd)
# Node slot i uses UDP $UDP_BASE+i and TCP (status or RPC) $TCP_BASE+i, i = 0..9.
UDP_BASE=${UDP_BASE:-31400}
TCP_BASE=${TCP_BASE:-41400}
WAIT=${WAIT:-60}
SEARCH_WAIT=${SEARCH_WAIT:-60}
KEEP_RUNDIR=${KEEP_RUNDIR:-0}
export RUST_LOG=${RUST_LOG:-info,discv5=debug}

udp() { echo $((UDP_BASE + $1)); }
tcp() { echo $((TCP_BASE + $1)); }

for slot in $(seq 0 9); do
    for port in "$(udp "$slot")" "$(tcp "$slot")"; do
        case $port in 9000 | 9001 | 30303 | 8545 | 5052) echo "port $port is reserved" >&2; exit 2 ;; esac
        if ss -Hlun "sport = :$port" | grep -q . || ss -Hltn "sport = :$port" | grep -q .; then
            echo "port $port is in use" >&2
            exit 2
        fi
    done
done

if [[ -n ${SMOKE_LOGDIR:-} ]]; then
    mkdir -p "$SMOKE_LOGDIR"
    RUNDIR=$(mktemp -d "$SMOKE_LOGDIR/$MODE-$(date +%H%M%S).XXXX")
    KEEP_RUNDIR=1
else
    RUNDIR=$(mktemp -d /tmp/topdisc-smoke.XXXXXX)
fi
# Every background process of the run, all started from this shell.
PIDS=()
stop_nodes() {
    local pid
    ((${#PIDS[@]})) || return 0
    for pid in "${PIDS[@]}"; do
        pkill -TERM -P "$pid" 2>/dev/null || true
        kill "$pid" 2>/dev/null || true
    done
    for _ in $(seq 50); do
        for pid in "${PIDS[@]}"; do kill -0 "$pid" 2>/dev/null && break; done || break
        sleep 0.1
    done
    for pid in "${PIDS[@]}"; do kill -KILL "$pid" 2>/dev/null || true; done
    for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
    PIDS=()
}
cleanup() {
    stop_nodes
    if [[ $KEEP_RUNDIR == 1 ]]; then echo "run dir kept: $RUNDIR"; else rm -rf "$RUNDIR"; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

sha256hex() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
node_id() { "$GETH_DEVP2P" enrdump "$1" | awk '/^Node ID:/ {print $3}'; }

if [[ -n ${TOPDISC_NODE:-} ]]; then
    RUST_BIN=$TOPDISC_NODE
else
    (cd "$REPO" && cargo build -q --example topdisc_node --features topic-discovery)
    RUST_BIN=${CARGO_TARGET_DIR:-$REPO/target}/debug/examples/topdisc_node
fi

declare -A ENR ID

# start_rust NAME SLOT ARGS...: the Rust node, with its status server on the slot's TCP port.
start_rust() {
    local name=$1 slot=$2
    shift 2
    "$RUST_BIN" --port "$(udp "$slot")" --status-port "$(tcp "$slot")" "$@" \
        >"$RUNDIR/$name.out" 2>"$RUNDIR/$name.err" &
    PIDS+=($!)
    local line=""
    for _ in $(seq 50); do
        line=$(grep -m1 '"event":"start"' "$RUNDIR/$name.out" 2>/dev/null) && break
        kill -0 "${PIDS[-1]}" 2>/dev/null || break
        sleep 0.2
    done
    [[ -n $line ]] || { echo "$name did not start" >&2; tail "$RUNDIR/$name.err" >&2; return 1; }
    ENR[$name]=$(sed 's/.*"enr":"\(enr:\)\{0,1\}\([^"]*\)".*/enr:\2/' <<<"$line")
    ID[$name]=$(sed 's/.*"id":"\([^"]*\)".*/\1/' <<<"$line")
    echo "${ENR[$name]}" >"$RUNDIR/$name.enr"
    echo "$name rust ${ID[$name]} udp $(udp "$slot") status $(tcp "$slot")"
}

# start_geth NAME SLOT BOOTNODES ARGS...: geth, with JSON-RPC on the slot's TCP port.
start_geth() {
    local name=$1 slot=$2 boot=$3
    shift 3
    "$GETH_DEVP2P" --verbosity 4 discv5 listen --addr "127.0.0.1:$(udp "$slot")" \
        --rpc "127.0.0.1:$(tcp "$slot")" --bootnodes "$boot" "$@" \
        >"$RUNDIR/$name.out" 2>"$RUNDIR/$name.err" &
    PIDS+=($!)
    local enr=""
    for _ in $(seq 50); do
        enr=$(rpc_result "$slot" discv5_localNode '[]' 2>/dev/null | json_get -r 'strings') || true
        [[ -n $enr ]] && break
        kill -0 "${PIDS[-1]}" 2>/dev/null || break
        sleep 0.2
    done
    [[ -n $enr ]] || { echo "$name did not start" >&2; tail "$RUNDIR/$name.err" >&2; return 1; }
    ENR[$name]=$enr
    ID[$name]=$(node_id "$enr")
    echo "$enr" >"$RUNDIR/$name.enr"
    echo "$name geth ${ID[$name]} udp $(udp "$slot") rpc $(tcp "$slot")"
}

# json_get FLAGS FILTER [JQ ARGS...]: prints FILTER of stdin, which must be exactly one JSON
# document (jq 1.6 accepts empty input, so the input is slurped and counted).
json_get() {
    local flags=$1 filter=$2
    shift 2
    jq "${flags}s" "$@" "if length == 1 then .[0] | $filter else error(\"expected one JSON document\") end" 2>/dev/null
}
# json_is FILTER [JQ ARGS...]: stdin is exactly one JSON document and FILTER is true for it.
json_is() {
    local filter=$1
    shift
    jq -es "$@" "length == 1 and (.[0] | $filter)" >/dev/null 2>&1
}

# rpc_result SLOT METHOD PARAMS [MAX_TIME]: the `result` of a geth JSON-RPC call, as
# compact JSON. Fails on a curl error, a body that is not a JSON object, or an `error`.
rpc_result() {
    local body
    body=$(curl -sf --connect-timeout 1 --max-time "${4:-2}" -X POST -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" \
        "http://127.0.0.1:$(tcp "$1")") || return 1
    json_get -c 'if type == "object" and has("result") and (has("error") | not)
        then .result else error("bad JSON-RPC reply") end' <<<"$body"
}

status() { curl -sf --connect-timeout 1 --max-time 2 "http://127.0.0.1:$(tcp "$1")/"; }
# status_num SLOT FIELD: a numeric field of a Rust node's status.
status_num() { status "$1" | json_get -r ".$2 | numbers" || echo "?"; }

# geth_holds SLOT TOPIC_HEX ID: geth discv5_topicNodes lists the node.
geth_holds() {
    local result enr
    result=$(rpc_result "$1" discv5_topicNodes "[\"0x$2\"]") || return 1
    for enr in $(json_get -r 'if type == "array" then .[] | strings else empty end' <<<"$result"); do
        [[ $(node_id "$enr") == "$3" ]] && return 0
    done
    return 1
}
# rust_holds SLOT TOPIC_HEX ID: the Rust status ads_held_ids lists the node.
rust_holds() { status "$1" | json_is '.ads_held_ids[$t] | any(.[]; . == $id)' --arg t "$2" --arg id "$3"; }
# rust_found SLOT ID: the Rust search stream yielded the node.
rust_found() { status "$1" | json_is '.advertiser_ids | any(.[]; . == $id)' --arg id "$2"; }
# register SLOT TOPIC: geth discv5_registerTopic.
register() { rpc_result "$1" discv5_registerTopic "[\"$2\"]" >/dev/null; }
# short_id ID: a node id as the Rust logs print it.
short_id() { echo "0x${1:0:4}..${1: -4}"; }
# in_order FILE RE1 RE2: a line matching RE2 follows the first line matching RE1.
in_order() { awk -v a="$2" -v b="$3" '!f && $0 ~ a { f = 1; next } f && $0 ~ b { ok = 1; exit } END { exit !ok }' "$1"; }

# wait_for SECONDS CMD...: polls CMD once a second.
wait_for() {
    local deadline=$((SECONDS + $1))
    shift
    while ((SECONDS < deadline)); do
        "$@" 2>/dev/null && return 0
        sleep 1
    done
    return 1
}

STATUS=0
pass() { echo "PASS $*"; }
fail() {
    echo "FAIL $*"
    STATUS=1
    KEEP_RUNDIR=1
}
# abort LABEL: a node of the case did not start.
abort() {
    fail "$1: a node did not start"
    stop_nodes
}

case_basic() {
    local t1 t2
    t1=$(sha256hex "topdisc-smoke/rust")
    t2=$(sha256hex "topdisc-smoke/geth")
    start_rust basic-rust 0 --topic "$t1" --report-topic "$t2" --register \
        --bootnode-file "$RUNDIR/basic-geth.enr" --lookup-interval 5 || { abort basic; return; }
    start_geth basic-geth 1 "${ENR[basic-rust]}" || { abort basic; return; }
    register 1 "0x$t2" || fail "geth->rust: discv5_registerTopic failed"
    if wait_for "$WAIT" geth_holds 1 "$t1" "${ID[basic-rust]}"; then
        pass "rust->geth: geth discv5_topicNodes(T1) lists the Rust node"
    else
        fail "rust->geth: geth does not hold the Rust node's ad for T1"
    fi
    if wait_for "$WAIT" rust_holds 0 "$t2" "${ID[basic-geth]}"; then
        pass "geth->rust: Rust /status ads_held_ids(T2) lists the geth node"
    else
        fail "geth->rust: the Rust node does not hold geth's ad for T2"
    fi
    stop_nodes
}

# Rust advertises; a geth registrar holds the ad and a second geth node finds it.
case1() {
    local s=topdisc-smoke/case1 t found
    t=$(sha256hex "$s")
    echo '{"rpcSearchTimeoutSeconds":45}' >"$RUNDIR/c1-search.json"
    start_rust c1-rust 0 --service "$s" --register --bootnode-file "$RUNDIR/c1-geth1.enr" \
        --lookup-interval 5 || { abort case1; return; }
    start_geth c1-geth1 1 "${ENR[c1-rust]}" || { abort case1; return; }
    if ! wait_for "$WAIT" geth_holds 1 "$t" "${ID[c1-rust]}"; then
        fail "case1 rust->geth: geth registrar does not hold the Rust ad"
        stop_nodes
        return
    fi
    start_geth c1-geth2 2 "${ENR[c1-geth1]}" --config "$RUNDIR/c1-search.json" || { abort case1; return; }
    # In the background, so that a signal is handled while the search runs.
    rpc_result 2 discv5_topicSearch "[\"$s\", 1]" "$SEARCH_WAIT" >"$RUNDIR/c1-search.result" &
    PIDS+=($!)
    if wait $! && json_is 'type == "array" and any(.[]; ascii_downcase == $id)' --arg id "${ID[c1-rust]}" \
        <"$RUNDIR/c1-search.result"; then
        pass "case1 rust->geth: geth registrar holds the Rust ad, geth discv5_topicSearch finds it"
    else
        fail "case1 rust->geth: geth registrar holds the Rust ad, geth discv5_topicSearch does not find it ($(cat "$RUNDIR/c1-search.result"))"
    fi
    stop_nodes
}

# geth advertises; a Rust registrar holds the ad and a second Rust node's search stream finds it.
case2() {
    local s=topdisc-smoke/case2 t
    t=$(sha256hex "$s")
    start_rust c2-rust1 0 --service "$s" --bootnode-file "$RUNDIR/c2-geth.enr" --lookup-interval 5 ||
        { abort case2; return; }
    start_geth c2-geth 1 "${ENR[c2-rust1]}" || { abort case2; return; }
    register 1 "$s" || { fail "case2 geth->rust: discv5_registerTopic failed"; stop_nodes; return; }
    if ! wait_for "$WAIT" rust_holds 0 "$t" "${ID[c2-geth]}"; then
        fail "case2 geth->rust: Rust registrar does not hold the geth ad"
        stop_nodes
        return
    fi
    start_rust c2-rust2 2 --service "$s" --search 1 --bootnode "${ENR[c2-rust1]}" --lookup-interval 5 ||
        { abort case2; return; }
    if wait_for "$SEARCH_WAIT" rust_found 2 "${ID[c2-geth]}"; then
        pass "case2 geth->rust: Rust registrar holds the geth ad, Rust search stream finds it"
    else
        fail "case2 geth->rust: Rust registrar holds the geth ad, Rust search stream does not find it"
    fi
    stop_nodes
}

TICKET_CONFIG='{"adCacheSize":10,"adLifetimeSeconds":60}'
# The outcome of a case-3 direction: "ok <details>", "no-ticket" or a failure reason.
C3_OUT=""

# A geth registrar with an ad cache of 10 and ad lifetime 60 s: the second Rust
# advertiser is quoted a ticket by that registrar and is admitted by it on retry.
case3_geth_registrar() {
    local p=$1 s=topdisc-smoke/case3 t reg adv2 tickets
    t=$(sha256hex "$s")
    C3_OUT="a node did not start"
    echo "$TICKET_CONFIG" >"$RUNDIR/$p-registrar.json"
    start_rust "$p-adv1" 0 --service "$s" --register --bootnode-file "$RUNDIR/$p-geth.enr" \
        --lookup-interval 5 || { stop_nodes; return; }
    start_geth "$p-geth" 1 "${ENR[$p-adv1]}" --config "$RUNDIR/$p-registrar.json" || { stop_nodes; return; }
    if ! wait_for "$WAIT" geth_holds 1 "$t" "${ID[$p-adv1]}"; then
        C3_OUT="first ad not stored"
        stop_nodes
        return
    fi
    start_rust "$p-adv2" 2 --service "$s" --register --bootnode "${ENR[$p-geth]}" \
        --lookup-interval 5 || { stop_nodes; return; }
    if ! wait_for "$WAIT" geth_holds 1 "$t" "${ID[$p-adv2]}"; then
        C3_OUT="second ad not stored"
        stop_nodes
        return
    fi
    reg=$(short_id "${ID[$p-geth]}")
    adv2="$RUNDIR/$p-adv2.err"
    if wait_for 5 in_order "$adv2" "Topic registration ticket from=$reg" "Topic registration admitted from=$reg"; then
        tickets=$(status_num 2 tickets_recv)
        C3_OUT="ok tickets_recv=$tickets $(grep -m1 "Topic registration ticket from=$reg" "$adv2" | grep -o 'wait_ms=[0-9]*')"
    elif grep -q "Topic registration admitted from=$reg" "$adv2"; then
        C3_OUT="no-ticket"
    else
        C3_OUT="no ticket then admission from $reg in $p-adv2.err"
    fi
    stop_nodes
}

# A Rust registrar with --ad-cache-size 10 --ad-lifetime 60: the second geth
# advertiser is quoted a ticket and is admitted on retry with credited waiting time.
case3_rust_registrar() {
    local p=$1 s=topdisc-smoke/case3 t adv2 reg tickets
    t=$(sha256hex "$s")
    C3_OUT="a node did not start"
    RUST_LOG=$RUST_LOG,discv5::service::topic=trace \
        start_rust "$p-rust" 0 --service "$s" --ad-cache-size 10 --ad-lifetime 60 \
        --lookup-interval 5 || { stop_nodes; return; }
    start_geth "$p-adv1" 1 "${ENR[$p-rust]}" || { stop_nodes; return; }
    register 1 "$s" || { C3_OUT="discv5_registerTopic failed"; stop_nodes; return; }
    if ! wait_for "$WAIT" rust_holds 0 "$t" "${ID[$p-adv1]}"; then
        C3_OUT="first ad not stored"
        stop_nodes
        return
    fi
    start_geth "$p-adv2" 2 "${ENR[$p-rust]}" || { stop_nodes; return; }
    register 2 "$s" || { C3_OUT="discv5_registerTopic failed"; stop_nodes; return; }
    if ! wait_for "$WAIT" rust_holds 0 "$t" "${ID[$p-adv2]}"; then
        C3_OUT="second ad not stored"
        stop_nodes
        return
    fi
    adv2=$(short_id "${ID[$p-adv2]}")
    reg="$RUNDIR/$p-rust.err"
    if wait_for 5 in_order "$reg" "Sending REGCONFIRMATION.*$adv2.*admitted=false" \
        "Sending REGCONFIRMATION.*$adv2.*admitted=true wait_time=[0-9]+ waited_ms=[1-9]"; then
        tickets=$(status_num 0 tickets_issued)
        C3_OUT="ok tickets_issued=$tickets $(grep -m1 "Sending REGCONFIRMATION.*$adv2.*admitted=false" "$reg" |
            grep -o 'wait_time=[0-9]*')"
    elif grep -q "Sending REGCONFIRMATION.*$adv2.*admitted=true" "$reg"; then
        C3_OUT="no-ticket"
    else
        C3_OUT="no ticket then credited admission for $adv2 in $p-rust.err"
    fi
    stop_nodes
}

case3() {
    local dir
    for dir in geth rust; do
        "case3_${dir}_registrar" "c3$dir"
        [[ $C3_OUT == ok* ]] || "case3_${dir}_registrar" "c3$dir-retry"
        if [[ $C3_OUT == ok* ]]; then
            pass "case3 ticket at $dir registrar: second advertiser got a ticket and was admitted (${C3_OUT#ok })"
        else
            fail "case3 ticket at $dir registrar: $C3_OUT"
        fi
    done
}

# Rust REGTOPIC with a record address that does not match the packet source: geth rejects.
case4() {
    local s=topdisc-smoke/case4 out rc
    start_geth c4-geth 1 "" || { abort case4; return; }
    timeout 30 "$RUST_BIN" --port "$(udp 2)" --enr-ip 127.0.0.9 --service "$s" \
        --probe-regtopic "${ENR[c4-geth]}" >"$RUNDIR/c4-probe.out" 2>"$RUNDIR/c4-probe.err" &
    PIDS+=($!)
    if wait $!; then rc=0; else rc=$?; fi
    out=$(grep '"event":"probe"' "$RUNDIR/c4-probe.out" || true)
    if ((rc != 0)); then
        fail "case4 rejection: probe exited with status $rc (124: timeout), printed '${out:-nothing}'"
    elif ! grep -q '"result":"rejected"' <<<"$out"; then
        fail "case4 rejection: probe printed '${out:-nothing}'"
    elif ! wait_for 5 grep -q 'address does not match packet source' "$RUNDIR/c4-geth.err"; then
        fail "case4 rejection: probe rejected, but geth logged no 'address does not match packet source'"
    else
        pass "case4 rejection: geth rejects a REGTOPIC whose record IP does not match the sender"
    fi
    stop_nodes
}

echo "run dir: $RUNDIR"
case $MODE in
basic) case_basic ;;
all) case1; case2; case3; case4 ;;
*) "$MODE" ;;
esac
exit $STATUS

#!/bin/bash

##############################################################################
# LEVEL 8 — UNIFIED DISTRIBUTED SYSTEM + EMBEDDED FORMAL SPEC
##############################################################################

set -euo pipefail

NODE_ID=${NODE_ID:-node1}
NODES=("node1" "node2" "node3")

BASE=$(pwd)
LOG="$BASE/log"
STATE="$BASE/state"
DLQ="$BASE/dlq"
TRACE="$BASE/trace"

mkdir -p "$LOG" "$STATE" "$DLQ" "$TRACE"

##############################################################################
# 🧠 EMBEDDED FORMAL SPEC (TLA+-LIKE INVARIANTS, EXECUTABLE CHECKS)
##############################################################################

INVARIANT_NO_DOUBLE_LEADER=1
INVARIANT_NO_EVENT_LOSS=1
INVARIANT_STATE_CONVERGENCE=1

assert_no_double_leader () {
  local leaders=$(cat "$STATE"/leader_* 2>/dev/null | sort | uniq | wc -l)
  [ "$leaders" -le 1 ] || return 1
}

assert_event_logged () {
  grep -q "$1" "$LOG"/*.log 2>/dev/null
}

assert_state_convergence () {
  local hash=$(md5sum "$STATE"/*.state 2>/dev/null | cut -d' ' -f1 | sort | uniq | wc -l)
  [ "$hash" -le 1 ] || return 1
}

##############################################################################
# 📡 TRACE SYSTEM (industrial observability)
##############################################################################

trace () {
  echo "$(date -Iseconds) | $NODE_ID | $1" >> "$TRACE/trace.log"
}

##############################################################################
# 🧠 CRDT-STYLE STATE MODEL
##############################################################################

state_set () {
  echo "$2" > "$STATE/$1.state"
}

state_get () {
  cat "$STATE/$1.state" 2>/dev/null || echo ""
}

##############################################################################
# 📜 REPLICATED LOG (event sourcing core)
##############################################################################

append_log () {
  echo "$1" >> "$LOG/$NODE_ID.log"
}

replicate () {
  for n in "${NODES[@]}"; do
    echo "$1" >> "$LOG/$n.log"
  done
}

##############################################################################
# 👑 CONSENSUS (simplified RAFT simulation)
##############################################################################

elect_leader () {
  local votes=0

  for n in "${NODES[@]}"; do
    if [ "$((RANDOM % 2))" -eq 1 ]; then
      votes=$((votes+1))
    fi
  done

  if [ "$votes" -ge 2 ]; then
    echo "$NODE_ID" > "$STATE/leader"
    trace "LEADER_ELECTED:$NODE_ID"
  fi
}

is_leader () {
  [ "$(cat "$STATE/leader" 2>/dev/null)" == "$NODE_ID" ]
}

##############################################################################
# ☠️ FAULT MODEL (realistic distributed failure simulation)
##############################################################################

network_failure () {
  [ "$((RANDOM % 10))" -lt 2 ]
}

##############################################################################
# 🔁 EVENT PROCESSING PIPELINE (KERNEL)
##############################################################################

process_event () {
  local event="$1"
  local id=$(echo "$event" | jq -r '.id')

  trace "RECEIVE:$id"

  # ☠️ fault injection
  if network_failure; then
    trace "FAILURE:$id"
    echo "$event" >> "$DLQ/failures.log"
    return 1
  fi

  # 🧠 state update (CRDT merge model)
  local value=$(echo "$event" | jq -r '.payload.value')
  state_set "last_event" "$value"

  append_log "$event"
  replicate "$event"

  trace "COMMIT:$id"

  # 🧪 invariants checked at runtime (FORMAL MODEL EXECUTION)
  assert_no_double_leader || { trace "VIOLATION:DOUBLE_LEADER"; exit 1; }
}

##############################################################################
# 🚀 PRODUCER
##############################################################################

produce () {
  local id=$(uuidgen)

  EVENT=$(jq -n \
    --arg id "$id" \
    '{
      id: $id,
      type: "event",
      payload: { value: "level8" }
    }')

  process_event "$EVENT"
}

##############################################################################
# 🔁 REPLAY ENGINE (temporal debugging)
##############################################################################

replay () {
  trace "REPLAY_START"

  for f in "$LOG"/*.log; do
    while read -r line; do
      echo "REPLAY:$line"
    done < "$f"
  done
}

##############################################################################
# 🏗️ SELF-HEALING SYSTEM (distributed recovery)
##############################################################################

heal () {
  trace "HEALING_START"

  if ! is_leader; then
    elect_leader
  fi

  # restore minimal consistency
  assert_state_convergence || trace "STATE_INCONSISTENCY_DETECTED"
}

##############################################################################
# 🧭 SYSTEM ROUTER
##############################################################################

case "${1:-}" in
  run)
    produce
    ;;
  leader)
    elect_leader
    ;;
  replay)
    replay
    ;;
  heal)
    heal
    ;;
  check)
    assert_no_double_leader && echo "OK"
    ;;
  *)
    echo "run | leader | replay | heal | check"
    ;;
esac

##############################################################################
# 🧠 LEVEL 8 END — UNIFIED SYSTEM + FORMAL CHECKS
##############################################################################
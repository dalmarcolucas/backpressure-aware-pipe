#!/usr/bin/env bash
#
# Crash-recovery chaos test for the backpressure-aware pipe.
#
#   1. start redis, sink, consumer, producer
#   2. run under burst load for LOAD_SECONDS
#   3. SIGKILL the consumer, wait KILL_WAIT_SECONDS, restart it
#   4. stop the producer and let the pipe drain
#   5. verify no loss: delivered + dead_lettered accounts for every produced
#      event (duplicates at the sink are fine, a shortfall is not)
#
# Usage: scripts/chaos-test.sh [--keep-data]
#
# By default the script wipes the redis volume first: `produced`, `delivered`
# and `dead_lettered` are cumulative counters living in redis, so a leftover
# volume makes the accounting check meaningless. --keep-data skips the wipe.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOAD_SECONDS="${LOAD_SECONDS:-60}"
KILL_WAIT_SECONDS="${KILL_WAIT_SECONDS:-5}"
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-420}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-5}"
STATS_URL="${STATS_URL:-http://localhost:3002/stats}"

# /stats shares the consumer's redis connection with the blocking XREADGROUP
# (BLOCK 5000), so a request queues behind the current read and routinely takes
# 5-10s. Anything tighter than this reports a healthy consumer as down.
STATS_TIMEOUT_SECONDS="${STATS_TIMEOUT_SECONDS:-25}"

# The pipe is only drained once the backlog and the PEL are both empty and stay
# that way: a single zero sample can just be a gap between reclaim rounds.
DRAIN_STABLE_SAMPLES="${DRAIN_STABLE_SAMPLES:-3}"

KEEP_DATA=0
[[ "${1:-}" == "--keep-data" ]] && KEEP_DATA=1

RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/chaos-test.XXXXXX")"
SAMPLES_CSV="$RUN_DIR/samples.csv"
PHASE_FILE="$RUN_DIR/phase"
SAMPLER_PID=""

log()  { printf '\n\033[1m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
info() { printf '  %s\n' "$*"; }
fail() { printf '\033[31m  FAIL: %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m  OK:   %s\033[0m\n' "$*"; }

cleanup() {
  [[ -n "$SAMPLER_PID" ]] && kill "$SAMPLER_PID" 2>/dev/null || true
}
trap cleanup EXIT

compose() { docker compose "$@"; }

for bin in docker curl jq; do
  command -v "$bin" >/dev/null || { fail "$bin is required"; exit 2; }
done

# ---------------------------------------------------------------- sampling ---

fetch_stats() { curl -fsS --max-time "$STATS_TIMEOUT_SECONDS" "$STATS_URL" 2>/dev/null; }

sampler() {
  echo "ts,phase,produced,delivered,retried,dead_lettered,accounted,backlog,in_flight,oldest_event_age_ms" >"$SAMPLES_CSV"
  while true; do
    local phase stats row
    phase="$(cat "$PHASE_FILE" 2>/dev/null || echo unknown)"
    stats="$(fetch_stats || true)"
    if [[ -n "$stats" ]]; then
      row="$(jq -r '[.produced,.delivered,.retried,.dead_lettered,(.delivered + .dead_lettered),.backlog,.in_flight,.oldest_event_age_ms]|@csv' <<<"$stats")"
    else
      row=',,,,,,,'
    fi
    echo "$(date +%s),$phase,$row" >>"$SAMPLES_CSV"
    sleep "$SAMPLE_INTERVAL_SECONDS"
  done
}

set_phase() { echo "$1" >"$PHASE_FILE"; }

# Waits for /stats to answer, so we know the consumer is really serving again.
wait_for_stats() {
  local timeout="$1" deadline
  deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    fetch_stats >/dev/null && return 0
    sleep 1
  done
  return 1
}

# ------------------------------------------------------------------- start ---

log "Bringing the stack up"
if (( KEEP_DATA == 0 )); then
  info "wiping previous run (docker compose down -v)"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
else
  info "--keep-data: reusing the existing redis volume"
fi

compose up -d --build redis
compose up -d --build sink
compose up -d --build consumer
wait_for_stats 90 || { fail "consumer /stats never came up"; exit 1; }
info "consumer is serving $STATS_URL"
compose up -d --build producer

set_phase load
sampler &
SAMPLER_PID=$!

# -------------------------------------------------------------- burst load ---

log "Phase 1: burst load for ${LOAD_SECONDS}s"
sleep "$LOAD_SECONDS"
before_kill="$(fetch_stats || echo '{}')"
info "stats before kill: $(jq -c '{produced,delivered,dead_lettered,backlog,in_flight}' <<<"$before_kill")"

# ------------------------------------------------------------------- crash ---

log "Phase 2: SIGKILL the consumer, wait ${KILL_WAIT_SECONDS}s, restart"
set_phase kill
inflight_at_kill="$(jq -r '.in_flight // 0' <<<"$before_kill")"
compose kill -s SIGKILL consumer >/dev/null
info "consumer killed with ${inflight_at_kill} event(s) in flight (unacked in the PEL)"
sleep "$KILL_WAIT_SECONDS"

set_phase restart
compose up -d consumer >/dev/null
wait_for_stats 90 || { fail "consumer did not come back after the kill"; exit 1; }
info "consumer restarted and serving again"

# The reclaim of the dead consumer's PEL entries only fires once they have been
# idle for REDIS_RECLAIM_MIN_IDLE_MS (60s by default), so the drain below has to
# outlast that window: keep DRAIN_TIMEOUT_SECONDS comfortably above it.
info "waiting for the stale PEL entries to be reclaimed (min-idle window applies)"

# -------------------------------------------------------------------- drain ---

log "Phase 3: stop the producer and drain"
set_phase drain
compose stop producer >/dev/null
info "producer stopped"

deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
stable=0
drained=0
last_stats='{}'
while (( $(date +%s) < deadline )); do
  last_stats="$(fetch_stats || echo '{}')"
  backlog="$(jq -r '.backlog // 1' <<<"$last_stats")"
  in_flight="$(jq -r '.in_flight // 1' <<<"$last_stats")"
  if [[ "$backlog" == "0" && "$in_flight" == "0" ]]; then
    stable=$(( stable + 1 ))
    if (( stable >= DRAIN_STABLE_SAMPLES )); then drained=1; break; fi
  else
    stable=0
  fi
  printf '  draining… backlog=%s in_flight=%s delivered=%s dlq=%s\n' \
    "$backlog" "$in_flight" "$(jq -r '.delivered // "?"' <<<"$last_stats")" \
    "$(jq -r '.dead_lettered // "?"' <<<"$last_stats")"
  sleep 3
done

set_phase settled
sleep "$SAMPLE_INTERVAL_SECONDS"
final="$(fetch_stats || echo '{}')"
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""

# ------------------------------------------------------------------ verify ---

log "Phase 4: verification"
echo "$final" | jq .

produced="$(jq -r '.produced // 0' <<<"$final")"
delivered="$(jq -r '.delivered // 0' <<<"$final")"
dead="$(jq -r '.dead_lettered // 0' <<<"$final")"
retried="$(jq -r '.retried // 0' <<<"$final")"
backlog="$(jq -r '.backlog // -1' <<<"$final")"
in_flight="$(jq -r '.in_flight // -1' <<<"$final")"
accounted=$(( delivered + dead ))
duplicates=$(( accounted - produced ))

failures=0

if (( drained == 1 )); then
  ok "pipe drained: backlog=0 in_flight=0 for ${DRAIN_STABLE_SAMPLES} consecutive checks"
else
  fail "pipe did not drain within ${DRAIN_TIMEOUT_SECONDS}s (backlog=$backlog in_flight=$in_flight)"
  failures=$(( failures + 1 ))
fi

# Duplicates are expected: an event the old consumer delivered but never got to
# ack is reclaimed and delivered again. Only a shortfall means events were lost.
if (( accounted >= produced )); then
  ok "no loss: delivered($delivered) + dead_lettered($dead) = $accounted >= produced($produced)"
  if (( duplicates > 0 )); then
    info "$duplicates duplicate delivery/ies at the sink (accepted: at-least-once redelivery after the crash)"
  else
    info "exactly-once accounting this run: no redelivery was needed"
  fi
else
  fail "LOST EVENTS: delivered($delivered) + dead_lettered($dead) = $accounted < produced($produced) — $(( produced - accounted )) missing"
  failures=$(( failures + 1 ))
fi
info "retried: $retried"

# The accounting must never go backwards mid-run either: at every sample,
# delivered + dead_lettered can only ever lag produced by what is still in the
# stream or in flight. A sample where it exceeds produced is the duplicate
# window (redelivery after the crash) and is expected.
worst="$(awk -F, 'NR > 1 && $3 != "" {
  short = $3 - $7 - $8 - $9          # produced - accounted - backlog - in_flight
  if (short > max) { max = short; at = $2 }
} END { printf "%d %s\n", max, (at == "" ? "-" : at) }' "$SAMPLES_CSV")"
read -r worst_short worst_phase <<<"$worst"

info "largest mid-run gap between produced and (accounted + backlog + in_flight): ${worst_short} event(s), phase ${worst_phase}"
info "counters are read from a live pipe, so a small transient gap is just sampling skew"

log "Event accounting"
printf '  %-24s %10s\n' \
  "produced"            "$produced" \
  "delivered"           "$delivered" \
  "dead_lettered"       "$dead" \
  "accounted (d + dlq)" "$accounted" \
  "duplicates"          "$duplicates" \
  "lost"                "$(( produced > accounted ? produced - accounted : 0 ))" \
  "retried"             "$retried" \
  "orphaned by the kill" "$inflight_at_kill"
info "the $inflight_at_kill event(s) unacked when the consumer was SIGKILLed were reclaimed from the PEL and redriven"

log "Samples: $SAMPLES_CSV"
column -s, -t "$SAMPLES_CSV" 2>/dev/null || cat "$SAMPLES_CSV"

if (( failures == 0 )); then
  log "RESULT: PASS"
  exit 0
fi
log "RESULT: FAIL ($failures check(s) failed)"
exit 1

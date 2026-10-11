#!/usr/bin/env bash
# Injects NUM_KILLS SIGKILLs into the coordinator process while a stream of
# requests is actively flowing through claim -> execute -> webhook ->
# commit, restarting it (new PID, new epoch, recovery runs on startup)
# after each kill, then checks the side-effect log for any (request_id,
# job_id) key that appears more than once. Every kill is SIGKILL against a
# PID this script itself started and holds, never a PID collected from ps
# or netstat.
set -uo pipefail
cd "$(dirname "$0")/.."
BUILD=~/build/datagen-control-plane/build
CONNINFO="dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"

PORT="${1:-50095}"
CSV="${2:-/tmp/crash_inject_requests.csv}"
SIDE_LOG="${3:-/tmp/crash_inject_side_effects.log}"
ORDERING_FLAG="${4:-}"       # "" (fixed, the real mode) or "--buggy-ordering"
NUM_KILLS="${5:-20}"
COORD_LOG="${6:-/tmp/crash_inject_coord.log}"

rm -f "$SIDE_LOG" "$COORD_LOG"
"$BUILD/control_plane_cli" reset --conninfo="$CONNINFO"

start_coordinator() {
  "$BUILD/coordinator" --bind=127.0.0.1:$PORT --side-effect-log="$SIDE_LOG" \
    --dispatch-threads=4 $ORDERING_FLAG >> "$COORD_LOG" 2>&1 &
  echo $!
}

COORD_PID=$(start_coordinator)
echo "initial coordinator pid=$COORD_PID"
sleep 1

"$BUILD/control_plane_cli" bulk --server=127.0.0.1:$PORT --file="$CSV" --threads=4 \
  > /tmp/crash_inject_submit.log 2>&1 &
SUBMIT_PID=$!

KILLS_DONE=0
while [ $KILLS_DONE -lt $NUM_KILLS ]; do
  sleep 0.4
  if kill -9 "$COORD_PID" 2>/dev/null; then
    wait "$COORD_PID" 2>/dev/null
    KILLS_DONE=$((KILLS_DONE + 1))
    COORD_PID=$(start_coordinator)
    sleep 0.2
  fi
done
echo "kills injected: $KILLS_DONE, final coordinator pid=$COORD_PID"

wait "$SUBMIT_PID" 2>/dev/null
cat /tmp/crash_inject_submit.log

echo "draining remaining backlog..."
"$BUILD/control_plane_cli" drain --conninfo="$CONNINFO" --timeout_s=900

kill "$COORD_PID" 2>/dev/null
wait "$COORD_PID" 2>/dev/null

echo "--- duplicate check ($SIDE_LOG) ---"
sort "$SIDE_LOG" | uniq -c | awk '{ if ($1 > 1) { dup++; print } } END { print "duplicate_keys=" (dup+0) }'
echo "total_side_effect_lines=$(wc -l < "$SIDE_LOG")"
echo "unique_keys=$(sort -u "$SIDE_LOG" | wc -l)"

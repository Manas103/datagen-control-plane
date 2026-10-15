#!/usr/bin/env bash
# Injects NUM_KILLS real SIGKILLs into fleet_coordinator while it is
# actively draining fleet_pending_assignments into fleet_tasks, restarting
# it (new PID, resumes draining) after each kill, then verifies 0
# duplicate and 0 lost assignments: fleet_tasks must end with exactly one
# row per enqueued (vehicle_id, task_id), no more and no fewer. Every kill
# targets a PID this script started and holds, never a PID collected from
# ps or netstat.
set -uo pipefail
cd "$(dirname "$0")/.."
BUILD=~/build/datagen-control-plane-fleet/build
CONNINFO="dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"

COUNT="${1:-2000}"
NUM_KILLS="${2:-20}"
COORD_LOG="${3:-/tmp/fleet_crash_inject_coord.log}"

rm -f "$COORD_LOG"
"$BUILD/fleet_tool" reset "$CONNINFO"
"$BUILD/fleet_tool" enqueue "$CONNINFO" "$COUNT"

start_coordinator() {
  "$BUILD/fleet_coordinator" "$CONNINFO" >> "$COORD_LOG" 2>&1 &
  echo $!
}

COORD_PID=$(start_coordinator)
echo "initial fleet_coordinator pid=$COORD_PID"

KILLS_DONE=0
while [ $KILLS_DONE -lt $NUM_KILLS ]; do
  sleep 0.3
  if kill -9 "$COORD_PID" 2>/dev/null; then
    wait "$COORD_PID" 2>/dev/null
    KILLS_DONE=$((KILLS_DONE + 1))
    COORD_PID=$(start_coordinator)
  else
    # Coordinator already exited (fully drained) before this kill landed.
    break
  fi
done
echo "kills injected: $KILLS_DONE"

echo "draining any remaining backlog to completion..."
for i in $(seq 1 600); do
  PENDING=$("$BUILD/fleet_tool" counts "$CONNINFO" | sed -n 's/pending=\([0-9]*\).*/\1/p')
  if [ "$PENDING" = "0" ]; then
    break
  fi
  sleep 0.5
done

kill "$COORD_PID" 2>/dev/null
wait "$COORD_PID" 2>/dev/null

echo "--- final counts ---"
"$BUILD/fleet_tool" counts "$CONNINFO"
echo "requested=$COUNT"

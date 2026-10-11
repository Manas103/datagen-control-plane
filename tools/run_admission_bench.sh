#!/usr/bin/env bash
# Admission-latency benchmark: starts the coordinator with its normal
# dispatch pool running, floods it with CSV requests faster than dispatch
# can drain them (so a large undispatched backlog accumulates), and
# records per-call Submit() latency throughout. Stops the coordinator by
# the PID this script captured.
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD=~/build/datagen-control-plane/build
PORT="${1:-50070}"
CSV="${2:-/tmp/backlog_200k.csv}"
OUT_LAT="${3:-/tmp/admission_latencies.txt}"
SIDE_LOG="${4:-/tmp/bench_side_effects.log}"
CONNINFO="dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"

rm -f "$SIDE_LOG"
"$BUILD/control_plane_cli" reset --conninfo="$CONNINFO"

"$BUILD/coordinator" --bind=127.0.0.1:$PORT --side-effect-log="$SIDE_LOG" \
  --dispatch-threads=6 > /tmp/admission_bench_coord.log 2>&1 &
COORD_PID=$!
echo "coordinator pid=$COORD_PID"
sleep 1

"$BUILD/control_plane_cli" bulk --server=127.0.0.1:$PORT --file="$CSV" \
  --threads=6 --out="$OUT_LAT"

BACKLOG=$("$BUILD/control_plane_cli" pending --conninfo="$CONNINFO")
echo "pending_or_ready_job_backlog_at_end_of_submission=$BACKLOG"

kill "$COORD_PID"
wait "$COORD_PID" 2>/dev/null || true
echo "--- coordinator log tail ---"
tail -5 /tmp/admission_bench_coord.log

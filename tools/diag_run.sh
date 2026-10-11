#!/usr/bin/env bash
# One-off diagnostic: starts the coordinator, submits a small CSV, polls
# pending-job count over time, then stops the coordinator by the PID this
# script itself captured. Not part of the measured harness.
set -u
cd "$(dirname "$0")/.."
BUILD=~/build/datagen-control-plane/build
PORT="${1:-50063}"
CSV="${2:-/tmp/smoke_200.csv}"

"$BUILD/control_plane_cli" reset --conninfo="dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"

"$BUILD/coordinator" --bind=127.0.0.1:$PORT --side-effect-log=/tmp/diag_side_effects.log > /tmp/diag_coord.log 2>&1 &
COORD_PID=$!
echo "coordinator pid=$COORD_PID"
sleep 1

"$BUILD/control_plane_cli" bulk --server=127.0.0.1:$PORT --file="$CSV" --threads=4

for i in $(seq 1 10); do
  sleep 2
  PENDING=$("$BUILD/control_plane_cli" pending)
  echo "t=$((i*2))s pending=$PENDING"
done

kill "$COORD_PID"
wait "$COORD_PID" 2>/dev/null
echo "--- coordinator log ---"
cat /tmp/diag_coord.log

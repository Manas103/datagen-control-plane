#!/usr/bin/env bash
set -uo pipefail
BUILD=~/build/datagen-control-plane/build
CONN="dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"
"$BUILD/coordinator" --bind=127.0.0.1:50083 --side-effect-log=/tmp/replay_resume2.log --dispatch-threads=6 > /tmp/replay_resume2_coord.log 2>&1 &
PID=$!
echo "pid=$PID"
sleep 1
"$BUILD/control_plane_cli" drain --conninfo="$CONN" --timeout_s=900
"$BUILD/control_plane_cli" digest --conninfo="$CONN"
kill "$PID"
wait "$PID" 2>/dev/null

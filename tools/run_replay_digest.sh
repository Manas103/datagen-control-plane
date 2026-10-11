#!/usr/bin/env bash
# The 5%-duplicated, fully-reordered replay test: submits the canonical
# 50,000-request stream to a clean database, drains it, digests the final
# state; resets, submits the noisy (duplicated + shuffled) stream to a
# second clean database, drains it, digests; the two digests must match.
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD=~/build/datagen-control-plane/build
CONNINFO="dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"
CANONICAL_CSV="${1:-/tmp/canonical_50k.csv}"
NOISY_CSV="${2:-/tmp/noisy_50k.csv}"
PORT_A="${3:-50080}"
PORT_B="${4:-50081}"
DRAIN_TIMEOUT_S="${5:-1800}"

run_one() {
  local label="$1" csv="$2" port="$3" out_digest_var="$4"
  "$BUILD/control_plane_cli" reset --conninfo="$CONNINFO"
  "$BUILD/coordinator" --bind=127.0.0.1:$port \
    --side-effect-log=/tmp/replay_side_effects_${label}.log \
    --dispatch-threads=6 > /tmp/replay_coord_${label}.log 2>&1 &
  local pid=$!
  echo "[$label] coordinator pid=$pid"
  sleep 1

  "$BUILD/control_plane_cli" bulk --server=127.0.0.1:$port --file="$csv" --threads=6

  echo "[$label] draining..."
  "$BUILD/control_plane_cli" drain --conninfo="$CONNINFO" --timeout_s=$DRAIN_TIMEOUT_S

  local digest
  digest=$("$BUILD/control_plane_cli" digest --conninfo="$CONNINFO")
  echo "[$label] digest=$digest"

  kill "$pid"
  wait "$pid" 2>/dev/null || true
  eval "$out_digest_var=$digest"
}

run_one canonical "$CANONICAL_CSV" "$PORT_A" DIGEST_A
run_one noisy "$NOISY_CSV" "$PORT_B" DIGEST_B

echo "canonical_digest=$DIGEST_A"
echo "noisy_digest=$DIGEST_B"
if [ "$DIGEST_A" = "$DIGEST_B" ]; then
  echo "RESULT: DIGESTS MATCH"
else
  echo "RESULT: DIGESTS DIFFER"
fi

#!/usr/bin/env bash
# One benchmark run: fresh state, start Herald, drive load with k6, wait for
# every notification to finish, then record ingest and end-to-end results.
#
# bench/run.sh <name> <rate> <duration> [extra env for herald, e.g. HERALD_BATCH_MAX=1]
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=$1 RATE=$2 DURATION=$3; shift 3
OUT=bench/results/$NAME
mkdir -p "$OUT"
PG=${HERALD_POSTGRES_URL:-postgres://herald:herald@localhost:5432/herald?sslmode=disable}
K6=${K6:-k6}

scripts/dev-down.sh >/dev/null 2>&1 || true
psql -q "$PG" -c "TRUNCATE notifications, collapse_state" 2>/dev/null || true
redis-cli FLUSHALL >/dev/null
env "$@" scripts/dev-up.sh 1 1 >/dev/null
sleep 3 # let the worker join its consumer groups

"$K6" run --quiet -e RATE="$RATE" -e DURATION="$DURATION" -e OUT="$OUT/k6.json" bench/k6/ingest.js > "$OUT/k6.log" 2>&1

# Wait until nothing is queued, delivering or retrying (max 5 minutes).
for i in $(seq 1 300); do
  left=$(psql -qtA "$PG" -c "SELECT count(*) FROM notifications WHERE status IN ('queued','delivering','retrying')")
  [ "$left" = "0" ] && break
  sleep 1
done
bin/herald report > "$OUT/report.json"
curl -s localhost:8090/v1/stats > "$OUT/provider.json"
curl -s localhost:8080/metrics | grep -E '^herald_insert_batch_size_(sum|count)' > "$OUT/batch.txt" || true
{
  echo "rate=$RATE duration=$DURATION env=$*"
  echo "host: $(nproc) vCPU, $(free -g | awk '/Mem:/ {print $2}') GB RAM, everything on one machine"
  echo "drain_seconds_after_load=$i remaining=$left"
} > "$OUT/meta.txt"
scripts/dev-down.sh >/dev/null 2>&1 || true
echo "== $NAME"
cat "$OUT/k6.json"
python3 -c "import json;r=json.load(open('$OUT/report.json'));print('status', r['status_counts'], 'e2e', {k: round(v) if v else v for k, v in r['e2e_latency_ms'].items()})"

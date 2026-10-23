#!/usr/bin/env bash
# Fault injection under load. Each scenario runs 600 req/s for 60 s with two
# workers, breaks something at 15 s, repairs it at 30 s, then checks that every
# accepted notification reached a final state and nothing was sent twice.
#
# bench/chaos.sh kafka|worker|provider
set -euo pipefail
cd "$(dirname "$0")/.."
SCENARIO=$1
OUT=bench/results/chaos-$SCENARIO
mkdir -p "$OUT"
PG=${HERALD_POSTGRES_URL:-postgres://herald:herald@localhost:5432/herald?sslmode=disable}
KAFKA_BIN=${KAFKA_BIN:-/opt/kafka/bin}
KAFKA_CONFIG=${KAFKA_CONFIG:-/opt/kafka/herald.properties}
LOGS=${HERALD_LOG_DIR:-/tmp/herald-logs}

scripts/dev-down.sh >/dev/null 2>&1 || true
psql -q "$PG" -c "TRUNCATE notifications, collapse_state"
redis-cli FLUSHALL >/dev/null
scripts/dev-up.sh 1 2 >/dev/null
sleep 4

k6 run --quiet -e RATE=600 -e DURATION=60s -e OUT="$OUT/k6.json" bench/k6/ingest.js > "$OUT/k6.log" 2>&1 &
K6=$!
sleep 15
case $SCENARIO in
  kafka)
    echo "t=15s stopping Kafka" | tee "$OUT/events.txt"
    "$KAFKA_BIN/kafka-server-stop.sh" >/dev/null 2>&1
    sleep 15
    echo "t=30s starting Kafka" | tee -a "$OUT/events.txt"
    KAFKA_HEAP_OPTS="-Xmx768m -Xms768m" nohup "$KAFKA_BIN/kafka-server-start.sh" "$KAFKA_CONFIG" > /tmp/kafka-chaos.log 2>&1 &
    ;;
  worker)
    victim=$(pgrep -f '^bin/herald worker' | head -1)
    echo "t=15s kill -9 worker pid $victim (no graceful shutdown, offsets uncommitted)" | tee "$OUT/events.txt"
    kill -9 "$victim"
    sleep 15
    echo "t=30s starting a replacement worker" | tee -a "$OUT/events.txt"
    HERALD_NODE=worker-replacement nohup bin/herald worker > "$LOGS/worker-replacement.log" 2>&1 &
    ;;
  provider)
    echo "t=15s push provider down" | tee "$OUT/events.txt"
    curl -s -X POST localhost:8090/v1/chaos -d '{"channel":"push","down":true}' >/dev/null
    sleep 15
    echo "t=30s push provider back" | tee -a "$OUT/events.txt"
    curl -s -X POST localhost:8090/v1/chaos -d '{"channel":"push","down":false}' >/dev/null
    ;;
esac
wait $K6 || true

for i in $(seq 1 300); do
  left=$(psql -qtA "$PG" -c "SELECT count(*) FROM notifications WHERE status IN ('queued','delivering','retrying')")
  [ "$left" = "0" ] && break
  sleep 1
done
bin/herald report > "$OUT/report.json"
curl -s localhost:8090/v1/stats > "$OUT/provider.json"
redis-cli HGETALL herald:stats | paste - - > "$OUT/counters.txt"
python3 - "$OUT" "$left" "$i" <<'PY'
import json, sys
out, left, waited = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
k = json.load(open(f"{out}/k6.json")); r = json.load(open(f"{out}/report.json")); p = json.load(open(f"{out}/provider.json"))
c = dict(l.split("\t") for l in open(f"{out}/counters.txt").read().strip().splitlines())
accepted, rows = k["accepted"], sum(r["status_counts"].values())
final = sum(v for s, v in r["status_counts"].items() if s in ("delivered", "failed", "superseded"))
summary = {
    "accepted_by_api": accepted, "rows_in_postgres": rows, "in_final_state": final, "still_pending": left,
    "drain_seconds_after_load": waited, "api_errors": k["other_status"], "api_p99_ms": round(k["latency_ms"]["p99"]),
    "status_counts": r["status_counts"], "delivered_by_channel": r["delivered_by_channel"],
    "provider_sent": sum(v["sent"] for v in p.values()),
    "provider_duplicates_suppressed": sum(v["duplicates_suppressed"] for v in p.values()),
    "provider_stale_fenced": sum(v["stale_fenced"] for v in p.values()),
    "republished_by_sweeper": int(c.get("republished", 0)), "stale_copies_dropped": int(c.get("stale_dropped", 0)),
    "fallback_deliveries": int(c.get("delivered_fallback", 0)), "breaker_skips": int(c.get("breaker_skip", 0)),
    "retries": int(c.get("retried", 0)),
}
summary["lost"] = accepted - final - left
json.dump(summary, open(f"{out}/summary.json", "w"), indent=2)
print(json.dumps(summary, indent=2))
PY
scripts/dev-down.sh >/dev/null 2>&1 || true

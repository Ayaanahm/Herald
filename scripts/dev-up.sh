#!/usr/bin/env bash
# Run Herald from source against local PostgreSQL, Redis and Kafka.
# Usage: scripts/dev-up.sh [api replicas] [workers]   (defaults: 1 1)
set -euo pipefail
cd "$(dirname "$0")/.."
APIS=${1:-1}
WORKERS=${2:-1}
LOGS=${HERALD_LOG_DIR:-/tmp/herald-logs}
mkdir -p "$LOGS" bin
go build -o bin/herald ./cmd/herald
bin/herald migrate
nohup bin/herald provider > "$LOGS/provider.log" 2>&1 &
for i in $(seq 1 "$WORKERS"); do
  HERALD_NODE=worker-$i HERALD_METRICS_ADDR=:$((9100 + i)) nohup bin/herald worker > "$LOGS/worker-$i.log" 2>&1 &
done
for i in $(seq 1 "$APIS"); do
  HERALD_NODE=api-$i HERALD_HTTP_ADDR=:$((8079 + i)) nohup bin/herald api > "$LOGS/api-$i.log" 2>&1 &
done
for i in $(seq 1 50); do
  curl -sf localhost:8080/readyz >/dev/null && break
  sleep 0.2
done
echo "Herald is up: $APIS API replica(s) from :8080, $WORKERS worker(s). Logs in $LOGS"
echo "Dashboard: http://localhost:8080/?key=admin-key"

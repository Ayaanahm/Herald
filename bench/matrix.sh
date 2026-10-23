#!/usr/bin/env bash
# The full benchmark set behind bench/README.md. Takes about 20 minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
D=${DURATION:-60s}

# 1. Fixed load, each optimization switched off in turn.
bench/run.sh ablation-full          1200 "$D"
bench/run.sh ablation-no-coalescing 1200 "$D" HERALD_COALESCE_MAX=1
bench/run.sh ablation-no-batching   1200 "$D" HERALD_BATCH_MAX=1
bench/run.sh ablation-neither       1200 "$D" HERALD_COALESCE_MAX=1 HERALD_BATCH_MAX=1

# 2. Step up the request rate to find where latency breaks down.
for r in 1400 1600 1800 2000; do
  bench/run.sh "rate-$r" "$r" "$D"
done

# 3. Insert batching again, near saturation, where it starts to matter.
bench/run.sh rate-1800-no-batching 1800 "$D" HERALD_BATCH_MAX=1

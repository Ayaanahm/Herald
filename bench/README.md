# Benchmarks

Every number in the main README comes from a run in [results/](results). Each folder holds the raw k6 summary (`k6.json`), the end-to-end report computed in PostgreSQL (`report.json`), the mock provider's counters (`provider.json`) and the run's settings (`meta.txt`).

## Setup

- **Machine:** one VM, 2 vCPU (Intel Xeon @ 2.80 GHz), 7.8 GB RAM, Ubuntu 24.04.
- **On that one machine:** 1 API process, 1 worker (2 in the chaos runs), the mock provider, PostgreSQL 16, Redis 7, Kafka 4.1 (single broker, KRaft, 12 partitions, 768 MB heap) and k6. Default PostgreSQL settings, `synchronous_commit` on.
- **Load:** [k6/ingest.js](k6/ingest.js), `constant-arrival-rate` for 60 s. Random recipients from 100,000 users, a mix of channel lists, 10% of notifications with a collapse key, ~200-byte payloads.
- **Mock providers:** push 7–22 ms, email 20–60 ms, SMS 30–90 ms per call, 2% random failures on each channel.
- **Measured:** API latency is k6's `http_req_duration`. Delivery latency is `delivered_at - created_at` in PostgreSQL, over every delivered notification. Each run waits until nothing is queued, delivering or retrying before reporting.

"Not sent" is k6's `dropped_iterations`: requests k6 was scheduled to start but couldn't, because all of its virtual users were waiting on slow responses. It's how an open-model test shows a server falling behind.

## Results

| Run | Target req/s | Achieved req/s | Not sent | Errors | API p50 | API p95 | API p99 | Delivery p50 | Delivery p99 | Delivered |
|---|---|---|---|---|---|---|---|---|---|---|
| ablation-full | 1,200 | 1,199 | 0.0% | 0.0% | 13 ms | 50 ms | 81 ms | 0.19 s | 1.22 s | 71,999 / 71,999 |
| ablation-neither | 1,200 | 1,096 | 8.5% | 0.0% | 183 ms | 470 ms | 672 ms | 10.17 s | 25.16 s | 65,891 / 65,892 |
| ablation-no-batching | 1,200 | 1,200 | 0.0% | 0.0% | 16 ms | 72 ms | 115 ms | 0.21 s | 1.24 s | 71,992 / 71,992 |
| ablation-no-coalescing | 1,200 | 1,176 | 2.0% | 0.0% | 56 ms | 180 ms | 280 ms | 7.58 s | 19.49 s | 70,580 / 70,581 |
| chaos-kafka | 600 | 600 | 0.0% | 0.0% | 6 ms | 54 ms | 102 ms | 4.15 s | 31.20 s | 36,000 / 36,000 |
| chaos-provider | 600 | 600 | 0.0% | 0.0% | 6 ms | 17 ms | 27 ms | 0.14 s | 1.18 s | 36,001 / 36,001 |
| chaos-worker | 600 | 600 | 0.0% | 0.0% | 6 ms | 23 ms | 41 ms | 0.14 s | 9.52 s | 36,001 / 36,001 |
| rate-1400 | 1,400 | 1,400 | 0.0% | 0.0% | 17 ms | 62 ms | 100 ms | 0.22 s | 1.25 s | 84,000 / 84,000 |
| rate-1600 | 1,600 | 1,598 | 0.1% | 0.0% | 23 ms | 86 ms | 141 ms | 0.26 s | 1.29 s | 95,935 / 95,935 |
| rate-1800 | 1,800 | 1,764 | 1.9% | 0.0% | 50 ms | 168 ms | 243 ms | 1.08 s | 2.82 s | 105,901 / 105,901 |
| rate-1800-no-batching | 1,800 | 1,612 | 10.3% | 0.0% | 169 ms | 406 ms | 575 ms | 0.97 s | 2.11 s | 96,845 / 96,845 |
| rate-2000 | 2,000 | 1,889 | 5.2% | 0.0% | 72 ms | 237 ms | 343 ms | 1.32 s | 3.75 s | 113,763 / 113,764 |

"Delivered" counts against every notification accepted; the remainder were superseded by a newer notification with the same collapse key, which is the intended outcome.

## Chaos runs

[chaos.sh](chaos.sh) runs 600 req/s for 60 s with two workers, injects a fault at 15 s and repairs it at 30 s, then waits for every notification to finish. "Lost" is accepted notifications that never reached a final state.

| Fault (at 15 s, repaired at 30 s) | Accepted | Lost | API errors | API p99 | Delivery p99 | What happened |
|---|---|---|---|---|---|---|
| Kafka broker stopped | 36,000 | **0** | 0 | 102 ms | 31.2 s | API kept answering `202`; sweeper republished 39 rows; 1,636 duplicate copies dropped; 0 repeat sends |
| A worker `kill -9`'d, replaced 15 s later | 36,001 | **0** | 0 | 41 ms | 9.5 s | Survivor took its partitions after the 10 s session timeout; 1,292 redelivered copies dropped; 4 in-flight sends repeated and absorbed by the provider's idempotency key |
| Push provider down | 36,001 | **0** | 0 | 27 ms | 1.2 s | Circuit opened; 7,303 notifications delivered by email or SMS instead; 0 repeat sends |

## Reproduce

```sh
# PostgreSQL, Redis and Kafka running locally; k6 on PATH
bench/matrix.sh          # ablations and rate ladder, ~15 min
bench/chaos.sh kafka     # also: worker, provider
python3 bench/summarize.py
```

Rate limiter cost per decision (sliding log vs sliding counter):

```sh
go test -run '^$' -bench BenchmarkLimiter -benchtime 5s ./internal/ratelimit/
# sliding-log      18,912 ns/op
# sliding-counter  17,304 ns/op   (16 goroutines per core, pipelined, 1M/s limit)
```

Per decision they cost about the same. The difference is memory: the sliding log stores one sorted-set entry per request in the window, the counter stores two integers.

"""Print a table of every run in bench/results (used for bench/README.md)."""
import json
import os
import sys

root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
rows = []
for name in sorted(os.listdir(root)):
    try:
        k = json.load(open(os.path.join(root, name, "k6.json")))
        r = json.load(open(os.path.join(root, name, "report.json")))
    except (OSError, ValueError):
        continue
    lat, e2e = k["latency_ms"], r["e2e_latency_ms"]
    sent = k["requests"] + k["dropped_iterations"]
    rows.append((name, k["target_rate"], k["achieved_rps"], 100 * k["dropped_iterations"] / sent,
                 100 * k["failed_rate"], lat["p50"], lat["p95"], lat["p99"], e2e["p50"], e2e["p99"],
                 r["delivered"], sum(r["status_counts"].values())))
print("| Run | Target req/s | Achieved req/s | Not sent | Errors | API p50 | API p95 | API p99 | Delivery p50 | Delivery p99 | Delivered |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for n, t, a, d, f, p50, p95, p99, e50, e99, dl, tot in rows:
    print(f"| {n} | {t:,} | {a:,.0f} | {d:.1f}% | {f:.1f}% | {p50:.0f} ms | {p95:.0f} ms | {p99:.0f} ms | "
          f"{e50/1000:.2f} s | {e99/1000:.2f} s | {dl:,} / {tot:,} |")

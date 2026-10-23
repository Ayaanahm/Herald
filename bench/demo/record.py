"""Record the demo video: the live dashboard while real faults are injected.

Requires Herald running (scripts/dev-up.sh), k6 on PATH and Playwright:
    pip install playwright && playwright install chromium
    python3 bench/demo/record.py --out demo.webm [--kafka-bin /opt/kafka/bin]

Nothing in the video is staged: k6 drives real traffic, the provider outage
is a call to the mock provider's fault-injection endpoint, and the Kafka
outage (optional) stops the actual broker.
"""
import argparse
import json
import os
import shutil
import subprocess
import time
import urllib.request

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8080"
ADMIN = "admin-key"


def post(path, body, key=ADMIN):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), method="POST",
                                 headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.read()


def k6(rate, duration, key="load-key"):
    return subprocess.Popen(
        ["k6", "run", "--quiet", "-e", f"RATE={rate}", "-e", f"DURATION={duration}", "-e", f"KEY={key}",
         os.path.join(os.path.dirname(__file__), "..", "k6", "ingest.js")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="demo.webm")
    ap.add_argument("--rate", type=int, default=800)
    ap.add_argument("--kafka-bin", default="", help="Kafka bin dir; if set, the demo stops and restarts the broker")
    ap.add_argument("--kafka-config", default="/opt/kafka/herald.properties")
    args = ap.parse_args()

    post("/v1/admin/reset-stats", {})
    viddir = "/tmp/herald-demo-video"
    shutil.rmtree(viddir, ignore_errors=True)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1600, "height": 900}, device_scale_factor=1,
                                  record_video_dir=viddir, record_video_size={"width": 1600, "height": 900})
        page = ctx.new_page()
        page.goto(f"{BASE}/?key={ADMIN}")
        page.wait_for_selector(".conn.live")
        cap = lambda t: page.evaluate("t => window.setCaption(t)", t)
        wait = page.wait_for_timeout

        load = k6(args.rate, "200s")
        cap("Herald: a notification platform in Go, Kafka, Redis and PostgreSQL")
        wait(5000)
        cap(f"k6 sends {args.rate} requests a second. Each is committed to PostgreSQL before the API replies.")
        wait(9000)
        cap("Kafka spreads the work over the workers. Every channel is a mocked provider with real latency.")
        wait(7000)

        cap("Now the push provider goes down.")
        page.click('#chaos button[data-ch="push"]')
        wait(4000)
        cap("After 5 failures its circuit opens. New notifications skip push and go to email.")
        wait(9000)
        cap("Push comes back. The breaker sends one test request, then closes.")
        page.click('#chaos button[data-ch="push"]')
        wait(10000)

        cap("A second tenant sends 150 requests a second against a 50/s limit.")
        burst = k6(150, "10s", key="demo-key")
        wait(4500)
        cap("A Redis Lua sliding window answers 429 for the excess, the same on every API replica.")
        wait(7000)
        burst.wait()

        if args.kafka_bin:
            cap("Now Kafka itself goes down.")
            subprocess.run([os.path.join(args.kafka_bin, "kafka-server-stop.sh")], check=False)
            wait(4000)
            cap("The API keeps accepting: rows are safe in PostgreSQL, which is the outbox.")
            wait(9000)
            cap("Kafka restarts. The outbox sweeper republishes what never reached it.")
            env = dict(os.environ, KAFKA_HEAP_OPTS="-Xmx768m -Xms768m")
            subprocess.Popen([os.path.join(args.kafka_bin, "kafka-server-start.sh"), args.kafka_config],
                             stdout=open("/tmp/kafka-demo.log", "w"), stderr=subprocess.STDOUT, env=env)
            wait(16000)
            cap("The backlog drains. Duplicate copies are dropped by step and fencing checks.")
            wait(9000)

        load.terminate()
        load.wait()
        cap("Load stops. Every notification ends delivered or in the dead-letter queue.")
        wait(8000)
        cap("Code, tests and benchmarks: github.com/Ayaanahm/herald")
        wait(5000)
        video = page.video.path()
        ctx.close()
        browser.close()
    shutil.move(video, args.out)
    print(args.out)


if __name__ == "__main__":
    main()

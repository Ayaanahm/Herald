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
import socket
import subprocess
import time
import urllib.request

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8080"
PROVIDER = "http://localhost:8090"
PG = os.environ.get("HERALD_POSTGRES_URL", "postgres://herald:herald@localhost:5432/herald?sslmode=disable")
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


def port_open(host, port):
    try:
        socket.create_connection((host, port), timeout=0.5).close()
        return True
    except OSError:
        return False


def pending():
    out = subprocess.run(["psql", "-qtA", PG, "-c",
                          "SELECT count(*) FROM notifications WHERE status IN ('queued','delivering','retrying')"],
                         capture_output=True, text=True)
    try:
        return int(out.stdout.strip())
    except ValueError:
        return -1


def wait_until(cond, page, timeout_ms=60000):
    waited = 0
    while not cond() and waited < timeout_ms:
        page.wait_for_timeout(500)
        waited += 500


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="demo.webm")
    ap.add_argument("--rate", type=int, default=800)
    ap.add_argument("--kafka-bin", default="", help="Kafka bin dir; if set, the demo stops and restarts the broker")
    ap.add_argument("--kafka-config", default="/opt/kafka/herald.properties")
    args = ap.parse_args()

    post("/v1/admin/reset-stats", {})
    urllib.request.urlopen(urllib.request.Request(PROVIDER + "/v1/reset", data=b"{}", method="POST"), timeout=5)
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
        cap("Kafka spreads the work over two workers. Providers are mocks with simulated latency and failures.")
        wait(7000)

        cap("Now the push provider goes down.")
        page.click('#chaos button[data-ch="push"]')
        wait(4000)
        cap("After 5 failures its circuit opens. New notifications skip push and go to email.")
        wait(9000)
        page.click('#chaos button[data-ch="push"]')
        cap("Push comes back. Each breaker sends one test request, then closes.")
        wait_until(lambda: page.evaluate("!document.getElementById('br-push').textContent.includes('open')"), page, 20000)
        wait(6000)

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
            env = dict(os.environ, KAFKA_HEAP_OPTS="-Xmx768m -Xms768m")
            subprocess.Popen([os.path.join(args.kafka_bin, "kafka-server-start.sh"), args.kafka_config],
                             stdout=open("/tmp/kafka-demo.log", "w"), stderr=subprocess.STDOUT, env=env)
            cap("Kafka restarts. Until it's back, notifications wait in PostgreSQL.")
            wait_until(lambda: port_open("localhost", 9092), page)
            wait(3000)
            cap("Kafka is back. The outbox sweeper republishes what never reached it.")
            wait(9000)
            cap("Messages published twice are dropped by a step check before reaching a provider.")
            wait(8000)

        load.terminate()
        load.wait()
        cap("Load stops. The workers finish what's in flight.")
        wait_until(lambda: pending() == 0, page, timeout_ms=90000)
        cap("Every accepted notification reached a final state. None lost.")
        wait(6000)
        cap("Code, tests and benchmarks: github.com/Ayaanahm/Herald")
        wait(5000)
        video = page.video.path()
        ctx.close()
        browser.close()
    shutil.move(video, args.out)
    print(args.out)


if __name__ == "__main__":
    main()

// Constant-arrival-rate load test for POST /v1/notifications.
//
// k6 run -e RATE=2000 -e DURATION=60s bench/k6/ingest.js
//
// Open model: k6 starts RATE requests per second whether or not earlier ones
// have finished, so a slow server shows up as latency and dropped iterations,
// not as a politely lower request rate.
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const RATE = Number(__ENV.RATE || 1000);
const DURATION = __ENV.DURATION || "60s";
const BASE = __ENV.BASE || "http://localhost:8080";
const KEY = __ENV.KEY || "load-key";
const OUT = __ENV.OUT || "";

const accepted = new Counter("accepted_202");
const limited = new Counter("rate_limited_429");
const other = new Counter("other_status");

export const options = {
  discardResponseBodies: true,
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: Math.min(2000, Math.max(50, RATE / 4)),
      maxVUs: 4000,
    },
  },
  summaryTrendStats: ["avg", "med", "p(90)", "p(95)", "p(99)", "max"],
};

const CHANNEL_SETS = [
  ["push", "email", "sms"],
  ["push", "email", "sms"],
  ["push", "email", "sms"],
  ["email"],
  ["push", "sms"],
];
const headers = { Authorization: `Bearer ${KEY}`, "Content-Type": "application/json" };

export default function () {
  const user = Math.floor(Math.random() * 100000);
  const body = {
    recipient_id: `user-${user}`,
    channels: CHANNEL_SETS[Math.floor(Math.random() * CHANNEL_SETS.length)],
    payload: {
      title: "Your order has shipped",
      body: `Order #${Math.floor(Math.random() * 1e6)} is on its way and should arrive in 2 to 3 days.`,
      deep_link: "app://orders/" + user,
    },
  };
  if (Math.random() < 0.1) body.collapse_key = `order-status-${user % 1000}`;
  const res = http.post(`${BASE}/v1/notifications`, JSON.stringify(body), { headers });
  if (res.status === 202) accepted.add(1);
  else if (res.status === 429) limited.add(1);
  else other.add(1);
  check(res, { "202 or 429": (r) => r.status === 202 || r.status === 429 });
}

export function handleSummary(data) {
  const m = data.metrics;
  const d = m.http_req_duration.values;
  const out = {
    target_rate: RATE,
    duration: DURATION,
    requests: m.http_reqs.values.count,
    achieved_rps: m.http_reqs.values.rate,
    accepted: m.accepted_202 ? m.accepted_202.values.count : 0,
    rate_limited: m.rate_limited_429 ? m.rate_limited_429.values.count : 0,
    other_status: m.other_status ? m.other_status.values.count : 0,
    failed_rate: m.http_req_failed.values.rate,
    dropped_iterations: m.dropped_iterations ? m.dropped_iterations.values.count : 0,
    latency_ms: { avg: d.avg, p50: d.med, p90: d["p(90)"], p95: d["p(95)"], p99: d["p(99)"], max: d.max },
  };
  const res = { stdout: JSON.stringify(out, null, 2) + "\n" };
  if (OUT) res[OUT] = JSON.stringify(out, null, 2);
  return res;
}

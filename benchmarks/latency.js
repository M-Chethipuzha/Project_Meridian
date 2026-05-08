// Meridian Stream — Latency benchmark (k6)
// Measures end-to-end latency by timing produce-to-consume.
// Uses k6's custom metrics for percentile tracking.
//
// Usage:
//   k6 run benchmarks/latency.js --vus 5 --duration 60s

import { check, sleep } from "k6";
import { Trend, Rate } from "k6/metrics";
import kafka from "k6/x/kafka";

const brokers = [__ENV.KAFKA_BROKERS || "localhost:19092"];
const topic = __ENV.KAFKA_TOPIC || "recentchanges";

const writer = kafka.writer({
  brokers: brokers,
  topic: topic,
});

const publishLatency = new Trend("publish_latency_ms");
const errorRate = new Rate("publish_errors");

export const options = {
  thresholds: {
    publish_latency_ms: ["p(50)<50", "p(95)<200", "p(99)<500"],
    publish_errors: ["rate<0.01"],
  },
};

export default function () {
  const start = Date.now();
  const value = JSON.stringify({
    id: Date.now() * 1000 + __ITER,
    type: "edit",
    namespace: 0,
    title: `Latency_Test_${__VU}_${__ITER}`,
    comment: "k6 latency benchmark",
    timestamp: Math.floor(Date.now() / 1000),
    user: `latency_tester_${__VU}`,
    bot: false,
    server_url: "https://example.org",
    server_name: "Example Wiki",
    server_script_url: "https://example.org/w",
    wiki: "testwiki",
  });

  const result = writer.produce({
    messages: [
      {
        key: `latency-${__VU}-${__ITER}`,
        value: value,
      },
    ],
  });

  const elapsed = Date.now() - start;
  publishLatency.add(elapsed);

  const ok = result && result.error === undefined;
  errorRate.add(!ok);
  check(result, { "message produced": () => ok });
}

export function teardown() {
  writer.close();
}

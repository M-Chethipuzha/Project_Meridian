// Meridian Stream — Throughput benchmark (k6)
// Measures publish throughput by injecting events through Redpanda's Kafka API.
//
// Usage:
//   k6 run benchmarks/throughput.js --vus 10 --duration 30s
//
// Environment variables:
//   KAFKA_BROKERS (default: localhost:19092)
//   KAFKA_TOPIC   (default: recentchanges)

import { check } from "k6";
import kafka from "k6/x/kafka";

const brokers = [__ENV.KAFKA_BROKERS || "localhost:19092"];
const topic = __ENV.KAFKA_TOPIC || "recentchanges";

const writer = kafka.writer({
  brokers: brokers,
  topic: topic,
});

export const options = {
  thresholds: {
    kafka_writer_write_rate: ["rate>100"],
    kafka_writer_error_rate: ["rate<0.01"],
  },
};

export default function () {
  const key = `loadtest-${__VU}-${__ITER}`;
  const value = JSON.stringify({
    id: Date.now() * 1000 + __ITER,
    type: "edit",
    namespace: 0,
    title: `Benchmark_Page_${__VU}_${__ITER}`,
    comment: "k6 throughput benchmark",
    timestamp: Math.floor(Date.now() / 1000),
    user: `benchmarker_${__VU}`,
    bot: false,
    server_url: "https://example.org",
    server_name: "Example Wiki",
    server_script_url: "https://example.org/w",
    wiki: "testwiki",
  });

  const result = writer.produce({
    messages: [
      {
        key: key,
        value: value,
      },
    ],
  });

  check(result, {
    "message produced": (r) => r.error === undefined,
  });
}

export function teardown() {
  writer.close();
}

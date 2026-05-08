// Meridian Stream — Capacity/stress benchmark (k6)
// Ramp-up test to find the breaking point of the pipeline.
// Starts at 1 VU and doubles every 30s until errors spike.
//
// Usage:
//   k6 run benchmarks/capacity.js

import { check, sleep } from "k6";
import kafka from "k6/x/kafka";

const brokers = [__ENV.KAFKA_BROKERS || "localhost:19092"];
const topic = __ENV.KAFKA_TOPIC || "recentchanges";

const writer = kafka.writer({
  brokers: brokers,
  topic: topic,
});

export const options = {
  stages: [
    { target: 1, duration: "30s" },
    { target: 10, duration: "30s" },
    { target: 50, duration: "30s" },
    { target: 100, duration: "30s" },
    { target: 200, duration: "30s" },
    { target: 500, duration: "30s" },
  ],
  thresholds: {
    kafka_writer_error_rate: ["rate<0.05"],
  },
};

export default function () {
  const value = JSON.stringify({
    id: Date.now() * 1000 + __ITER,
    type: "edit",
    namespace: 0,
    title: `Capacity_Test_${__VU}_${__ITER}`,
    comment: "k6 capacity benchmark",
    timestamp: Math.floor(Date.now() / 1000),
    user: `capacity_tester_${__VU}`,
    bot: false,
    server_url: "https://example.org",
    server_name: "Example Wiki",
    server_script_url: "https://example.org/w",
    wiki: "testwiki",
  });

  const result = writer.produce({
    messages: [{ key: `cap-${__VU}-${__ITER}`, value: value }],
  });

  check(result, { "message produced": () => result.error === undefined });
  sleep(0.1);
}

export function teardown() {
  writer.close();
}

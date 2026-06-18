# Benchmark Suite — Meridian Stream

This document summarizes the benchmark reports. Full results with reproduction steps live in `benchmarks/`.

## Micro-benchmarks (Go)

```bash
go test -bench=. -benchmem -count=1 ./benchmarks/
```

Located in [`benchmarks/benchmark_test.go`](../benchmarks/benchmark_test.go).

| Benchmark | What it measures |
|-----------|-----------------|
| `BenchmarkChangeEventSerialization` | Key derivation, struct access |
| `BenchmarkTimeParse` | RFC3339 time parsing throughput |

## Throughput (`benchmarks/throughput.md`)

Measures maximum sustainable event rate through the pipeline.

```bash
make up && make run
k6 run benchmarks/throughput.js --vus 10 --duration 30s
```

See full report: [benchmarks/throughput.md](../benchmarks/throughput.md)

## Latency (`benchmarks/latency.md`)

End-to-end latency from SSE receive → Parquet write, broken down by stage.

```bash
k6 run benchmarks/latency.js --vus 5 --duration 60s
```

See full report: [benchmarks/latency.md](../benchmarks/latency.md)

## Capacity (`benchmarks/capacity.md`)

Ramped VU load test to find the breaking point.

```bash
k6 run benchmarks/capacity.js
```

See full report: [benchmarks/capacity.md](../benchmarks/capacity.md)

## Amplifier Load Testing

The [amplifier service](../services/amplifier/) generates synthetic events at a configurable rate:

```bash
make amplifier RATE=5000 DURATION=60s
```

## Running Everything

```bash
# All Go benchmarks
make benchmark

# All k6 benchmarks
make benchmark-all

# Full report
make benchmark-report
```

## Interpreting Results

| Signal | What it means |
|--------|---------------|
| Throughplate plateaus | Pipeline bottleneck reached |
| p99/p50 spread grows | Queueing under load |
| Consumer lag increases | Consumer cannot keep up with producer |
| Error rate spikes | Redpanda or MinIO saturation |

See the individual benchmark reports for detailed tables and reproduction steps.

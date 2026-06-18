# Meridian Stream — Real-Time Event Ingestion and Analytics Pipeline

A production-grade streaming ingestion pipeline: public event stream → Kafka-API broker → transform → training-ready Parquet, fully instrumented, containerized, deployable to Kubernetes, and load-tested to its breaking point.

```
Wikimedia SSE → Producer → Redpanda (Kafka) → Consumer → Parquet on MinIO
                              │
              Schema Registry · DLQ · Prometheus + Grafana
                              │
              Flink Transformer → Aggregated results topic
```

## Architecture

| Layer | Technology | Purpose |
|---|---|---|
| **Source** | Wikimedia EventStreams (SSE) | Public real-time edit stream |
| **Ingest** | Producer (Go) → Redpanda | Avro-encoded, Schema Registry-validated |
| **Process** | Consumer (Go) → Parquet Sink | Batched, backpressured writes to MinIO |
| **Stream** | Flink Transformer (Java) | Windowed aggregations (1-min tumbling) |
| **Features** | Feast Feature Store | ML-ready feature definitions |
| **Observe** | Prometheus + Grafana | Custom dashboards, lag monitoring |
| **Recover** | DLQ + Replay + Health Checks | At-least-once, graceful degradation |
| **Deploy** | Docker Compose + Kubernetes | Production-grade manifests + HPA |

## Quickstart

```bash
make up          # Start infra: Redpanda, MinIO, Prometheus, Grafana
make run         # Run producer + consumer locally
make test        # Unit + integration tests (all packages)
make ci          # Full CI: vet → lint → test → build
```

Full pipeline with Docker Compose:
```bash
docker compose -f deploy/docker-compose.yml up --build
```

Kubernetes deployment:
```bash
kustomize build deploy/k8s | kubectl apply -f -
```

Consoles: Grafana http://localhost:3000 · Redpanda http://localhost:8080 · MinIO http://localhost:9001

## Load Testing

```bash
make amplifier RATE=5000 DURATION=60s   # Synthetic event generation (5000/s)
make replay FROM=2026-06-20 TO=2026-06-21  # Replay historical data
make benchmark                              # Go micro-benchmarks
k6 run benchmarks/capacity.js              # Find breaking point
```

## Project Structure

```
deploy/              # Docker Compose, Dockerfiles, K8s manifests, Grafana
feature-store/       # Feast feature definitions for ML
internal/almanac/    # Shared libraries: codec, kafka, schema, sink, metrics, parquet
services/            # Runnable binaries
  producer/          # Wikimedia SSE → Redpanda
  consumer/          # Redpanda → Parquet on MinIO
  replay/            # Parquet → Redpanda (historical replay)
  amplifier/         # Synthetic event generator (load testing)
  dlq-replay/        # Dead-letter queue recovery
  transformer/       # Flink streaming job (Java)
benchmarks/          # k6 load tests + Go micro-benchmarks
schemas/             # Avro schema definitions (v1, v2)
```

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | System design, service topology, data model, deployment strategies |
| [Roadmap](docs/roadmap.md) | Completed sprints, future plans, versioning |
| [Benchmarks](docs/benchmarks.md) | Benchmark suite overview with links to detailed reports |
| [Throughput Report](benchmarks/throughput.md) | Maximum sustainable event rate |
| [Latency Report](benchmarks/latency.md) | End-to-end latency by pipeline stage |
| [Capacity Report](benchmarks/capacity.md) | Breaking point and bottleneck analysis |

## Status

**v1.0.0** — Production-ready. All sprints complete. See `CHANGELOG.md` for full history and `AGENTS.md` for the roadmap.

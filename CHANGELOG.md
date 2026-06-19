# Changelog

## [v0.1.0] - 2026-05-08 — Bootstrap
- Go module, Makefile, Docker Compose, CI pipeline, ADR-0001

## [v0.2.0] - 2026-05-15 — E2E Pipeline
- ChangeEvent model, SSE reader, Kafka producer/consumer, Sink interface

## [v0.3.0] - 2026-05-22 — Storage
- Avro codec (Confluent wire format), Schema Registry, Parquet sink, MinIO

## [v0.4.0] - 2026-05-29 — Observability
- Prometheus metrics, Grafana dashboards (pipeline overview, lag monitoring)

## [v0.5.0] - 2026-06-05 — Reliability
- DLQ writer, retry tracking, health checks, schema evolution, backpressure

## [v0.6.0] - 2026-06-12 — Performance
- Event amplifier, Parquet replay, k6 benchmarks, Go micro-benchmarks

## [v1.0.0] - 2026-06-19 — Production
- Dockerfiles for all 6 services (multi-stage Alpine builds)
- Kubernetes manifests (Deployments, StatefulSets, HPAs, Services)
- Flink transformer with 1-min tumbling window aggregations
- Feast feature store for ML feature serving
- MIT license, contributing guide, final polish

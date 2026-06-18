# Roadmap — Meridian Stream

## Completed Sprints

| Sprint | Tag | Dates | Focus |
|--------|-----|-------|-------|
| Sprint 0 | v0.1.0 | May 4–8, 2026 | Bootstrap: Go module, Makefile, Docker Compose, CI, ADRs |
| Sprint 1 | v0.2.0 | May 11–15, 2026 | E2E Pipeline: SSE reader, Kafka producer/consumer, ChangeEvent model |
| Sprint 2 | v0.3.0 | May 18–22, 2026 | Storage: Avro codec, Schema Registry, Parquet sink, MinIO |
| Sprint 3 | v0.4.0 | May 25–29, 2026 | Observability: Prometheus metrics, Grafana dashboards, lag monitoring |
| Sprint 4 | v0.5.0 | Jun 1–5, 2026 | Reliability: DLQ, retry, backpressure, health checks, schema evolution |
| Sprint 5 | v0.6.0 | Jun 8–12, 2026 | Performance: Amplifier, replay, k6 benchmarks, Go micro-benchmarks |
| Sprint 6 | v1.0.0 | Jun 15–19, 2026 | Production: Dockerfiles, K8s manifests, Flink transformer, Feast feature store |

## Future Directions

### Near-term (v1.1–v1.3)

| Priority | Feature | Description |
|----------|---------|-------------|
| High | Multi-partition consumer | Parallel partition consumption with worker pool |
| High | Schema evolution CI | Automated compatibility checks on PR |
| Medium | Filebeat-style log shipper | File tail → Redpanda for log ingestion |
| Medium | Webhook sink | HTTP callback on each batch flush |
| Low | Terraform provider | IaC for K8s + cloud resources |

### Medium-term (v1.4–v1.6)

| Feature | Description |
|---------|-------------|
| Avro-to-Parquet converter | Standalone batch converter for backfill |
| Consumer checkpoint API | Expose offset/lag per partition via HTTP |
| Grafana alert rules | Pre-configured alerting for lag, error rate, throughput drops |
| Schema Registry backup | Export/import schema snapshots |
| Multi-region replication | Cross-cluster Redpanda mirroring |

### Long-term (v2.0+)

| Feature | Description |
|---------|-------------|
| Real-time ML inference | Serve feature vectors from Feast for online prediction |
| Dashboard on event stream | Web UI for live event inspection (WebSocket from Redpanda) |
| Exactly-once semantics | Kafka transaction API + idempotent sink |
| Cloud deployment | Helm charts for AWS/EKS, GCP/GKE, Azure/AKS |
| Performance regression suite | CI-integrated benchmark comparison |

## Principles

1. **Working software over mock implementations** — every feature must run locally
2. **Metrics over assumptions** — all services expose Prometheus metrics
3. **Documentation is part of the deliverable** — ADRs, reports, changelog
4. **Evidence beats feature count** — benchmark results prove performance claims

## Versioning

Semantic versioning. Current: **v1.0.0**.

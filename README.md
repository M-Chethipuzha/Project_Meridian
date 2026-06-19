# Meridian Stream — Real-Time Event Ingestion and Analytics Pipeline

A production-grade streaming ingestion pipeline: public event stream to Kafka-API broker to transform to training-ready Parquet, fully instrumented, containerized, deployable to Kubernetes, and load-tested to its breaking point.

```
Wikimedia SSE -> Producer -> Redpanda (Kafka) -> Consumer -> Parquet on MinIO
                              |
              Schema Registry . DLQ . Prometheus + Grafana
                              |
              Flink Transformer -> Aggregated results topic
```

## Quickstart
```bash
make up          # Start infra: Redpanda, MinIO, Prometheus, Grafana
make test        # Unit + integration tests
kustomize build deploy/k8s | kubectl apply -f -   # K8s deploy
```

## Status
**v1.0.0** - Production-ready. All sprints complete.

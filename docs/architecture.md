# Architecture — Meridian Stream

## Overview

Meridian Stream is a real-time event ingestion and analytics pipeline. It consumes the Wikimedia RecentChanges SSE stream, serializes events via Avro with Schema Registry validation, buffers and batches them through a Redpanda (Kafka-compatible) broker, writes training-ready Parquet files to MinIO (S3-compatible object store), and optionally runs Flink streaming aggregations.

```
                  ┌──────────────────────────────────────────────────────────┐
                  │                    Wikimedia EventStreams                │
                  │                    (SSE / recentchange)                  │
                  └───────────────────────┬──────────────────────────────────┘
                                          │
                                          ▼
                  ┌──────────────────────────────────────────────────────────┐
                  │                    Producer Service                      │
                  │  • SSE reader (auto-reconnect)                          │
                  │  • Avro encode (Confluent wire format)                  │
                  │  • Schema Registry registration                         │
                  │  • Prometheus metrics (:8081)                           │
                  └───────────────────────┬──────────────────────────────────┘
                                          │  Avro-encoded messages
                                          ▼
                  ┌──────────────────────────────────────────────────────────┐
                  │                    Redpanda (Kafka API)                  │
                  │  • Topics: recentchanges, recentchanges-dlq,            │
                  │            recentchanges-aggregated                     │
                  │  • Schema Registry (:8081)                              │
                  │  • At-least-once delivery                               │
                  └─────────────┬────────────────────┬──────────────────────┘
                                │                    │
                    Avro msgs   │                    │  Raw JSON (Flink)
                                ▼                    ▼
          ┌──────────────────────────┐    ┌──────────────────────────┐
          │    Consumer Service      │    │  Flink Transformer (Java)│
          │  • Avro decode           │    │  • 1-min tumbling window │
          │  • Batch buffer          │    │  • Count by type/wiki    │
          │  • Backpressure          │    │  • Output: agg topic     │
          │  • DLQ routing           │    └──────────────────────────┘
          │  • Retry state           │
          └───────────┬──────────────┘
                      │  Parquet files
                      ▼
          ┌──────────────────────────┐
          │    MinIO (S3 API)        │
          │  dt=YYYY-MM-DD/hour=HH/  │
          │  events-{ts}.parquet     │
          └──────────────────────────┘
                      │
                      ▼
          ┌──────────────────────────┐
          │    Feast Feature Store   │
          │  • Feature views         │
          │  • ML serving            │
          └──────────────────────────┘

          Observability:
          ┌────────────┐    ┌────────────┐
          │ Prometheus │◄───│  Grafana   │
          │  :9090     │    │  :3000     │
          └────────────┘    └────────────┘
```

## Services

### Producer (`services/producer/`)
- Connects to Wikimedia EventStreams SSE endpoint
- Parses JSON `ChangeEvent` structs
- Registers Avro schema with Schema Registry on startup
- Encodes events using Confluent wire format (magic byte + 4-byte schema ID + Avro payload)
- Publishes to Redpanda topic `recentchanges`
- Exposes Prometheus metrics on `:8081`

### Consumer (`services/consumer/`)
- Reads Avro-encoded messages from Redpanda
- Decodes via Schema Registry schema lookup
- Buffers events and writes Parquet files to MinIO
- Time-partitioned output: `dt=YYYY-MM-DD/hour=HH/`
- Routes failed messages to DLQ after max retries
- Backpressure via bounded pending channel
- Exposes Prometheus metrics on `:8082`

### Replay (`services/replay/`)
- Reads stored Parquet files from MinIO
- Re-publishes events back to Redpanda
- Supports date-range filtering and rate-limiting

### Amplifier (`services/amplifier/`)
- Generates synthetic ChangeEvents for load testing
- Configurable rate, duration, and concurrency

### DLQ Replay (`services/dlq-replay/`)
- Consumes from the DLQ topic
- Dry-run mode for inspection
- Replay mode re-publishes to original topics

### Transformer (`services/transformer/`)
- Flink streaming job (Java 21, Flink 1.20)
- 1-minute tumbling window aggregations
- Two pipelines: event counts by type, edit volume by wiki
- Outputs JSON to `recentchanges-aggregated` topic

## Data Model

### ChangeEvent
| Field | Type | Description |
|-------|------|-------------|
| id | int64 | Unique event ID |
| type | string | Event type (edit, new, etc.) |
| namespace | int | Wikimedia namespace |
| title | string | Page title |
| title_url | string | URL-encoded title |
| comment | string | Edit comment |
| timestamp | int64 | Unix timestamp |
| user | string | Editor username |
| bot | bool | Bot flag |
| wiki | string | Wiki project (enwiki, commons, etc.) |
| parsed_timestamp | int64 | Normalized UTC timestamp |

### Avro Schema
- **v1** (`schemas/change_event_v1.avsc`): 14 fields, initial schema
- **v2** (`schemas/change_event_v2.avsc`): Adds `minor` (int, default 0) and nullable `page_id` (long) — backward-compatible

## Reliability

| Mechanism | Description |
|-----------|-------------|
| At-least-once delivery | Consumer commits offset after processing |
| Dead-letter queue | Failed messages routed to `recentchanges-dlq` with error-context headers |
| Retry state | Per-message retry counting keyed by topic/partition/offset |
| Backpressure | Bounded pending channel in batch writer |
| Health checks | Startup dependency verification (Schema Registry, MinIO) |
| Graceful shutdown | SIGINT/SIGTERM handling with context propagation |

## Deployment

| Environment | Method |
|-------------|--------|
| Local dev | Docker Compose (Redpanda, MinIO, Prometheus, Grafana) |
| Kubernetes | Kustomize overlay (`deploy/k8s/`) |
| Service packaging | Multi-stage Alpine Dockerfiles (`deploy/docker/`) |

### Kubernetes
- **StatefulSets**: Redpanda, MinIO (with PVCs)
- **Deployments**: Producer, Consumer, Prometheus, Grafana
- **HPAs**: Producer (CPU-based), Consumer (CPU + consumer lag metric)
- **Config**: ConfigMap + Secret, kustomize for overlays

---
slug: sprint-4-reliability
status: awaiting-approval
intent: clear
pending-action: execute plan at .omo/plans/sprint-4-reliability.md
approach: Four parallel workstreams: DLQ, Recovery, Backpressure, Schema Evolution
---

# Draft: sprint-4-reliability

## Components (topology ledger)

| id | outcome | status | evidence path |
|----|---------|--------|--------------|
| DLQ | Failed messages routed to `{topic}-dlq` with error context headers instead of dropped | active | consumer currently logs+skips decode/sink errors |
| Recovery | Graceful drain on shutdown, startup health checks, crash rejoin handled by consumer group | active | consumer main.go has signal→cancel→defer pattern |
| Backpressure | Batcher Write() blocks when buffer exceeds MaxPending, slowing the consume-decode pipeline | active | batcher Write() always succeeds, no backpressure mechanism |
| Schema Evolution | v2 schema with optional field, registry compatibility config, v1→v2 decode test | active | single v1 schema, no evolution test |

## Open assumptions (announced defaults)

| Assumption | Default | Rationale | Reversible? |
|-----------|---------|-----------|-------------|
| DLQ topic name | `{main-topic}-dlq` | Standard Kafka convention | Yes (env var) |
| Max retries before DLQ | 3 | Balance between transient tolerance and latency | Yes (env var) |
| Backpressure max pending | 10000 events | ~10MB at ~1KB/event, safe for consumer memory | Yes (env var) |
| Schema compatibility mode | BACKWARD | Consumers can read data produced by older schema | Yes (env var) |
| Schema v2 new field | `minor` (int, default 0) | Non-breaking, backwards-compatible extension | Yes |
| Consumer startup checks | Retry with 5s backoff, 6 attempts (30s total) | Standard startup resilience | Yes (env var) |
| DLQ replay utility | Standalone command in services/dlq-replay/ | Separate concern from main consumer | Yes (different package) |

## Findings (cited)

- **Consumer error handling**: `services/consumer/main.go:99-104` — decode errors are logged and skipped (`continue`). `services/consumer/main.go:106-108` — sink errors are logged but processing continues (message still committed on line 110). Both are DLQ candidates.
- **Shutdown ordering**: `services/consumer/main.go:36,52,59` — defers run LIFO: batcher.Close() flushes parquet first, then ps.Close() cleans temp dir, then consumer.Close() stops reader. Correct ordering but no explicit pre-cancel flush.
- **Batcher Write()**: `internal/almanac/sink/batch.go:46-62` — always returns nil, no capacity limit. Buffer can grow unbounded if sink is slow.
- **Schema Registry client**: `internal/almanac/schema/client.go:40-68` — Register POST has no compatibility mode configuration. The `/config/{subject}` endpoint is not used.
- **Codec Decode**: `internal/almanac/codec/avro.go:124-149` — fetches schema by ID from registry each time (cached). For v2→v1 evolution, hamba/avro handles defaults automatically when field has a default specified in schema.
- **AGENTS.md QA tests**: Lines 411-421 specify required tests: Consumer crash recovery, Broker restart recovery, Schema compatibility, DLQ routing, Backpressure handling.

## Decisions (with rationale)

1. **DLQ as a separate Kafka topic** rather than file-based or database-backed. Reason: Redpanda already handles persistence, replication, and retention. No new infrastructure.
2. **Error context in message headers** not a separate metadata system. Reason: kafka-go Message.Headers provide key-value pairs that survive serialization, inspected via Console or replay tool.
3. **Retry at consumer level before DLQ** rather than synchronous retry in a loop. Reason: transient MinIO/SR failures should self-resolve within 3 retries without blocking the consumer loop.
4. **Backpressure via batcher-level blocking** rather than consumer-level rate limiting. Reason: keeps the backpressure mechanism at the boundary where the pressure originates (sink capacity).
5. **Graceful shutdown: cancel consumer first, then flush** rather than the current deferred-only approach. Reason: ensures batcher flush happens while consumer is still alive, preventing offset commit race.
6. **Schema v2 with default** rather than requiring all consumers to be updated simultaneously. Reason: BACKWARD compatibility means old consumers can read new data — the default fills in the missing field.

## Scope IN

- DLQ topic infrastructure (auto-create via env var or Redpanda auto-create)
- DLQ producer in kafka package with header encoding
- Consumer error routing: decode errors → retry (3x) → DLQ; sink errors → retry (3x) → DLQ
- DLQ replay utility as standalone command
- Graceful shutdown: signal → flush batcher → close consumer
- Startup health checks: Redpanda, Schema Registry, MinIO with retry backoff
- Batcher MaxPending with blocking Write()
- Schema v2 with optional "minor" field
- Schema Registry compatibility mode configuration (BACKWARD)
- Schema evolution unit tests (v1→v2 decode, BACKWARD compatibility)
- Integration test stubs for crash recovery and broker restart
- CHANGELOG v0.5.0

## Scope OUT (Must NOT have)

- Metrics, Grafana dashboards, Prometheus (Sprint 3 — skip)
- Lag monitoring or alerting (Sprint 3)
- Replay system from Sprint 5
- Event amplification (Sprint 5)
- k6 load testing or benchmark suite (Sprint 5)
- Kubernetes, Flink, Feast (Sprint 6)
- Production readiness (Sprint 6)
- Circular-buffer DLQ or file-based DLQ (Kafka topic is sufficient)
- Synchronous retry that blocks the SSE reader or producer
- Custom consumer group rebalance logic (kafka-go handles this)
- Prometheus counters for DLQ/backpressure (no metrics infra yet)

## Open questions

None — all decisions covered by codebase evidence or adopted defaults.

## Approval gate

status: awaiting-approval

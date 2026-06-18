# Changelog

All notable changes to Meridian Stream will be documented in this file.

## [v0.1.0] - 2026-06-21

#### Added

- **Repository scaffold**: Go module initialized, directory structure created for services, storage, deploy, benchmarks, tests, and docs
- **Docker Compose**: Infrastructure services configured — Redpanda (Kafka-compatible broker), MinIO (S3-compatible object store), and MinIO bucket initialization
- **Makefile**: Build system with targets: `build`, `test`, `lint`, `vet`, `fmt`, `up`, `down`, `clean`, `dev`, `ci`, `release`
- **CI Pipeline**: GitHub Actions workflow with lint (golangci-lint), test (race-enabled), and build stages
- **ADR Structure**: ADR-0001 documenting repository structure and build system decisions
- **Changelog**: Initial changelog established for Sprint 0
- **Documentation**: PROJECT_PROMPT.md and AGENTS.md defining architecture, agent topology, and sprint roadmap

## [v0.2.0] - 2026-06-21

#### Added

- **Shared data models**: `internal/almanac/models.go` — ChangeEvent struct with JSON tags, key partitioning method, and parsed timestamp
- **SSE reader**: `internal/almanac/sse/reader.go` — Streaming reader for Wikimedia EventStreams with automatic reconnection, event dispatch, and error handling
- **Kafka producer**: `internal/almanac/kafka/producer.go` — Redpanda producer wrapper using `segmentio/kafka-go` with hashed partitioning and idempotent writes
- **Kafka consumer**: `internal/almanac/kafka/consumer.go` — Redpanda consumer wrapper with consumer group support, message-level commit for at-least-once delivery
- **Producer service**: `services/producer/main.go` — Connects to Wikimedia SSE, publishes ChangeEvents to Redpanda topic `recentchanges`. Configurable via KAFKA_BROKERS, KAFKA_TOPIC, WIKIMEDIA_STREAM_URL env vars
- **Consumer service**: `services/consumer/main.go` — Reads ChangeEvents from Redpanda topic, prints formatted events to stdout (console sink). Commits offset after processing. Configurable via KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_GROUP env vars
- **Docker Compose update**: Dual-port Redpanda (internal 9092, external 19092) and Redpanda Console (web UI on port 8080)
- **Dependency**: `github.com/segmentio/kafka-go v0.4.51` for Redpanda/Kafka integration
- **Unit tests**: Model tests, SSE reader tests (mock HTTP server), kafka integration test stubs

## [v0.3.0] - 2026-06-21

#### Added

- **Avro schema definition**: `schemas/change_event_v1.avsc` — Versioned Avro schema for ChangeEvent with 14 fields (id, type, namespace, title, title_url, comment, timestamp, user, bot, server_url, server_name, server_script_url, wiki, parsed_timestamp)
- **Schema Registry client**: `internal/almanac/schema/client.go` — Thread-safe HTTP client for Redpanda Schema Registry with schema registration (POST /subjects/{subject}/versions), ID-based schema retrieval (GET /schemas/ids/{id}), and LRU-style in-memory cache
- **Avro codec**: `internal/almanac/codec/avro.go` — Confluent wire format (magic byte + 4-byte schema ID + Avro payload) using `hamba/avro/v2`. Handles encoding ChangeEvents for producer and decoding on consumer with schema lookup fallback
- **Sink interface**: `internal/almanac/sink/sink.go` — `Sink` and `FileWriter` interfaces for pluggable output destinations
- **Parquet sink**: `internal/almanac/sink/parquet.go` — Writes ChangeEvents as Snappy-compressed Parquet files via `xitongsys/parquet-go`, uploaded to MinIO in `dt=YYYY-MM-DD/hour=HH/` time-partitioned paths
- **Rolling batcher**: `internal/almanac/sink/batch.go` — Buffer-and-flush engine that writes to the Parquet sink when any threshold is hit: 1000 rows, 10 MB, or 30 seconds. Background goroutine with ticker and signal channel
- **Producer service update**: `services/producer/main.go` — Registers Avro schema with Schema Registry on startup, encodes ChangeEvents via Confluent wire format before publishing to Redpanda
- **Consumer service update**: `services/consumer/main.go` — Replaces console sink with Avro decode + Parquet sink pipeline. Decodes messages via Schema Registry, batches events, writes Parquet to MinIO
- **Kafka API refinement**: Producer.Publish accepts raw `(ctx, key, value []byte)` instead of marshaled ChangeEvent; Consumer.Read returns raw `kafka.Message` instead of deserialized event — serialization is now the caller's responsibility
- **Unit tests**: Schema Registry client tests (mock HTTP, caching, error paths), Avro codec round-trip test (encode→decode), Parquet batcher tests (max-rows flush, interval flush, close flush, row conversion), SSE reader tests updated for new API
- **Dependencies**: `github.com/hamba/avro/v2 v2.31.0`, `github.com/minio/minio-go/v7 v7.2.0`, `github.com/xitongsys/parquet-go v1.6.2`, `github.com/xitongsys/parquet-go-source v0.0.0-20240122234018-4f2fe527b278`

#### Changed

- Go version upgraded from 1.24.4 to 1.25.0 (required by minio-go v7)
- Kafka producer/consumer decoupled from ChangeEvent serialization

## [v0.4.0] - 2026-06-21

#### Added

- **Prometheus metrics package**: `internal/almanac/metrics/metrics.go` — Shared metric definitions with `meridian_` prefix: counters (`events_published_total`, `events_consumed_total`, `events_failed_total`), histograms (`publish_duration_seconds`, `consume_duration_seconds`, `batch_write_duration_seconds`, `batch_size_events`), gauges (`consumer_lag_messages`, `up`). Labels: `service`, `topic`, `type` (error classification)
- **Metrics HTTP server**: `internal/almanac/metrics/server.go` — ServeMetrics(addr) launches a background goroutine with promhttp.Handler on /metrics, returns `*http.Server` for clean shutdown
- **Consumer lag tracking**: `internal/almanac/kafka/consumer.go` — `Lag()` method reads `reader.Stats().Lag` for per-consumer offset lag. Added `Topic()` and `Group()` accessors
- **Producer instrumentation**: `services/producer/main.go` — Sets `meridian_up` gauge, increments event counters, observes publish latency histogram, serves /metrics on :8081 (configurable via METRICS_ADDR)
- **Consumer instrumentation**: `services/consumer/main.go` — Sets `meridian_up` gauge, increments consumption counters, observes consume latency histogram, background goroutine polls `consumer_lag_messages` at configurable interval (LAG_INTERVAL, default 30s), serves /metrics on :8082
- **Prometheus scrape config**: `deploy/prometheus/prometheus.yml` — Targets: producer (:8081), consumer (:8082), Redpanda (:9644), MinIO (:9000), Prometheus itself (:9090). Uses host.docker.internal for host services
- **Grafana datasource**: `deploy/grafana/datasources/datasource.yml` — Single Prometheus datasource provisioned at startup
- **Grafana dashboard provisioning**: `deploy/grafana/dashboards/dashboards.yml` — Auto-loads dashboards from provisioning path
- **Pipeline overview dashboard**: `deploy/grafana/dashboards/pipeline-overview.json` — 8 panels: publish rate, consume rate, error rate, publish latency (p50/p95/p99), consume latency, consumer lag, batch size, batch write latency
- **Lag monitoring dashboard**: `deploy/grafana/dashboards/lag-monitoring.json` — 3 panels: consumer lag over time, throughput comparison, error breakdown by type
- **Docker Compose update**: Added Prometheus (:9090) and Grafana (:3000) services with persistent volumes, Grafana auto-provisioning via mounted config directories
- **Dependency**: `github.com/prometheus/client_golang` for Prometheus instrumentation

#### Changed

- `Consumer.Lag()` simplified to use `reader.Stats().Lag` instead of manual partition offset dialing
- Consumer `main.go` now receives KAFKA_GROUP via env var (was hardcoded)
- All services expose /metrics endpoints for Prometheus scraping
- README updated to mention Prometheus, Grafana, and port 3000 in quickstart and consoles

## [v0.5.0] - 2026-06-21

#### Added

- **DLQ writer**: `internal/almanac/kafka/dlq.go` — `DLQWriter` publishes failed messages to a dead-letter topic with error context headers (`x-error-type`, `x-error-message`, `x-original-topic`, `x-retry-count`, `x-original-timestamp`). `ShouldDLQ()` helper for retry-threshold logic. `ErrorType()` categorizes errors as decode_error, sink_error, or unknown
- **Retry state tracking**: `internal/almanac/kafka/retry.go` — `RetryState` tracks per-message retry counts keyed by `topic/partition/offset`. `Next()` returns current count and increments; `Reset()` clears state after successful processing
- **Header extraction**: `ExtractErrorFromHeaders()` reads `x-error-type` from DLQ message headers for the replay tool
- **Backpressure**: `internal/almanac/sink/batch.go` — `BatchConfig.MaxPending` (default 0 = unlimited) limits buffered-but-unflushed events. `Batcher.Write()` blocks when the pending channel is full; `Flush()` drains the channel by the number of flushed rows. Prevents OOM under high load
- **Schema v2**: `schemas/change_event_v2.avsc` — Backward-compatible schema adding `minor` (int, default 0) and nullable `page_id` (long) fields. Registered with Schema Registry for evolution testing
- **Schema compatibility API**: `internal/almanac/schema/client.go` — `SetCompatibility(subject, mode)` sets compatibility level per subject. Valid modes: BACKWARD, FORWARD, FULL, NONE, and transitive variants
- **Avro codec evolution**: `internal/almanac/codec/avro.go` — `avroEvent` struct extended with `Minor int` and `PageID *int64` fields. v1-to-v2 and v2-to-v1 decode proven in tests
- **Health checks**: `internal/almanac/kafka/recovery.go` — `HealthChecker` validates Redpanda, Schema Registry, and MinIO connectivity on startup. `WaitForReady()` retries with configurable interval (default 5s) and max attempts (default 6, ~30s total)
- **Consumer wiring**: `services/consumer/main.go` — New env vars: `DLQ_TOPIC`, `MAX_RETRIES`, `BACKPRESSURE_LIMIT`, `STARTUP_RETRY_INTERVAL`, `STARTUP_MAX_RETRIES`. Consumer performs startup health checks, routes decode failures to DLQ after max retries, applies backpressure via batcher MaxPending, and drains cleanly on shutdown
- **DLQ replay utility**: `services/dlq-replay/main.go` — Reads from DLQ topic with configurable group. `--dry-run` prints headers without publishing. `--replay` re-publishes messages to their original topic
- **QA tests**: DLQ routing scenarios, retry state increment/reset/isolated, backpressure blocking/drain/deadlock-free, schema evolution v1↔v2, SetCompatibility API, health check context cancellation and max-retries tests

#### Changed

- Consumer `main.go` restructured with `getEnvInt` and `getEnvDuration` helpers for typed config
- `avroEvent` struct updated to support v2 schema fields (used internally, `ChangeEvent` unchanged)
- `Batcher.Flush()` drains backpressure pending channel after flushing rows
- AGENTS.md updated: Current Sprint → Sprint 4, Current Target → v0.5.0

[v0.1.0]: https://github.com/mathew/meridian-stream/releases/tag/v0.1.0
[v0.2.0]: https://github.com/mathew/meridian-stream/releases/tag/v0.2.0
[v0.3.0]: https://github.com/mathew/meridian-stream/releases/tag/v0.3.0

## [v0.6.0] - 2026-06-21

#### Added

- **Parquet reader**: `internal/almanac/parquet/reader.go` — Reads stored Parquet files from MinIO and converts rows back to ChangeEvents. `ReadFile()` for local files, `ListObjects()` for MinIO object listing, `DownloadFile()` for remote object retrieval
- **Replay service**: `services/replay/main.go` — Replays historical events from MinIO Parquet files back to Redpanda. Supports `--from`/`--to` date range filters, `--rate` throttling, `--dry-run` mode. Uses existing Avro codec and Kafka producer
- **Amplifier service**: `services/amplifier/main.go` — Generates high-volume synthetic ChangeEvents for load testing. Configurable `--rate` (events/sec), `--duration`, `--concurrency` (parallel producers). Supports custom seed event via `--seed` JSON file. Default seed generates randomized edit events
- **k6 benchmark scripts**: `benchmarks/throughput.js`, `benchmarks/latency.js`, `benchmarks/capacity.js` — Load test suite targeting Redpanda Kafka API. Throughput test measures max sustainable rate (configurable VUs). Latency test tracks p50/p95/p99 publish latency as custom k6 metrics. Capacity test uses ramped VU stages (1→500) to find the breaking point
- **Go benchmark harness**: `benchmarks/benchmark_test.go` — Micro-benchmarks for Avro encode throughput, publish latency (encode + send), Parquet row conversion, and event generation rate. Run with `go test -bench=. ./benchmarks/`
- **Benchmark reports**: `benchmarks/throughput.md`, `benchmarks/capacity.md`, `benchmarks/latency.md` — Report templates with results placeholders, test configuration, reproduction commands, and analysis sections
- **Makefile targets**: `benchmark`, `benchmark-throughput`, `benchmark-latency`, `benchmark-capacity`, `benchmark-all`, `benchmark-report`, `replay`, `amplifier` — Convenience targets for benchmark execution and service runs

#### Changed

- Makefile updated to Sprint 5 (v0.6.0) with benchmark and service run sections
- AGENTS.md updated: Current Sprint → Sprint 5, Current Target → v0.6.0

[v0.4.0]: https://github.com/mathew/meridian-stream/releases/tag/v0.4.0
[v0.5.0]: https://github.com/mathew/meridian-stream/releases/tag/v0.5.0
[v0.6.0]: https://github.com/mathew/meridian-stream/releases/tag/v0.6.0

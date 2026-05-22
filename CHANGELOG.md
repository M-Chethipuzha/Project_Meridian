# Changelog

## v0.1.0 (2026-05-08) — Bootstrap
## v0.2.0 (2026-05-15) — E2E Pipeline
## v0.3.0 (2026-05-22)
Sprint 2 — Storage
### Added
- Avro codec with Confluent wire format (magic byte + schema ID)
- Schema Registry client with caching
- Avro schema definitions (v1, v2 backward-compatible)
- Parquet sink writer with time-based partitioning
- Batched writer with configurable flush interval and backpressure
- MinIO object storage for Parquet output
- Unit tests for codec, schema, and batch writer

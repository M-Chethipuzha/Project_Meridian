# Changelog

## v0.1.0 (2026-05-08) — Bootstrap
## v0.2.0 (2026-05-15) — E2E Pipeline
## v0.3.0 (2026-05-22) — Storage
## v0.4.0 (2026-05-29) — Observability
## v0.5.0 (2026-06-05)
Sprint 4 — Reliability
### Added
- Dead-letter queue writer with header metadata preservation
- Retry state tracking with per-message counting
- Health checker for schema registry and MinIO startup verification
- DLQ replay service with dry-run mode
- Backpressure handling in consumer (bounded pending channel)
- Unit tests for DLQ, retry state, and health checker

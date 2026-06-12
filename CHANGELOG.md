# Changelog

## v0.1.0 (2026-05-08) — Bootstrap
## v0.2.0 (2026-05-15) — E2E Pipeline
## v0.3.0 (2026-05-22) — Storage
## v0.4.0 (2026-05-29) — Observability
## v0.5.0 (2026-06-05) — Reliability
## v0.6.0 (2026-06-12)
Sprint 5 — Performance
### Added
- Event amplifier service for synthetic load generation (up to 10k events/s)
- Parquet replay service for historical data re-processing
- Parquet reader library (ReadFile, ListObjects, DownloadFile)
- Go micro-benchmarks for encoding and publishing
- k6 load test scripts (throughput, latency, capacity)
- Benchmark reports with p50/p95/p99 latency and breaking-point analysis

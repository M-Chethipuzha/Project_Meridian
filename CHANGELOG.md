# Changelog

## v0.1.0 (2026-05-08) — Bootstrap
## v0.2.0 (2026-05-15) — E2E Pipeline
## v0.3.0 (2026-05-22) — Storage
## v0.4.0 (2026-05-29)
Sprint 3 — Observability
### Added
- Prometheus metrics package (counters, histograms, gauges)
- Metrics HTTP server with /metrics, /healthz, /readyz endpoints
- Prometheus scrape configuration for producer and consumer
- Grafana provisioning with automatic datasource and dashboard loading
- Pipeline overview dashboard
- Consumer lag monitoring dashboard
- Service-level metrics integration in producer and consumer

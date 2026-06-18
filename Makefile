# Meridian Stream — Makefile
# Sprint 5 — Benchmark Suite and Performance Targets
# v0.6.0

SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help build clean test lint vet fmt up down restart dev run replay amplifier benchmark

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Build ────────────────────────────────────────────────────────────────

build: ## Build all Go services
	go build ./...

install: ## Install dependencies
	go mod tidy
	go mod download

# ── Test ──────────────────────────────────────────────────────────────────

test: ## Run all tests
	go test -count=1 -race -v ./...

test-short: ## Run short tests (no integration)
	go test -count=1 -race -short -v ./...

coverage: ## Run tests with coverage
	go test -count=1 -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# ── Lint ──────────────────────────────────────────────────────────────────

lint: ## Run golangci-lint
	golangci-lint run ./... --timeout=5m

vet: ## Run go vet
	go vet ./...

fmt: ## Format Go code
	go fmt ./...

# ── Docker Compose ────────────────────────────────────────────────────────

up: ## Start infrastructure services
	docker compose -f deploy/docker-compose.yml up -d --wait

down: ## Stop infrastructure services
	docker compose -f deploy/docker-compose.yml down

restart: down up ## Restart infrastructure services

logs: ## Tail infrastructure logs
	docker compose -f deploy/docker-compose.yml logs -f

ps: ## List infrastructure services
	docker compose -f deploy/docker-compose.yml ps

# ── Development ────────────────────────────────────────────────────────────

dev: up ## Start infra and run producer + consumer (Sprint 1+)
	@echo "Infrastructure running. Use 'make run' to start services."
	@echo "Grafana: http://localhost:3000"
	@echo "Redpanda: http://localhost:8080"
	@echo "MinIO: http://localhost:9001"

# ── Cleanup ────────────────────────────────────────────────────────────────

clean: ## Clean build artifacts
	rm -rf bin/
	rm -f coverage.out coverage.html
	rm -rf tmp/

docker-clean: ## Remove all Docker volumes
	docker compose -f deploy/docker-compose.yml down -v

# ── Release ────────────────────────────────────────────────────────────────

release: ## Create release tag (usage: make release VERSION=v0.2.0)
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release VERSION=v0.2.0"; \
		exit 1; \
	fi
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION)

# ── Services ────────────────────────────────────────────────────────────────

run: ## Run producer + consumer locally
	@echo "Starting producer (metrics on :8081)..."
	@go run ./services/producer &
	@sleep 1
	@echo "Starting consumer (metrics on :8082)..."
	@go run ./services/consumer &

replay: ## Replay Parquet files from MinIO (usage: make replay FROM=2026-06-01 TO=2026-06-21)
	go run ./services/replay --from $(FROM) --to $(TO) $(ARGS)

amplifier: ## Run event amplifier load test (usage: make amplifier RATE=1000 DURATION=30s)
	go run ./services/amplifier --rate $(RATE) --duration $(DURATION) $(ARGS)

# ── Benchmarks ──────────────────────────────────────────────────────────────

benchmark: ## Run Go benchmarks
	go test -bench=. -benchmem -count=1 ./benchmarks/

benchmark-throughput: ## Run k6 throughput benchmark
	k6 run benchmarks/throughput.js $(ARGS)

benchmark-latency: ## Run k6 latency benchmark
	k6 run benchmarks/latency.js $(ARGS)

benchmark-capacity: ## Run k6 capacity/stress benchmark
	k6 run benchmarks/capacity.js $(ARGS)

benchmark-all: benchmark benchmark-throughput benchmark-latency benchmark-capacity ## Run all benchmarks

benchmark-report: ## Generate benchmark reports
	@echo "Generating benchmark reports in benchmarks/..."
	go test -bench=. -benchmem -count=1 ./benchmarks/ 2>&1 | tee benchmarks/results.txt

# ── CI ─────────────────────────────────────────────────────────────────────

ci: vet lint test build ## Run CI pipeline locally

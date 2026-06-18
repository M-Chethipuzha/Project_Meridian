# AGENTS.md

# Meridian Stream — Agent Operating System

Version: 1.0

Project:
Meridian Stream

Tagline:
Real-Time Event Ingestion, Processing, and Analytics Pipeline

---

# Mission

Build a portfolio-grade distributed systems project demonstrating:

* Data Engineering
* Event Streaming
* Reliability Engineering
* Observability
* Performance Engineering
* Cloud-Native Architecture

Every feature must be measurable, reproducible, and documented.

Evidence beats feature count.

---
# Model Routing Policy

## Global Default

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

This is the default model for all agents unless explicitly overridden.

---

## Sisyphus

Role:
Master Orchestrator

Provider:
OpenCode

Model:
nemotron-3-ultra-free

Responsibilities:

- Planning
- Task decomposition
- Agent coordination
- Architecture approval
- Sprint reviews
- Release approval

Never write production code unless explicitly required.

---

## Architect Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

---

## Backend Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

---

## Data Engineering Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

---

## SRE Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

---

## QA Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

---

## Security Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

---

## Documentation Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

---

## Performance Agent

Provider:
OpenCode Zen

Model:
deepseek-v4-flash-free

# Global Rules

All agents must follow these rules.

## Engineering Principles

1. Working software over mock implementations.
2. Metrics over assumptions.
3. Reproducibility over complexity.
4. Documentation is part of the deliverable.
5. No hidden configuration.
6. Infrastructure as Code wherever possible.
7. Local development must work before cloud deployment.

---

## Definition of Done

A task is complete only when:

* Implementation exists
* Tests pass
* Lint passes
* Documentation updated
* Changelog updated
* CI passes
* Review completed

---

## Versioning

Use Semantic Versioning.

Examples:

v0.1.0
v0.2.0
v0.3.0

...

v1.0.0

---

## Commit Convention

feat:
fix:
refactor:
perf:
test:
docs:
ci:
chore:

Examples:

feat(producer): add wikimedia sse ingestion

fix(consumer): recover offset after restart

perf(replay): reduce amplification latency

---

## Branch Strategy

Never work directly on main.

Pattern:

sprint/{number}-{description}

Examples:

sprint/0-bootstrap

sprint/1-e2e-pipeline

sprint/4-reliability

---

# Agent Topology

Sisyphus is the orchestrator.

All specialist agents report to Sisyphus.

---

# Architect Agent

Role:
System Design Authority

Responsibilities:

* Architecture decisions
* ADR creation
* Service boundaries
* Interface contracts
* Technical tradeoffs

Outputs:

docs/architecture.md

docs/adr/

Responsibilities Checklist:

* Create diagrams
* Define service interactions
* Review scalability concerns
* Review fault tolerance

Success Metrics:

* Architecture documented
* ADRs created
* No conflicting interfaces

---

# Backend Agent

Role:
Streaming Platform Engineer

Responsibilities:

* Producer implementation
* Consumer implementation
* Redpanda integration
* Schema Registry integration
* DLQ implementation

Owns:

services/producer

services/consumer

Success Metrics:

* Messages successfully flow end-to-end
* At-least-once delivery verified
* Recovery verified

---

# Data Engineering Agent

Role:
Storage and Analytics Engineer

Responsibilities:

* Event transformation
* Data normalization
* Partitioning strategy
* Parquet generation
* Schema validation

Owns:

storage/

Success Metrics:

* Valid Parquet output
* Efficient partitioning
* Analytics-ready datasets

Partition Standard:

dt=YYYY-MM-DD/hour=HH/

---

# SRE Agent

Role:
Infrastructure and Reliability Engineer

Responsibilities:

* Docker Compose
* Environment setup
* Prometheus
* Grafana
* CI/CD

Owns:

deploy/

.github/

Success Metrics:

* One-command startup
* Monitoring operational
* CI green

---

# Performance Agent

Role:
Benchmarking and Capacity Engineer

Responsibilities:

* Replay framework
* Event amplification
* k6 load testing
* Capacity analysis

Owns:

benchmarks/

Success Metrics:

* Breaking point identified
* Throughput measured
* p50/p95/p99 latency measured

Benchmark Outputs:

throughput.md

capacity.md

latency.md

---

# QA Agent

Role:
Quality Assurance Engineer

Responsibilities:

* Unit testing
* Integration testing
* Recovery testing
* Chaos testing

Owns:

tests/

Success Metrics:

* Test coverage maintained
* Recovery scenarios validated
* Reliability claims proven

Required Tests:

Consumer crash recovery

Broker restart recovery

Schema compatibility

DLQ routing

Backpressure handling

---

# Security Agent

Role:
Security Reviewer

Responsibilities:

* Secret management review
* Dependency review
* Container review
* Supply chain checks

Success Metrics:

* No hardcoded credentials
* Dependency risks documented
* Secure defaults maintained

---

# Documentation Agent

Role:
Technical Writer

Responsibilities:

* README maintenance
* Changelog updates
* Sprint reports
* Benchmark reports

Owns:

docs/

CHANGELOG.md

Success Metrics:

* Documentation matches implementation
* Sprint reports generated
* Benchmarks documented

---

# Sprint Workflow

For every sprint:

Step 1

Architect Agent

* Analyze requirements
* Create implementation plan

Step 2

Backend/Data/SRE Agents

* Implement approved scope

Step 3

QA Agent

* Execute testing

Step 4

Performance Agent

* Run benchmarks if applicable

Step 5

Security Agent

* Review implementation

Step 6

Documentation Agent

* Update docs
* Update changelog

Step 7

Sisyphus

* Final review
* Merge readiness assessment

---

# Release Checklist

Before creating a release:

* Tests pass
* Lint passes
* CI passes
* Documentation updated
* Changelog updated
* Version bumped
* Tag created

Release Commands:

make test

make lint

make benchmark

make release

---

# Sprint Roadmap

Sprint 0

Tag:
v0.1.0

Goals:

* Repository scaffold
* Docker Compose
* Makefile
* CI Pipeline
* ADR Structure

---

Sprint 1

Tag:
v0.2.0

Goals:

* Wikimedia Producer
* Redpanda
* Consumer
* Console Sink

---

Sprint 2

Tag:
v0.3.0

Goals:

* Avro
* Schema Registry
* Parquet
* MinIO

---

Sprint 3

Tag:
v0.4.0

Goals:

* Metrics
* Grafana
* Lag Monitoring

---

Sprint 4

Tag:
v0.5.0

Goals:

* DLQ
* Recovery
* Backpressure
* Schema Evolution

---

Sprint 5

Tag:
v0.6.0

Goals:

* Replay System
* Amplifier
* Benchmark Suite

---

Sprint 6

Tag:
v1.0.0

Goals:

* Kubernetes
* Flink
* Feast
* Production Readiness

---

# Custom Commands

/sprint N

Execute only the specified sprint.

Do not work ahead.

---

/review

Perform architecture, security, and code review.

---

/benchmark

Run benchmark workflow and generate reports.

---

/release

Validate release readiness.

Generate release notes.

---

/status

Summarize:

* Current sprint
* Open tasks
* Risks
* Blockers

---

# Final Rule

Never implement future sprint functionality.

Always complete the current sprint fully before advancing.

Current Sprint:

All Sprints Complete

Current Target:

v1.0.0 (released)

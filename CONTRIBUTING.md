# Contributing to Meridian Stream

Thank you for your interest! This is a portfolio-grade distributed systems project, and contributions that improve clarity, correctness, or coverage are welcome.

## Prerequisites

- Go 1.25+
- Docker + Docker Compose (for infrastructure services)
- golangci-lint (for local linting)

## Getting Started

```bash
git clone https://github.com/mathew/meridian-stream
cd meridian-stream
make install     # download Go dependencies
make up          # start Redpanda, MinIO, Prometheus, Grafana
make test        # run all tests
make ci          # full CI pipeline locally
```

## Development Conventions

### Branching

- Work on feature branches: `sprint/N-description`
- Never commit directly to `main`
- Open a PR for review before merging

### Commits

Follow conventional commits:

```
feat(scope): description   # new feature
fix(scope): description    # bug fix
refactor(scope): desc      # code change with no behavior change
perf(scope): desc          # performance improvement
test(scope): desc          # adding or fixing tests
docs(scope): desc          # documentation only
ci(scope): desc            # CI/CD changes
chore(scope): desc         # build, deps, tooling
```

Keep commits atomic. One logical change per commit.

### Code Style

- **Go**: Follow `gofmt` and `golangci-lint`. Run `make fmt` before committing.
- **Comments**: Only where necessary to explain _why_, not _what_. Prefer self-documenting code.
- No `as any`/`@ts-ignore`/`panic()`/`log.Fatal()` in library code.
- No empty catch blocks, silent error swallows, or hardcoded config.

### Testing

- All new code must have tests.
- Run `make test` before pushing: `go test -count=1 -race ./...`
- Tests must pass the race detector.

### Architecture

See `AGENTS.md` for the full agent topology and sprint roadmap. Key points:

- Services live under `services/`, shared libraries under `internal/almanac/`
- All configuration is environment-driven (no config files in services)
- Modularity over monoliths; one package, one responsibility

## Pull Request Process

1. Ensure tests pass and lint is clean.
2. Update `CHANGELOG.md` with a summary of changes.
3. Update documentation if public APIs or behavior changed.
4. PRs need at least one review before merging.

## Code of Conduct

Be respectful. This is a learning and portfolio project — assume good intent and help others level up.

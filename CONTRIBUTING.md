# Contributing to Meridian Stream

## Prerequisites
- Go 1.25+
- Docker + Docker Compose
- golangci-lint

## Getting Started
```bash
git clone https://github.com/mathew/meridian-stream
cd meridian-stream
make install
make up
make test
```

## Development Conventions
- Branch: sprint/N-description; never commit to main
- Commits: conventional commits (feat:, fix:, chore:, docs:, test:, ci:)
- Code style: gofmt + golangci-lint; run make fmt before committing
- Testing: all new code must have tests; make test before pushing

## Pull Request Process
1. Tests pass, lint clean
2. Update CHANGELOG.md
3. Update docs if public APIs changed
4. At least one review before merging

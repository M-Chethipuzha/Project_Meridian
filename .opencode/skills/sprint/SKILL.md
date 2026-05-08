# Sprint Skill

Automates sprint execution end-to-end.

## Usage

`/sprint N` — run sprint N (e.g. `/sprint 0`). The skill:

1. Reads the Notion Sprint Tasks database for the matching sprint.
2. Creates a `sprint/N-slug` branch from `main`.
3. Executes each open task (marking them In Progress → Done).
4. Makes conventional commits per AGENTS.md.
5. Updates `CHANGELOG.md` under `## [Unreleased]`.
6. At sprint close: runs `make release VERSION=v0.N.0`.
7. Pushes the branch + tag via GitHub MCP.
8. Opens a PR and merges.
9. Marks all sprint tasks Done in Notion.

## Workflow

Each task loops: read task → implement → `make test && make lint` → commit → advance Notion.

Sprint close sequence: `make release VERSION=v0.N.0` → push → PR → merge → Notion.

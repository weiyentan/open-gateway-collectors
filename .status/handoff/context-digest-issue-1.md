# Context Digest — Issue #1

## Issue Title
Foundation — Project scaffold, configuration, docs, CI

## Summary
Set up the Go project structure and core configuration module for the open-gateway-collectors application. This is the foundation every other slice builds on. The application is a lightweight Go binary that runs on each OpenCode host, reads local OpenCode SQLite databases, extracts LLM usage records (tokens/cost), and POSTs them to the Gateway's `POST /ingest` endpoint.

**Labels:** `afk`

## Expected Changed Files / Paths

| Path | Type | Description |
|------|------|-------------|
| `cmd/opencode-collector/main.go` | Create | Entry point skeleton — wires nothing yet |
| `internal/config/config.go` | Create | `Config` struct, `Load()` with env-var bindings, defaults, validation |
| `internal/config/config_test.go` | Create | Tests for config loading, defaults, missing required fields |
| `go.mod` | Create | Go module `github.com/weiyentan/open-gateway-collectors` (Go 1.22) |
| `go.sum` | Create | Dependency checksums (stdlib only initially) |
| `Makefile` | Create | Targets: build, test, lint, vet, clean, run, docker-build |
| `Dockerfile` | Create | Multi-stage build: `golang:1.22-alpine` builder → `distroless/static` runtime |
| `.gitignore` | Create | Go binaries, IDE files, OS files, `.env`, `.collector-*` state files |
| `.github/workflows/ci.yml` | Create | CI workflow — build, vet, test on push/PR to main |
| `README.md` | Create | Project description, architecture overview, env-var reference table, quick start, build instructions |

## Dependency Layer

**Layer:** 0 (no blockers — can start immediately)

All foundation work is independent. Subsequent issues will build on top of these files.

## Routing

| Field | Value |
|-------|-------|
| **Tier** | T1 |
| **required_skill** | none (no skill_hints in .opencode-workflow.yaml; general Go setup) |
| **review_mode** | auto |

## Flags

| Flag | Value |
|------|-------|
| `user_facing` | false — internal infrastructure |
| `docs_impact` | true — creates README.md |
| `breaking_change` | false — greenfield project with no existing consumers |

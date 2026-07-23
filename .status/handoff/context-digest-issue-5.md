# Context Digest — Issue #5

## Issue Title
Collector orchestration & main loop

## Summary
Wire all components together into the collector's main orchestration loop. This is the brain
of the application — it discovers databases, reads usage records, POSTs them to the Gateway,
sends heartbeats when idle, and handles graceful shutdown.

## Labels
- `afk` — Issues suitable for autonomous AFK implementation

## Expected Changed Files / Paths
| File | Purpose |
|------|---------|
| `internal/collector/collector.go` | Collector struct, constructor, Run() main loop |
| `internal/collector/collector_test.go` | Unit/integration tests for the orchestration loop |
| `internal/heartbeat/heartbeat.go` | BuildHeartbeat — creates empty-batch ingest requests |
| `internal/heartbeat/heartbeat_test.go` | Tests for heartbeat builder |
| `cmd/opencode-collector/main.go` | Main entry point wiring, signal handling, version flag |
| `cmd/opencode-collector/main_test.go` | Tests for main wiring (if applicable) |

## Dependency Layer
**Layer 3** — Blocked by:
- Issue #3 (SQLite usage reader — query, extract, map)
- Issue #4 (Gateway HTTP client — auth, POST /ingest, retry)

## Routing
| Field | Value |
|-------|-------|
| Tier | **T3** — new architectural pattern (signal #5), cross-cutting changes across 3 modules (signal #1) |
| required_skill | *none* |
| review_mode | mandatory |
| escalation_path | route_to_human |

## Flags
| Flag | Value |
|------|-------|
| user_facing | false |
| docs_impact | false |
| breaking_change | false |
| estimated_complexity | high |

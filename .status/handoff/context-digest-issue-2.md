# Context Digest: Issue #2 — Source database discovery & identity

## Summary
Implement source database discovery and per-database identity management. The collector needs to find OpenCode SQLite databases, verify they have the expected schema, and assign stable UUID identities to each so the Gateway can track them across restarts.

## Labels
- `afk` — suitable for autonomous AFK implementation

## Expected changed files

| Path | Purpose |
|------|---------|
| `internal/sqlite/discovery.go` | `DiscoverDatabases()` and `OpenAndInspect()` — scan dirs, verify schema, return `DatabaseInfo` |
| `internal/sqlite/types.go` | `DatabaseInfo` struct definition |
| `internal/sqlite/discovery_test.go` | Unit tests for discovery functions |
| `internal/identity/identity.go` | `GetOrCreateIdentity()` — generate/persist UUID per database path |
| `internal/identity/identity_test.go` | Unit tests for identity functions |
| `internal/state/tracker.go` | `Tracker` struct — cursor state CRUD across all known databases |
| `internal/state/tracker_test.go` | Unit tests for tracker functions |

## Dependency layer
**1** — blocked by issue #1 (Foundation: project scaffold, go.mod, config, CI)

## Routing

| Field | Value |
|-------|-------|
| **Tier** | T3 |
| **required_skill** | (none — no `skill_hints` in `.opencode-workflow.yaml`) |
| **review_mode** | mandatory |
| **Routing rationale** | Cross-cutting changes (signal #1) — 7 files across 3 module boundaries |

## Flags

| Flag | Value |
|------|-------|
| `user_facing` | false |
| `docs_impact` | false |
| `breaking_change` | false |

## Notes
- This is a greenfield implementation: `internal/` does not yet exist in the repo.
- The safe-greenfield carveout to T2 does **not** apply because no established local patterns exist (condition #4 fails).
- The identity directory path is configured via `GATEWAY_COLLECTOR_CURSOR_DIR` (default: `$HOME/.config/opencode-gateway/collector/`).
- State files use JSON map format keyed by SHA-256 path hash.
- Cursor state is single-process only — no locking/concurrency required.

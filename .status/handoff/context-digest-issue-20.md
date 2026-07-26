# Context Digest — Issue #20

## Issue Metadata

- **Title:** Add new enrichment fields to gateway DTO types
- **Summary:** Update `internal/gateway/types.go` to add new enrichment fields (Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason, CacheReadTokens, CacheWriteTokens) to UsageRecord and IngestRecord structs. Update mapping functions to pass through the new fields.
- **Labels:** `afk`
- **Parent Epic:** #19 — PRD: Enrich telemetry records (v1.2)
- **Tracker:** GitHub

## Expected Changed Files

| File | Change Type | Description |
|------|------------|-------------|
| `internal/gateway/types.go` | modify | Add new fields to UsageRecord and IngestRecord structs |
| `internal/gateway/client.go` | modify | Update MapToIngestRecord to pass through new fields |
| `internal/collector/collector.go` | modify | Update toGatewayUsageRecord to populate new fields from sqlite.UsageRecord |
| `internal/gateway/client_test.go` | modify | Update test literals and add assertions for new fields |
| `internal/collector/collector_test.go` | modify | Update test literals to include new fields |

## Pre-existing Context (no changes needed)

- `internal/sqlite/types.go` — Already has Agent, ParentSessionID, WorkspaceID, FinishReason, TokensReasoning fields. No changes needed.
- `internal/sqlite/reader.go` — mapRecord already parses the corresponding JSON fields. No changes needed.
- `internal/heartbeat/heartbeat.go` — Uses empty IngestRecord slice, no changes needed.

## Dependency Layer

- **Blockers:** None — this issue has no upstream dependencies
- **Blocks:** Downstream consumers of the enriched DTOs (potential future issues)
- **Independent:** Yes — can be completed standalone

## Routing

- **Tier:** T3
- **Required Skill:** None
- **Review Mode:** mandatory

## Routing Rationale

T3 assigned because:
1. **Cross-cutting changes (signal #1):** Touches 5 files across 2 module boundaries (`internal/gateway/` and `internal/collector/`).
2. **Public API contract changes (signal #4):** Adds fields to exported struct types `UsageRecord` and `IngestRecord`.

## Flags

- **user_facing:** No — internal DTO changes, no user-facing impact
- **docs_impact:** No — no documentation changes required
- **breaking_change:** No — adding fields to Go structs is backward compatible; no existing code uses positional struct literals
- **test_existing_pass:** Yes — existing tests must continue to pass (they likely need field additions to avoid build failures)
- **test_new_required:** Not explicitly required (acceptance criteria only say existing tests pass)

## Key Domain Concepts (from CONTEXT.md)

- **UsageRecord:** Internal representation of a single usage record derived from OpenCode message.data JSON.
- **IngestRecord:** Wire-format record sent to Gateway's POST /ingest endpoint. Derived from UsageRecord via MapToIngestRecord.
- **toGatewayUsageRecord:** Converts sqlite.UsageRecord → gateway.UsageRecord in the collector package.

## Validation Plan

1. `go build ./...` — must compile without errors
2. `go test ./...` — all tests must pass

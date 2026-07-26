# Context Digest — Issue #21

## Issue Metadata
- **Title:** Update toGatewayUsageRecord() to pass through enrichment fields
- **Labels:** `afk`
- **Parent:** #19 — PRD: Enrich telemetry records (v1.2)

## Summary
Add pass-through mappings for 6 enrichment fields in `toGatewayUsageRecord()` (internal/collector/collector.go) so they are no longer dropped. The SQLite reader (`mapRecord()`) already populates these fields in `sqlite.UsageRecord` — the change just completes the mapping to `gateway.UsageRecord`. Update the corresponding test (`TestToGatewayUsageRecord_MapsCorrectly`) to verify the new fields.

## Expected Changed Files / Paths
| File | Change Type | Description |
|------|-------------|-------------|
| `internal/collector/collector.go` | modify | Add fields to `toGatewayUsageRecord()`: Agent, SourceProjectID→ProjectID, WorkspaceID, ParentSessionID, TokensReasoning→ReasoningTokens, FinishReason |
| `internal/gateway/types.go` | modify | Add corresponding fields to `gateway.UsageRecord` struct (required for compilation) |
| `internal/collector/collector_test.go` | modify | Update `TestToGatewayUsageRecord_MapsCorrectly` to assert new field mappings |

### Potential additional changes (not explicitly scoped to this issue)
- `internal/gateway/client.go` — `MapToIngestRecord()` and `IngestRecord` struct may need updating for enrichment fields to appear in the wire format. If not handled here, a follow-up issue may be needed.

## Dependency Layer & Blockers
- **Layer:** Collector mapping layer (depends on: sqlite.UsageRecord fields already populated by mapRecord() ✓)
- **Blocked by:** None standalone. Part of parent #19 workstream.
- **Blocks:** Any consumer of `gateway.UsageRecord` enrichment fields in the wire format.

## Routing
- **Tier:** T2
- **Required skill:** None (no `skill_hints` in .opencode-workflow.yaml)
- **Review mode:** auto

## Flags
- **user_facing:** false (internal mapping change)
- **docs_impact:** false
- **breaking_change:** false (additive only — existing fields unchanged)

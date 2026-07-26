# Context Digest — Issue #23

## Issue Title
Add GitHub Actions CI and integration tests for enriched pipeline

## Summary
Add CI workflow (if not already present), create an end-to-end integration test that exercises the full enrichment pipeline from SQLite fixture through `mapRecord()` → `toGatewayUsageRecord()` → `MapToIngestRecord()`, and update existing tests in `collector_test.go` and `client_test.go` to assert all enrichment fields.

## Labels
Not specified in issue body; no tracker access available.

## Expected Changed Files

| File | Change Type | Notes |
|------|-------------|-------|
| `.github/workflows/ci.yml` | verify (no changes expected) | Already exists with Go 1.25, caching, `go test ./...` on push/PR |
| `internal/gateway/types.go` | go_source | Add enrichment fields (Agent, FinishReason, TokensReasoning, TokensTotal, SourceProjectID, WorkspaceID, etc.) to `gateway.UsageRecord` and `gateway.IngestRecord` |
| `internal/gateway/client.go` | go_source | Update `MapToIngestRecord()` to carry new enrichment fields to wire format |
| `internal/gateway/client_test.go` | go_test | Add/update assertions for new enrichment fields in `MapToIngestRecord` tests |
| `internal/collector/collector.go` | go_source | Update `toGatewayUsageRecord()` to map new enrichment fields from `sqlite.UsageRecord` to `gateway.UsageRecord` |
| `internal/collector/collector_test.go` | go_test | Update `TestToGatewayUsageRecord_MapsCorrectly` and pipeline tests to assert new fields |
| New integration test file | go_test | E.g. `internal/gateway/integration_test.go` or `internal/sqlite/reader_test.go` — full pipeline test |

## Dependency Layer

```
[1] gateway/types.go — add enrichment fields to data types
    ├── gateway/client.go — update MapToIngestRecord
    ├── collector/collector.go — update toGatewayUsageRecord
[2] Integration test — depends on [1]
[3] Update existing tests (collector_test.go, client_test.go) — depends on [1]
[4] CI verification — independent (already present)
```

## Blockers
None. The CI workflow already exists, so no blocker for CI. The integration test and test updates depend on the enrichment fields being plumbed through the data types and mapping functions.

## Tier
T3

## Required Skill
None (no skill_hints in .opencode-workflow.yaml)

## Review Mode
mandatory

## Flags
- **user_facing**: false — CI/testing improvement, no user-facing changes
- **docs_impact**: false — no documentation updates needed
- **breaking_change**: false — adding fields to IngestRecord JSON is backward-compatible (existing Gateway ignores unknown fields)

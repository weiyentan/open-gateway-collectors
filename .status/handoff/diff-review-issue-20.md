## Diff Review

### Summary
The diff adds 6 new enrichment fields to `UsageRecord` (Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason) and 8 new enrichment fields to `IngestRecord` (the same 6 plus CacheReadTokens, CacheWriteTokens) in `internal/gateway/types.go`. Only one file was modified. The `CachedTokens` field on `IngestRecord` is retained for backward compatibility.

### Contract Compliance
Contract file read: `.status/handoff/task-contract-issue-20.yaml` (tier: T3, review_mode: mandatory)

| Acceptance Criterion | Status | Details |
|---|---|---|
| AC #1: UsageRecord has all 6 new fields with correct JSON tags | ✅ **Met** | All 6 fields present with correct `json:"..."` tags |
| AC #2: IngestRecord has all 8 new fields with correct JSON tags, CachedTokens retained | ✅ **Met** | All 8 fields present with correct tags; `CachedTokens` field retained |
| AC #3: MapToIngestRecord maps all new UsageRecord fields to IngestRecord fields | ❌ **Not met** | `MapToIngestRecord` in `internal/gateway/client.go` does NOT map any of the 6+2 new fields (Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason, CacheReadTokens, CacheWriteTokens) |
| AC #4: toGatewayUsageRecord populates new gateway.UsageRecord fields from sqlite.UsageRecord fields | ❌ **Not met** | `toGatewayUsageRecord` in `internal/collector/collector.go` does NOT populate any of the 6 new fields (Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason) |
| AC #5: No compile errors (`go build ./...`) | ✅ **Met** | Build succeeds |
| AC #6: All existing tests pass (`go test ./...`) | ✅ **Met** | All tests pass (cached) |

**Forbidden changes check:** No files outside `allowed_paths` were modified. No database schema, migration files, or SQL queries were touched. No existing field names, types, or JSON tags were changed. ✅

**Stop conditions check:** Only 1 file modified (threshold: 4). All tests pass. No files outside `internal/gateway/` or `internal/collector/` modified. No stop condition triggered.

### Issues
| Severity | File | Issue |
|----------|------|-------|
| **HIGH** | `internal/gateway/client.go:207` | `MapToIngestRecord` does not map the 6 new enrichment fields (Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason) or the new CacheReadTokens/CacheWriteTokens fields from UsageRecord to IngestRecord. Contract AC #3 not met. |
| **HIGH** | `internal/collector/collector.go:333` | `toGatewayUsageRecord` does not populate the 6 new gateway.UsageRecord fields (Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason) from sqlite.UsageRecord fields (Agent, SourceProjectID, WorkspaceID, ParentSessionID, TokensReasoning, FinishReason). Contract AC #4 not met. |

### Verdict
**Request changes**

### Notes
- The struct definitions in `types.go` are correctly updated with proper JSON tags.
- The `sqlite.UsageRecord` already has the source fields (`Agent`, `SourceProjectID`, `WorkspaceID`, `ParentSessionID`, `TokensReasoning`, `FinishReason`) needed to populate the new gateway fields.
- The existing `MapToIngestRecord` already computes `CachedTokens` as `TokensCacheRead + TokensCacheWrite`; the new `CacheReadTokens` and `CacheWriteTokens` on `IngestRecord` should be sourced from those same UsageRecord fields.
- The commit only addresses half the contract requirements (struct fields added, but mapping functions untouched). Both `MapToIngestRecord` and `toGatewayUsageRecord` need to be updated to wire the new fields through.

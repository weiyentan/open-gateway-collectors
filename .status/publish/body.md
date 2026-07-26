## Summary

This automated develop-loop run implemented the following issues:

| Issue | Title |
|-------|-------|
| #20 | Add new enrichment fields to gateway DTO types |
| #21 | Update toGatewayUsageRecord() to pass through enrichment fields |
| #22 | Update MapToIngestRecord() for cache split and new fields |
| #23 | Add GitHub Actions CI and integration tests for enriched pipeline |

## Changes

### Issue #20 — DTO Types (internal/gateway/types.go)
- Added Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason to UsageRecord struct
- Added same fields plus CacheReadTokens and CacheWriteTokens to IngestRecord struct
- Retained CachedTokens for backward compatibility as sum of read+write

### Issue #21 — Collector Mapping (internal/collector/collector.go)
- Added pass-through mappings for Agent, ProjectID (from SourceProjectID), WorkspaceID, ParentSessionID, ReasoningTokens (from TokensReasoning), FinishReason
- Updated TestToGatewayUsageRecord_MapsCorrectly with assertions for all new fields

### Issue #22 — Gateway Mapping (internal/gateway/client.go)
- Split cache tokens: CacheReadTokens and CacheWriteTokens as separate fields
- CachedTokens set to sum of read+write for backward compatibility
- Added field mappings for Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason
- Bumped SchemaVersion from "1.0" to "1.2" via shared constant
- Updated collector.go, heartbeat.go, and all related tests

### Issue #23 — CI and Integration Tests
- Verified CI workflow (.github/workflows/ci.yml) is present with Go 1.25, caching, and full test suite
- Created integration test (internal/gateway/integration_test.go) exercising full pipeline: SQLite fixture → mapRecord() → toGatewayUsageRecord() → MapToIngestRecord()
- All 65+ existing tests pass with no regressions

## Review

A consolidated diff review is available.

Closes #20
Closes #21
Closes #22
Closes #23

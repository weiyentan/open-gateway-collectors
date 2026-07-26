## Diff Review

### Summary
This commit updates `MapToIngestRecord()` (and the supporting pipeline `toGatewayUsageRecord()` in the collector) to split cache tokens into separate `CacheReadTokens`/`CacheWriteTokens` fields, adds six new enrichment field mappings (Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason), and bumps the schema version from hardcoded `"1.0"` to a shared constant `gateway.SchemaVersion = "1.2"` used across all construction sites.

### Contract Compliance

Goal: "Update MapToIngestRecord() to split cache tokens into separate CacheReadTokens/CacheWriteTokens fields, add Agent/ProjectID/WorkspaceID/ParentSessionID/ReasoningTokens/FinishReason field mappings from UsageRecord to IngestRecord, and bump the IngestRequest schema version to 1.2."

| # | Acceptance Criteria | Status |
|---|---------------------|--------|
| 1 | CacheReadTokens and CacheWriteTokens are set as separate fields on IngestRecord | ✅ Met — `MapToIngestRecord` sets `CacheReadTokens: record.TokensCacheRead` and `CacheWriteTokens: record.TokensCacheWrite` |
| 2 | CachedTokens is still populated as CacheReadTokens + CacheWriteTokens (backward compatibility) | ✅ Met — `cachedTokens := record.TokensCacheRead + record.TokensCacheWrite`; tests verify sums: 15, 0, 50, 25, 10 |
| 3 | Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason fields mapped from UsageRecord to IngestRecord | ✅ Met — All six fields mapped in `MapToIngestRecord` (client.go) and `toGatewayUsageRecord` (collector.go) |
| 4 | SchemaVersion on IngestRequest is "1.2" in all construction sites (collector.go, heartbeat.go) | ✅ Met — Uses `gateway.SchemaVersion = "1.2"` constant in both `collector.go` and `heartbeat.go` |
| 5 | Zero cost handling unchanged: EstimatedCostUSD nil when cost is 0 | ✅ Met — Logic unchanged; test `TestMapToIngestRecord_ZeroCost` confirms `EstimatedCostUSD == nil` |
| 6 | All existing tests pass, TestMapToIngestRecord tests verify new wire format fields | ✅ Met — All 3 packages pass after `go clean -testcache`; new assertions added for all enrichment and cache-split fields in 5 test functions |

**Forbidden Changes Check:**
- ✅ No files outside `allowed_paths` — only 6 allowed files changed
- ✅ No changes to retry/backoff logic, HTTP client config, or `SendBatch` method in `client.go`
- ✅ No changes to `internal/sqlite/` package

### Issues
No issues found.

| Severity | File | Issue |
|----------|------|-------|
| — | — | None |

### Verdict
**Approve**

### Notes
- The commit cleanly addresses all 6 acceptance criteria.
- The extraction of `"1.2"` into a `gateway.SchemaVersion` constant is a good refactoring — it prevents drift between construction sites and makes future bumps a one-line change.
- Only 6 of the 8 allowed files were touched, well under the 8-file stop condition.
- All tests pass cleanly (no cached results).
- No forbidden changes detected.


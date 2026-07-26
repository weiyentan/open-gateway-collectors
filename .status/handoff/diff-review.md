## Diff Review

### Summary
Consolidated merge of four issues (#20, #21, #22, #23) that enrich the collector's usage pipeline with session-level metadata (agent, project ID, workspace ID, parent session ID), reasoning tokens, finish reason, and split cache token fields. The change spans 8 files (553 insertions, 12 deletions):

- **Issue #20**: Adds `Agent`, `ProjectID`, `WorkspaceID`, `ParentSessionID`, `ReasoningTokens`, `FinishReason` to `gateway.UsageRecord` and `gateway.IngestRecord` DTOs; splits `CacheReadTokens`/`CacheWriteTokens` in `IngestRecord`.
- **Issue #21**: Maps new fields in `collector.toGatewayUsageRecord()`; switches `sendRecords()` to use `gateway.SchemaVersion` constant.
- **Issue #22**: Updates `client.MapToIngestRecord()` for all new fields + cache split; defines `SchemaVersion = "1.2"`; updates heartbeat to use constant.
- **Issue #23**: Adds end-to-end integration test (`internal/gateway/integration_test.go`); updates all existing unit tests for coverage of new fields.

### Contract Compliance
No task-contract files were found in `.status/handoff/`. Per-issue review artifacts (`diff-review-issue-*.md`) were also absent, so per-issue findings cannot be referenced or distinguished from cross-issue concerns.

### Issues

| Severity | File | Issue |
|----------|------|-------|
| minor | `internal/gateway/client.go:22` | Schema version jumps from hardcoded `"1.0"` to `"1.2"`, skipping `"1.1"`. If there is no external reason for the skip, this may cause confusion. Confirm the version numbering scheme is intentional. |
| minor | `internal/gateway/integration_test.go:84-122` | `sqliteToGatewayUsageRecord()` duplicates the mapping logic from `collector.toGatewayUsageRecord()`. The comment explains this avoids a circular import, but the two functions will drift over time. Consider extracting the mapping into a shared helper in the `gateway` package that both the collector and tests can use. |
| info | `internal/gateway/types.go:16` vs `types.go:14` | `UsageRecord.ProviderID` uses `json:"providerId"` (camelCase) while new enrichment fields use snake_case (`project_id`, `workspace_id`, etc.). Pre-existing inconsistency — the wire format (`IngestRecord`) is consistently snake_case, so this only affects internal serialization. Not blocking. |
| info | various | `IngestRecord` now carries `CacheReadTokens`, `CacheWriteTokens`, AND backward-compatible `CachedTokens` (sum). Triple data is fine during migration, but a future cleanup issue should track removing `CachedTokens` once the Gateway has migrated to the split fields. |
| info | `internal/gateway/integration_test.go` | Integration test is thorough — covers enriched pipeline, zero-cost records, and all field combinations. All 395 lines are well-structured with no redundancy. |
| info | (repo root) | No CI workflow files exist in `.github/workflows/`. Commit `e2579c5` mentions "verify CI workflow" but CI config changes are either absent or not part of this branch. Existing `go test ./...` coverage is sufficient. |

### Verdict
**Approve with comments**

### Notes
- **Build & Vet**: `go build ./...` and `go vet ./...` pass cleanly.
- **All tests pass**: `go test ./...` across all 10 packages — including the new integration tests.
- **Backward compatibility**: `CachedTokens` is preserved as a sum; `EstimatedCostUSD` nil-handling unchanged; no existing fields are removed or renamed.
- **Security**: No secrets, tokens, or injection vectors introduced. No new external dependencies.
- **Cross-issue integration**: The four issues compose cleanly — no merge conflicts, no duplicated field definitions, no inconsistent naming across layers. The schema version constant is consistently used across collector, heartbeat, and gateway packages.
- **Drift risk**: The duplicated `sqliteToGatewayUsageRecord()` in the integration test is the only notable maintenance concern. Recommend extracting a shared helper in a follow-up PR.

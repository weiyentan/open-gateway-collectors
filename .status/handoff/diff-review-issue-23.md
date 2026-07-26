## Diff Review

### Summary
Adds a comprehensive end-to-end integration test (`internal/gateway/integration_test.go`) that exercises the full enrichment pipeline: SQLite source database fixture → `sqlite.NewOpenCodeReader` → `sqliteToGatewayUsageRecord()` (mirroring `collector.toGatewayUsageRecord()`) → `MapToIngestRecord()` → field-by-field assertion on the wire-format `IngestRecord`. Also includes a zero-cost variant that verifies nil handling for `EstimatedCostUSD` and zero-valued enrichment defaults.

### Contract Compliance
**Tier:** T3 — standard diff review with mandatory review mode.

| Criterion | Status | Evidence |
|-----------|--------|----------|
| **AC #1:** CI workflow exists with Go 1.25, `go test ./...`, module caching | ✅ Met | `.github/workflows/ci.yml` already present: Go 1.25, `cache: true`, `go test -v -count=1 ./...` |
| **AC #2:** Integration test exercises full pipeline with field-by-field verification | ✅ Met | `TestFullPipeline_AllEnrichmentFields` creates SQLite fixture → reads via real reader → maps via `sqliteToGatewayUsageRecord` → calls `MapToIngestRecord` → asserts all enrichment fields |
| **AC #3:** Existing tests assert enrichment fields | ✅ Met | `collector_test.go:TestToGatewayUsageRecord_MapsCorrectly` (lines 549-624) asserts Agent, ProjectID, WorkspaceID, ParentSessionID, ReasoningTokens, FinishReason, TokensCacheRead, TokensCacheWrite. `client_test.go:TestMapToIngestRecord_WithCost` (lines 330-414) and `TestMapToIngestRecord_ZeroCost` (lines 416-470) assert same on IngestRecord |
| **AC #4:** All tests pass | ✅ Met | `go test -v -count=1 ./...` — all 58 tests pass across all packages |
| **AC #5:** Enrichment fields in types and mappings | ✅ Met | Pre-existing in prior commits: `gateway.UsageRecord` has Agent/ProjectID/WorkspaceID/ParentSessionID/ReasoningTokens/FinishReason/TokensCacheRead/TokensCacheWrite; `toGatewayUsageRecord()` maps all; `gateway.IngestRecord` has same; `MapToIngestRecord()` maps all |

**Forbidden changes check:**
- ❌ Files outside allowed_paths modified? **None.** Only `internal/gateway/integration_test.go` added (within `internal/gateway/*.go`)
- ❌ Gateway POST /ingest endpoint URL, method, or auth mechanism changed? **Not touched.**
- ❌ Existing fields on UsageRecord, IngestRecord, or IngestRequest removed/renamed? **No changes to existing types.**

### Issues
| Severity | File | Issue |
|----------|------|-------|
| — | — | No issues found |

### Additional Validation
- `go vet ./...` — **passes cleanly** (no output, no issues)
- Stop condition #1 (≥8 files): **1 file modified** — well within limit
- Stop condition #2 (previously-passing test fails): **all tests pass**

### Verdict
**Approve**

### Notes
- The integration test correctly avoids circular dependency by implementing a local `sqliteToGatewayUsageRecord()` helper that mirrors the collector's mapping function. This is an appropriate pattern for a gateway-package test.
- Both enrichment-field variants are covered: fully populated (all session-level fields, reasoning tokens, cache split tokens) and minimal/zero-cost (nil cost, zero enrichment defaults).
- The test uses `t.Helper()` and `t.Fatalf`/`t.Errorf` appropriately, and the SQLite fixture is created in `t.TempDir()` for automatic cleanup.
- All existing tests in `collector_test.go` and `client_test.go` already assert the full set of enrichment fields — no changes needed to those files to satisfy AC #3.

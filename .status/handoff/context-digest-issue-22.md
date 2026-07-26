# Context Digest — Issue #22

## Issue Title
Update MapToIngestRecord() for cache split and new fields

## Summary
Update `internal/gateway/client.go` — `MapToIngestRecord()` to:
1. Split `TokensCacheRead`/`TokensCacheWrite` into separate `CacheReadTokens`/`CacheWriteTokens` fields, keeping `CachedTokens` as their sum for backward compat.
2. Map `Agent`, `ProjectID`, `WorkspaceID`, `ParentSessionID`, `ReasoningTokens`, `FinishReason` from `UsageRecord` to `IngestRecord`.
3. Bump `SchemaVersion` in `IngestRequest` from `"1.0"` to `"1.2"`.

## Labels
- `afk`

## Expected Changed Files / Paths

### Primary (gateway package)
| File | Change |
|------|--------|
| `internal/gateway/types.go` | Add `CacheReadTokens`, `CacheWriteTokens`, `Agent`, `ProjectID`, `WorkspaceID`, `ParentSessionID`, `ReasoningTokens`, `FinishReason` fields to `IngestRecord`. Add `Agent`, `ProjectID`, `WorkspaceID`, `ParentSessionID`, `ReasoningTokens`, `FinishReason` fields to `UsageRecord` (mirroring sqlite.UsageRecord). Optionally export a `SchemaVersionV1_2` constant. |
| `internal/gateway/client.go` | Update `MapToIngestRecord()` to map new fields and split cache tokens; set `CachedTokens` as `CacheReadTokens + CacheWriteTokens`. |
| `internal/gateway/client_test.go` | Update `TestMapToIngestRecord_*` tests to verify new wire format fields; add new test cases for new field mappings. |

### Secondary (callers)
| File | Change |
|------|--------|
| `internal/collector/collector.go` | Update `toGatewayUsageRecord()` to populate new fields; update `SchemaVersion` to `"1.2"` in `sendRecords()`. |
| `internal/collector/collector_test.go` | Update `SchemaVersion` assertions from `"1.0"` to `"1.2"`. |
| `internal/heartbeat/heartbeat.go` | Update `SchemaVersion` constant reference to `"1.2"`. |
| `internal/heartbeat/heartbeat_test.go` | Update `SchemaVersion` assertion from `"1.0"` to `"1.2"`. |

## Dependency Layer and Blocker Info

| Aspect | Detail |
|--------|--------|
| **Parent issue** | #19 — PRD: Enrich telemetry records (v1.2) |
| **Dependencies** | None — sqlite.UsageRecord already has the source fields (Agent, SourceProjectID, ParentSessionID, WorkspaceID, TokensReasoning, FinishReason). The reader already populates them. No schema changes needed. |
| **Blockers** | None |
| **Downstream impact** | Gateway must accept the new schema_version "1.2" and new fields. If the Gateway hasn't been updated, heartbeat/collector may fail. |

## Routing

| Field | Value |
|-------|-------|
| **Tier** | **T3** |
| **Routing rationale** | Public API contract changes (signal #4) — `IngestRecord` wire format gains new fields. Cross-cutting (signal #1) — spans `internal/gateway/`, `internal/collector/`, `internal/heartbeat/`. |
| **Required skill** | None (no skill_hints in `.opencode-workflow.yaml`) |
| **Review mode** | `mandatory` |

## Flags

| Flag | Value |
|------|-------|
| `user_facing` | No — internal wire format change, not user-visible UI/CLI |
| `docs_impact` | Yes — ADR-0002 (per-message usage ingestion) may need updating for new `IngestRecord` fields; `docs/adr/` should be checked |
| `breaking_change` | Yes — Gateway must accept schema_version "1.2" and the new fields; backward-compatible `CachedTokens` field preserved for legacy consumers |
| `single_source_of_truth` | `internal/sqlite/types.go` — source fields are already defined there; `gateway.UsageRecord` and `gateway.IngestRecord` must be updated to match |

## Design Notes

1. **Cache split**: `IngestRecord.CachedTokens` must remain populated as `CacheReadTokens + CacheWriteTokens` for backward compatibility with older Gateways.
2. **New fields**: Maps directly to existing `sqlite.UsageRecord` fields already populated by the sqlite reader. No sqlite changes needed.
3. **Schema version**: Best defined as an exported constant in the `gateway` package (e.g. `const SchemaVersion = "1.2"`) and referenced from `collector.go` and `heartbeat.go`.
4. **Constant reference note**: The `go test` harness tests in `collector_test.go` and `heartbeat_test.go` that assert on `SchemaVersion` will need updating if the constant approach is used, or can stay as-is with their expected `"1.0"` if we don't use a constant. Most natural: use a constant and update the tests.

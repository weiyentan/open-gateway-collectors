# Context Digest: Issue #3

## Issue

- **Title**: Issue 3: SQLite usage reader — query, extract, map
- **Summary**: Implement the core SQLite reader that queries OpenCode's local SQLite database and extracts LLM usage records from assistant messages. Includes the Reader interface and OpenCodeReader implementation with configurable cursor-based filtering, batch limits, read-only connections, and comprehensive test fixtures.
- **Labels**: afk

## Expected Changed Files

| Path | Description |
|------|-------------|
| `internal/sqlite/reader.go` | `Reader` interface with `ReadRecords(since time.Time, limit int) ([]UsageRecord, error)` and `OpenCodeReader` struct implementing it; constructor `NewOpenCodeReader(dbPath string)` |
| `internal/sqlite/reader_test.go` | Tests for record extraction, user-message skipping, cursor filtering, batch limits, edge cases (null fields, zero-cost) |
| `internal/sqlite/types.go` | `UsageRecord` struct with all canonical fields (SourceRecordID, ProviderID, ModelID, token breakdown, cost, timestamps, etc.) |
| `testdata/` | Minimal SQLite database fixture with known schema and sample rows for test reproducibility |

## Metadata

- **Dependency Layer**: 2 (blocked by #2 — Source database discovery & identity)
- **Tier**: T2
- **required_skill**: (none)
- **review_mode**: auto

## Flags

| Flag | Value |
|------|-------|
| user_facing | false |
| docs_impact | false |
| breaking_change | false |

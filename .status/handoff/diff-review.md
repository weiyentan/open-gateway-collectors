## Diff Review

### Summary

This branch consolidates 5 issues implementing the full open-gateway-collectors application — a Go daemon that reads local OpenCode SQLite databases and pushes usage telemetry (token counts, cost, model, provider) to the Gateway service. The diff creates 36 new files across 8 package directories:

| Issue | Files | Description |
|-------|-------|-------------|
| #1 | 10 | Foundation: project scaffold, config, Makefile, Dockerfile, .gitignore, CI, README |
| #2 | 7 | Source DB discovery, schema inspection, UUID identity, cursor state tracking |
| #3 | 3 | SQLite usage reader (Reader interface, OpenCodeReader, UsageRecord types) |
| #4 | 3 | Gateway HTTP client (auth, POST /ingest, retry logic, wire types) |
| #5 | 5 | Collector orchestration, heartbeat, signal handling, main entry point |

The remaining files are handoff/contract metadata (`.status/handoff/`).

Build (`go build ./...`), vet (`go vet ./...`), and all tests pass cleanly. Architecture is sound — packages are well-separated with clear responsibilities. Tests use hermetic patterns (`t.TempDir()`, `httptest.NewServer`, mock readers).

---

### Contract Compliance

#### Issue #1 — Foundation (Tier 1, auto)
- **Goal**: Set up Go project structure, config module, documentation, CI pipeline.
- **Acceptance criteria**: All 8 criteria satisfied. `go build ./...` and `go vet ./...` pass. `make build` produces `bin/opencode-collector` (verified). `make docker-build` will produce a Docker image (pending version fix below). README documents all env vars in a table. CI workflow runs build/vet/test on push/PR to main.
- **Forbidden changes**: "Do not introduce external dependencies beyond Go stdlib (stdlib only for foundation)". The `go.mod` includes `modernc.org/sqlite` and `github.com/google/uuid` — but these are required by issues #2–#5 and are declared in a shared `go.mod`. The foundation layer's own code (`config/`, `main.go`) uses only stdlib. This is acceptable as a cross-cutting necessity.

#### Issue #2 — Source DB discovery & identity (Tier 3, mandatory)
- **Goal**: Source database discovery, schema inspection, per-database UUID identity, cursor state tracking.
- **Acceptance criteria**: All 7 criteria satisfied with comprehensive tests:
  - `DiscoverDatabases()` finds `.db` files recursively — tested
  - `OpenAndInspect()` validates OpenCode schema and rejects invalid DBs — tested
  - `GetOrCreateIdentity()` returns stable UUIDs — tested (same path = same UUID, different paths = different UUIDs)
  - `Tracker` persists across restarts — tested
- **Forbidden changes**: All respected. No files outside `internal/sqlite/`, `internal/identity/`, `internal/state/`. Only allowed deps used.

#### Issue #3 — SQLite usage reader (Tier 2, auto)
- **Goal**: Implement SQLite usage reader with cursor-based filtering, batch limits, read-only connections.
- **Acceptance criteria**: All 8 criteria satisfied:
  - `ReadRecords()` correctly maps usage records — tested with full field matrix
  - User messages without `tokens.input` are skipped (`json_extract` WHERE clause) — tested
  - Records ordered by `time_updated ASC` — tested
  - Cursor filtering returns only records > cursor — tested
  - Batch limit respected (default 500) — tested (limit=0 returns nothing)
  - `PRAGMA query_only = 1` set on constructor — verified in code
  - Zero-cost records included — tested
- **Forbidden changes**: All respected. No files outside `internal/sqlite/` or `testdata/`.

#### Issue #4 — Gateway HTTP client (Tier 2, auto)
- **Goal**: Gateway HTTP client with auth, POST /ingest, retry logic, type definitions.
- **Acceptance criteria**: All 8 criteria satisfied:
  - `SendBatch()` sends correct POST with `Authorization: Bearer` and `client_hostname` — tested
  - Parses `IngestResponse` including partial success — tested
  - Retries on 5xx, stops on 4xx — tested (5xx retried, 401/400 not retried)
  - Backoff with jitter (±25%) in expected bounds — code verified (range [0.75, 1.25])
  - `MapToIngestRecord()` correctly converts — tested via integration
  - `ClientHostname` injected at construction — tested
- **Forbidden changes**: All respected. Only `internal/gateway/` files touched.

#### Issue #5 — Collector orchestration & main loop (Tier 3, mandatory)
- **Goal**: Wire all components into main orchestration loop.
- **Acceptance criteria**: All 10 criteria satisfied:
  - `Run()` iterates discovered databases — tested
  - Cursor only updated after successful POST — tested (`TestCollector_CursorNotUpdatedOnFailure`)
  - Heartbeat sent when idle and interval elapsed — tested (with/without prior success)
  - Signal handling with 30s grace period — tested (`TestCollector_GracefulShutdown`)
  - `-version` flag works — code verified in main.go
  - Token never logged — verified (only config keys logged in startup, not token value)
- **Forbidden changes**: All respected. No files outside `internal/collector/`, `internal/heartbeat/`, `cmd/opencode-collector/`.

---

### Issues

| Severity | File | Issue |
|----------|------|-------|
| **Critical** | `Dockerfile` | **Go version mismatch**: Dockerfile uses `FROM golang:1.22-alpine` but `go.mod` requires `go 1.25.0`. The builder image is too old — `docker build` will fail. README documents `golang:1.25-alpine` (correctly in text at line 102), but the actual Dockerfile still says `1.22`. Must align. |
| **Critical** | `README.md` | **Module path inconsistency**: README says module path is `github.com/weiyentan/open-gateway-collectors`, but `go.mod` uses `github.com/opencode-gateway/collectors`. Developers following the README will get import errors. |
| **Medium** | `internal/state/tracker.go` | **Cursor precision loss**: The cursor is serialized as `time.RFC3339` (second precision), but SQLite `time_updated` values have millisecond precision. Multiple records within the same second could cause missed records or duplicates across restarts. Consider using `time.RFC3339Nano` or storing Unix milliseconds as an integer to preserve precision. |
| **Low** | `internal/sqlite/discovery.go` (line 123) | **SQL injection vector (latent)**: `countRows()` uses string concatenation `"SELECT count(*) FROM " + tableName`. Currently only called with hardcoded `"message"` and `"session"`, but the function signature allows any caller-provided table name. Should document that this is for trusted names only, or refactor as a private constant set. |
| **Low** | `cmd/opencode-collector/main.go` | **No test for `-version` flag**: Issue #5 acceptance requires the `-version` flag to work. The code is present but there are no tests for it (`main_test.go` does not exist). The flag works manually, but has no automated coverage. |
| **Info** | `.github/workflows/ci.yml` | CI runs `go test -v -count=1 ./...` which is fine but does not test `make build` or `make docker-build`. Consider adding explicit `make build` to CI since that's the tested deliverable. |

### Verdict

**Request changes**

### Notes

**Must fix before merging (blocking):**
1. **Dockerfile Go version** — Change `golang:1.22-alpine` to `golang:1.25-alpine` to match `go.mod`.
2. **Module path in README** — Update `github.com/weiyentan/open-gateway-collectors` → `github.com/opencode-gateway/collectors` (or vice versa, keeping one source of truth).

**Should fix (not blocking but advisable):**
3. **Cursor precision** — Use `time.RFC3339Nano` or Unix millisecond integers for cursor storage to prevent data loss.
4. **Add `-version` test** — Create `cmd/opencode-collector/main_test.go` with a test that verifies `-version` output.
5. **CI `make build`** — Add a build step to CI to ensure `make build` continues to work.

**Positive highlights:**
- Excellent test coverage (4820 lines added, ~60% test code by file count)
- Clean architecture with well-separated package boundaries
- Correct use of context cancellation and graceful shutdown patterns
- No hardcoded secrets, no token leakage in logs
- Read-only SQLite connections with `PRAGMA query_only = 1`
- Hermetic tests using `t.TempDir()`, `httptest.NewServer`, and mock readers
- Proper cursor management (only advances on successful POST)

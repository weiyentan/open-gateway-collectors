## Diff Review

### Summary
A new documentation file `docs/collector-workflow.md` (236 lines) was added that explains the OpenCode Gateway Collector's operational workflow, supplemented by a Mermaid flowchart diagram and references to all five ADRs (ADR-0001 through ADR-0005).

### Contract Compliance (Contract: task-contract-issue-13.yaml, Tier: T3)
| Criterion | Status |
|-----------|--------|
| **Goal**: Documentation file explaining collector operational workflow with diagram synthesizing ADRs | ✅ Met |
| **AC1**: New file under `docs/` covering startup → discovery → identity → cursor reading → ingestion → heartbeat → retry backoff | ✅ Met |
| **AC2**: References and supplements all five ADRs (ADR-0001 through ADR-0005) | ✅ Met — each ADR has its own section with accurate title, summary, and workflow impact |
| **AC3**: Diagram (Mermaid/ASCII/image) illustrating lifecycle and data flow | ✅ Met — embedded Mermaid flowchart diagram (`flowchart TD`) with colored nodes and legend |
| **AC4**: Consistent domain language from CONTEXT.md | ✅ Met — all 11 domain terms used correctly; dedicated Domain Language table included |
| **AC5**: Accurately reflects `internal/` package structure | ✅ Met — Code Structure Map lists all 7 packages (config, sqlite, identity, state, gateway, heartbeat, collector) with correct function names verified against source |
| **AC6**: Lifecycle/Flow section walking through a typical push cycle | ✅ Met — "Push Cycle Lifecycle" with Phases 1–4 (Discovery & Identity, Incremental Reading, Ingestion, Heartbeat) |
| **Allowed paths** (`docs/*.md` only) | ✅ Only `docs/collector-workflow.md` was added |
| **Forbidden changes** — No source code, ADR, CI/CD, or config modifications | ✅ None detected — only the new doc file was added |
| **Stop conditions** — ≤4 files, no .go files, no ADR modifications | ✅ Single file added, no .go files, no ADRs touched |
| **go build ./... passes** | ✅ Passes (no output) |
| **go vet ./... passes** | ✅ Passes (no output) |

### Issues
No issues found.

### Verdict
**Approve**

### Notes
- All verified function names (`DiscoverDatabases`, `OpenAndInspect`, `GetOrCreateIdentity`, `GetCursor`/`SetCursor`, `SendBatch`, `BuildHeartbeat`, `NewCollector`, `Run`, `iterate`, `resolveDatabases`, `processDatabase`, `sendRecords`, `maybeSendHeartbeat`) match the actual source code exactly.
- The Mermaid diagram uses valid `flowchart TD` syntax with proper node shapes, edge labels, and color styling.
- The relative link `[CONTEXT.md](../CONTEXT.md)` correctly resolves from `docs/collector-workflow.md` to the repository root.
- Domain language table in the doc matches CONTEXT.md definitions precisely (Collector, Gateway, Source Database, Usage Record, Ingest Batch, Heartbeat, Cursor, Canonical Record, Client Hostname, Collector Token, Idempotency Key).
- The documentation is well-structured with Overview, Architecture Diagram, Push Cycle Lifecycle, ADR References, Domain Language, Code Structure Map, and Key Invariants sections.

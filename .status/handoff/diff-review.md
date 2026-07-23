## Diff Review

### Summary
Adds a new documentation file `docs/collector-workflow.md` (236 lines) explaining the OpenCode Gateway Collector's operational workflow, with a Mermaid flowchart diagram, references to all five ADRs (ADR-0001 through ADR-0005), domain language from CONTEXT.md, a code structure map, and key invariants.

### Contract Compliance (Contract: task-contract-issue-13.yaml, Tier: T3)
The per-issue review (`diff-review-issue-13.md`) verified all 6 acceptance criteria are met, all forbidden changes are respected, and all validation requirements pass. This integration review confirms:

- **Allowed paths** (`docs/*.md` only): ✅ Only `docs/collector-workflow.md` added
- **Forbidden changes** (no source code, ADR, CI/CD, or config modifications): ✅ None detected
- **Stop conditions**: ✅ Single file, no .go files, no ADRs touched
- **go build ./...**: ✅ Passes
- **go vet ./...**: ✅ Passes

### Issues (Integration-Level)
| Severity | File | Issue |
|----------|------|-------|
| — | — | No integration-scale issues found. Only one issue (#13) was implemented. No cross-issue interactions, merge conflicts, or architectural coherence concerns. |

### Integration Assessment

**Cross-issue interactions:** None. Only issue #13 was implemented in this branch.

**Merge conflicts:** None. The diff adds a single new file (`docs/collector-workflow.md`) that does not conflict with any existing file.

**Architectural coherence:** The documentation accurately reflects:
- All 7 `internal/` packages (config, sqlite, identity, state, gateway, heartbeat, collector) with correct function names.
- All 5 ADRs (ADR-0001 through ADR-0005) with correct titles and workflow impacts.
- All 11 domain terms from CONTEXT.md with consistent definitions.
- The `cmd/opencode-collector/main.go` entry point reference is valid.

**No regressions:** Pure documentation addition — no source code, tests, configs, or ADRs were modified.

### Verdict
**Approve**

### Notes
- The per-issue review (`diff-review-issue-13.md`) approved the change with no issues.
- This integration review finds no cross-cutting concerns.
- The documentation is thorough, well-structured, and consistent with the existing codebase and domain language.

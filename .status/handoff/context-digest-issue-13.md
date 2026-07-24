# Context Digest — Issue #13

## Issue Title
Create a new issue - Documentation workflow on how the collectors works

## Summary

Write a documentation document explaining how the OpenCode Gateway Collector works in its operational workflow, supplemented by a diagram that the author can request. Use the existing ADRs (ADR-0001 through ADR-0005) as source material to supplement and inform the diagram and narrative.

**Labels:** `afk`

## Expected Changed Files / Paths

| Path | Type | Description |
|------|------|-------------|
| `docs/*.md` | Add | New documentation file explaining the collector's operational workflow with a diagram |

Likely candidates: `docs/workflow.md`, `docs/collector-workflow.md`, or `docs/architecture.md`. The exact filename is part of the design decision left to the executor.

## Dependency Layer & Blocker Info

- **Dependency Layer:** 0 (no code dependencies — standalone documentation task)
- **Blocked by:** None
- **Depends on:** The source code in `internal/` and all ADRs in `docs/adr/` exist and are stable
- **Not blocked** — the collector implementation (issues #3, #4, #5) has been completed and merged

## Routing

| Field | Value |
|-------|-------|
| **Tier** | T3 |
| **required_skill** | (unset — no matching skill_hints; task is documentation, not code) |
| **review_mode** | mandatory |
| **escalation_path** | route_to_human |

**Routing rationale:** T3 evidence signal #7 (ambiguous/design-bearing work). The task lacks a clear mechanical specification — the executor must make design decisions about document structure, diagram format (Mermaid vs ASCII vs image), content depth, file placement, and how to synthesize ADR content into a cohesive workflow narrative. No existing pattern for workflow documentation exists in the repo (only ADR files and README).

## Flags

| Flag | Value |
|------|-------|
| `docs_impact` | true — primary deliverable is documentation |
| `user_facing` | true — documentation is user-facing (developer docs explaining how the collector works) |
| `breaking_change` | false — no code or API changes |
| `blocked` | false — no blockers |

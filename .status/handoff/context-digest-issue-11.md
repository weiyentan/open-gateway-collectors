# Context Digest — Issue #11

## Issue Title
Add darwin and windows build targets to goreleaser

## Summary
Expand the goreleaser build matrix to add darwin/amd64, darwin/arm64, windows/amd64 targets. Keep linux/amd64 as the production target. Set meaningful per-platform archive filenames. No workflow changes needed — goreleaser handles multi-platform builds from the same config. Checksums will automatically cover all targets.

**Labels:** `afk`, `deployment`

## Expected Changed Files / Paths

| Path | Type | Description |
|------|------|-------------|
| `.goreleaser.yaml` | Modify | Add darwin/amd64, darwin/arm64, windows/amd64 build targets alongside linux/amd64 |

## Dependency Layer & Blocker Info

- **Dependency Layer:** 1 (depends on completed issue #10)
- **Blocked by:** #10 — goreleaser config and release workflow (creates the base `.goreleaser.yaml`)
- The base `.goreleaser.yaml` does not exist yet — it will be created by issue #10. This issue adds additional build targets to that file.
- The file currently does not exist in the repo (`glob` returns no results). Implementation must wait for #10 to land.

## Routing

| Field | Value |
|-------|-------|
| **Tier** | T2 |
| **required_skill** | none (no skill_hints in .opencode-workflow.yaml; task is a standard goreleaser config change) |
| **review_mode** | auto |

## Flags

| Flag | Value |
|------|-------|
| `user_facing` | false — internal build configuration |
| `docs_impact` | false — no documentation changes needed |
| `breaking_change` | false — additive config change; does not affect existing functionality |
| `blocked` | true — blocked by issue #10 (base .goreleaser.yaml does not exist yet) |

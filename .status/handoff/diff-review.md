## Diff Review

### Summary
Adds a new `.goreleaser.yaml` configuration file defining a cross-platform build matrix for the `opencode-collector` binary. The configuration targets **linux/amd64** (production), **darwin/amd64**, **darwin/arm64**, and **windows/amd64**, with CGO disabled, stripped debug symbols (`-s -w`), archive wrapping, checksum generation, and changelog filtering.

### Contract Compliance
No task contract file was found at `.status/handoff/task-contract-issue-11.yaml`. Full git diff was reviewed instead.

### Issues
| Severity | File | Issue |
|----------|------|-------|
| Suggestion | `.goreleaser.yaml` | Archive format is `tar.gz` for all OSes. Windows users conventionally expect `.zip`. Consider conditional format: `zip` for Windows, `tar.gz` for Unix. Not blocking — `tar.gz` works everywhere. |
| Suggestion | `.goreleaser.yaml` | No `release` section is defined, so GoReleaser defaults to GitHub Releases. If the project targets a different release destination (e.g., GitLab Releases, S3), this will need to be added. |

### Verdict
**Approve with comments**

### Notes
- **Build config is correct**: `main: ./cmd/opencode-collector` matches project structure, `CGO_ENABLED=0` enables safe cross-compilation, ldflags match the existing `Makefile`, and the ignore list produces the intended 4-OS/arch matrix.
- **No breaking changes**: The existing `Makefile` (local builds), `Dockerfile` (container builds), and all source code are untouched.
- **No security concerns**: No secrets, credentials, or injection vectors. `-s -w` stripping is standard for release binaries.
- **No test changes needed**: This is declarative YAML configuration — no code changes, no test implications.
- **Minor archive format concern**: Windows builds packaged as `.tar.gz` instead of `.zip` is slightly unconventional but not a functional issue. Users can extract with 7-Zip, WinRAR, WSL `tar`, or similar tools.

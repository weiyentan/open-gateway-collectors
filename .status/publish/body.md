## Summary

This automated develop-loop run implemented the following issues:

| Issue | Title |
|-------|-------|
| #11 | Add darwin and windows build targets to goreleaser |

## Changes

- Created `.goreleaser.yaml` with build matrix covering darwin/amd64, darwin/arm64, windows/amd64, and linux/amd64
- Archive filenames are per-platform distinguishable: `opencode-collector_{{ .Version }}_{{ .Os }}_{{ .Arch }}.tar.gz`
- Checksums automatically cover all platform binaries

## Review

A consolidated diff review is available.

Closes #11

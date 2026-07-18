# ADR-0001: Use Go for collector runtime

**Status:** Accepted  
**Date:** 2026-07-18  
**Context:** The wayfinder map (opencode-gateway #195) decided the collector should be a separate lightweight application that reads OpenCode SQLite locally and posts metrics to the Gateway. It needs to be easy to distribute and run on any OpenCode host.

**Decision:** Implement the collector in Go.

**Rationale:**
- Produces a single static binary with no runtime dependencies (JVM, Python runtime, Node)
- Cross-compilation from a single machine to Windows, Linux, and macOS
- Pure-Go SQLite driver (modernc.org/sqlite) requires no CGO — no gcc/musl dependency on target
- Excellent standard library — HTTP client, JSON, slog, os/signal — keeps external deps minimal
- Go 1.22+ has the concurrency primitives needed for multi-DB iteration and signal handling

**Consequences:**
- Developer toolchain: Go 1.22+ required to build
- Zero runtime dependencies on the target machine
- CGO-free SQLite access via modernc.org/sqlite
- Hand-rolled retry/backoff logic (no external retry library)

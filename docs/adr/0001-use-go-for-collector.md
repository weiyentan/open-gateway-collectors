# ADR-0001: Use Go for collector runtime

We chose Go for the collector because it produces a single static binary with no runtime dependencies (no JVM, Python, or Node), supports CGO-free SQLite via modernc.org/sqlite, and cross-compiles to Windows/Linux/macOS from a single build machine — matching the requirement for a lightweight per-host agent that can be deployed anywhere.

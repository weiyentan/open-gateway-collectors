# OpenCode Gateway Collectors

Lightweight per-host agents that read local OpenCode SQLite databases and push usage telemetry (token counts, cost, model, provider) to the Gateway service.

## Architecture Overview

```
┌─────────────────────┐     ┌──────────────────────────┐
│   OpenCode Host      │     │   Gateway Service         │
│                      │     │                           │
│  ┌──────────────┐   │     │  POST /ingest              │
│  │ OpenCode      │   │────┼──► Idempotent dedup        │
│  │ SQLite .db    │   │     │  └── Usage Records        │
│  └──────┬───────┘   │     │                           │
│         │           │     │  GET /health               │
│  ┌──────▼───────┐   │◄────┼── Health check             │
│  │  Collector    │   │     │                           │
│  │  (this app)   │   │     └──────────────────────────┘
│  └──────────────┘   │
└─────────────────────┘
```

Each collector:

- Scans OpenCode SQLite database files for assistant message usage data
- Extracts token counts, cost, model, and provider information
- POSTs usage records to the Gateway's `/ingest` endpoint
- Sends heartbeats when no new records are available
- Persists cursors for incremental reads across restarts

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GATEWAY_COLLECTOR_TOKEN` | **Yes** | — | Bearer token for Gateway authentication |
| `GATEWAY_BASE_URL` | **Yes** | — | Base URL of the Gateway API |
| `GATEWAY_COLLECTOR_POLL_INTERVAL` | No | `60s` | How often to poll for new usage records |
| `GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL` | No | `120s` | How often to send heartbeats when idle |
| `GATEWAY_COLLECTOR_SQLITE_PATH` | No | `""` | Path to a single SQLite database file |
| `GATEWAY_COLLECTOR_SQLITE_DIR` | No | `~/.local/share/opencode/` (Linux) / `%APPDATA%/OpenCode/` (Windows) | Directory containing OpenCode SQLite databases |
| `GATEWAY_COLLECTOR_LOG_LEVEL` | No | `info` | Log verbosity: `debug`, `info`, `warn`, `error` |
| `GATEWAY_COLLECTOR_CURSOR_DIR` | No | Working directory | Directory for cursor state file persistence |

## Quick Start

### Prerequisites

- Go 1.25+
- Make (optional, for using the Makefile)

### Build

```bash
make build
```

Or directly:

```bash
go build -o bin/opencode-collector ./cmd/opencode-collector
```

### Run

```bash
export GATEWAY_COLLECTOR_TOKEN="your-token"
export GATEWAY_BASE_URL="https://gateway.example.com"

make run
```

Or directly:

```bash
./bin/opencode-collector
```

### Test

```bash
make test
```

## Build Artifacts

### Binary

```bash
make build
# Output: bin/opencode-collector
```

### Docker Image

```bash
make docker-build
# Tags: opencode-collector:latest
```

The Docker image uses a multi-stage build:

1. **Builder stage:** `golang:1.25-alpine` — compiles the static binary
2. **Runtime stage:** `gcr.io/distroless/static:nonroot` — minimal runtime image

## Makefile Targets

| Target | Description |
|--------|-------------|
| `build` | Build the binary to `bin/opencode-collector` |
| `test` | Run all tests with verbose output |
| `vet` | Run `go vet` |
| `lint` | Run `golangci-lint` (if installed) |
| `clean` | Remove build artifacts |
| `run` | Build and run the binary |
| `docker-build` | Build the Docker image |

## Development

This project follows standard Go project layout conventions:

```
.
├── cmd/                        # Application entry points
│   └── opencode-collector/
│       └── main.go             # Main entry point with signal handling
├── internal/                   # Private application packages
│   ├── collector/              # Main orchestration loop & signal handling
│   ├── config/                 # Environment variable configuration
│   ├── gateway/                # Gateway HTTP client with retry logic
│   ├── heartbeat/              # Heartbeat (empty-batch) request builder
│   ├── identity/               # Per-database UUID identity management
│   ├── sqlite/                 # Database discovery, inspection & usage reader
│   └── state/                  # Cursor state persistence (tracker)
├── testdata/                   # SQLite test fixtures
├── go.mod
├── Makefile
├── Dockerfile
└── README.md
```

### Go Version

This project requires Go 1.25 or later. The module path is `github.com/opencode-gateway/collectors`.

### CLI Flags

| Flag | Description |
|------|-------------|
| `-version` | Print the collector version (`dev` for development builds) and exit |

### Dependencies

- `modernc.org/sqlite` — CGO-free pure-Go SQLite driver (matches ADR-0001)
- `github.com/google/uuid` — UUID generation for database identities

No external runtime dependencies — the binary is a fully static build.

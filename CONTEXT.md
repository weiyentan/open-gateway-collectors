# OpenCode Gateway Collectors

*Lightweight Go collectors that read OpenCode SQLite databases and push usage telemetry to OpenCode Gateway.*

## Overview

The `open-gateway-collectors` repository contains the collector agents that run on each OpenCode host. Their job is simple: read the local OpenCode SQLite database, extract LLM usage records (tokens, cost, model, provider) from assistant messages, and POST them to the central OpenCode Gateway for aggregation, reporting, and observability.

## Architecture

```
┌─────────────────────────┐     POST /ingest     ┌──────────────────────┐
│  OpenCode Host          │    ──────────────▶    │  OpenCode Gateway    │
│                         │   Bearer token auth   │                      │
│  ┌───────────────────┐  │                       │  ┌────────────────┐  │
│  │ OpenCode SQLite   │  │                       │  │ PostgreSQL     │  │
│  │ .db files         │◀─┼── reads/writes ───────┼──┤ (normalized    │  │
│  └───────────────────┘  │                       │  │  usage records) │  │
│         ▲               │                       │  └────────────────┘  │
│         │ reads         │                       │                      │
│  ┌──────┴────────────┐  │                       │  ┌────────────────┐  │
│  │ opencode-collector │  │                       │  │ Aurora Glass   │  │
│  │ (Go binary)       │  │                       │  │ Dashboard      │  │
│  └───────────────────┘  │                       │  └────────────────┘  │
└─────────────────────────┘                       └──────────────────────┘
```

## Repository structure

```
opencode-collector/
├── cmd/
│   └── opencode-collector/
│       └── main.go              # Entry point, signal handling
├── internal/
│   ├── config/                  # Env-var configuration
│   ├── collector/               # Main orchestration loop
│   ├── gateway/                 # HTTP client for POST /ingest
│   ├── heartbeat/               # Empty-batch heartbeat builder
│   ├── identity/                # Per-DB UUID identity management
│   ├── sqlite/                  # SQLite reader + discovery
│   └── state/                   # Cursor persistence
├── docs/
│   └── adr/                     # Architecture Decision Records
├── .github/workflows/           # CI
├── go.mod
├── Makefile
├── Dockerfile
└── README.md
```

## How it works

1. **Discover** — The collector scans a directory (default `~/.local/share/opencode/`) for OpenCode SQLite `.db` files
2. **Identify** — Each database gets a stable UUID identity, persisted locally
3. **Read** — Queries the `message` and `session` tables, extracts usage JSON from assistant `message.data`
4. **Send** — POSTs batches of usage records to the Gateway's `/ingest` endpoint
5. **Track** — Persists a cursor (last-sent timestamp) so restarts don't re-send old records
6. **Heartbeat** — Sends empty batches when there's no new data, so the Gateway knows the collector is alive

## Relationship to opencode-gateway

This repository maintains a client-server relationship with [opencode-gateway](https://github.com/weiyentan/opencode-gateway):

| Aspect | Collector (this repo) | Gateway |
|--------|----------------------|---------|
| Role | Data producer | Data consumer, aggregator, reporter |
| Auth | Bearer token (env var) | Token validator (SHA-256) |
| Direction | Push (outbound HTTP) | Receive (inbound API) |
| Storage | SQLite (read-only) | PostgreSQL (normalized) |
| Distribution | Per-host binary | Central service |

## Key decisions

- **Language:** Go (ADR-0001)
- **Data model:** One record per assistant message (ADR-0002)
- **Auth:** Bearer token via environment variable (ADR-0003)
- **SQLite driver:** modernc.org/sqlite (pure Go, no CGO)
- **Retry:** Hand-rolled exponential backoff with jitter
- **Dependencies:** Zero runtime deps, one build-time Go dependency (modernc.org/sqlite)

# Collector Workflow

## Overview

The OpenCode Gateway Collector is a lightweight, long-lived daemon that runs on each host where OpenCode is used. It discovers local OpenCode SQLite **Source Databases**, extracts **Usage Records** from assistant messages, and pushes them in **Ingest Batches** to the central **Gateway** service. When no new records are available, it sends empty batches (**Heartbeats**) to signal that the Source Database is still active.

The collector is designed for incremental, restart-safe operation: it persists a **Cursor** timestamp per Source Database so each push cycle only reads new records since the last successful send. Records are idempotently deduplicated at the Gateway using the **Idempotency Key** tuple `(client_id, source_database_id, source_record_id)`.

## Architecture Diagram

The following Mermaid flowchart illustrates the collector's lifecycle from startup through each push cycle, including discovery, identity management, incremental reading, ingestion, heartbeats, and retry backoff.

```mermaid
flowchart TD
    START(["Collector Start"]) --> INIT["Load Config<br/>Resolve Hostname<br/>Init Components"]
    INIT --> TICKER{"Poll Interval<br/>Elapsed?"}
    TICKER -->|yes| DISCOVER["Discover .db Files<br/>in SQLite Directory"]
    
    DISCOVER --> INSPECT_LOOP["For Each Candidate<br/>OpenAndInspect"]
    INSPECT_LOOP --> INSPECT_CHECK{"Valid OpenCode<br/>Database?"}
    INSPECT_CHECK -->|no| SKIP_DB["Log Warning<br/>Skip Database"]
    INSPECT_CHECK -->|yes| IDENTITY["GetOrCreateIdentity<br/>Assign Stable UUID"]
    SKIP_DB --> NEXT_CANDIDATE{More<br/>Candidates?}
    IDENTITY --> NEXT_CANDIDATE
    NEXT_CANDIDATE -->|yes| INSPECT_LOOP
    NEXT_CANDIDATE -->|no| DB_LOOP["For Each Identified<br/>Source Database"]
    
    DB_LOOP --> GET_CURSOR["GetCursor<br/>from State Tracker"]
    GET_CURSOR --> READ["ReadRecords<br/>since Cursor"]
    
    READ --> HAS_RECORDS{"Records<br/>Found?"}
    
    HAS_RECORDS -->|yes| CONVERT["Map to<br/>Ingest Records"]
    CONVERT --> POST["POST /ingest<br/>to Gateway"]
    
    POST --> RETRY_CHECK{"Success?"}
    RETRY_CHECK -->|"no: 5xx or conn error"| RETRY_BACKOFF["Exponential Backoff<br/>1s → 2s → 4s → 30s<br/>±25% Jitter"]
    RETRY_BACKOFF --> RETRY_MAX{"Max Retries<br/>(3)?"}
    RETRY_MAX -->|no| POST
    RETRY_MAX -->|yes| FAIL_LOG["Log Error<br/>Cursor NOT Updated"]
    RETRY_CHECK -->|yes (2xx)| UPDATE_CURSOR["SetCursor<br/>to Max OccurredAt<br/>Record Last Success"]
    RETRY_CHECK -->|"no: 4xx"| FAIL_LOG
    FAIL_LOG --> NEXT_DB{More<br/>Databases?}
    UPDATE_CURSOR --> NEXT_DB
    
    HAS_RECORDS -->|no| HEARTBEAT_CHECK{"Prior Success<br/>& Heartbeat<br/>Interval Elapsed?"}
    HEARTBEAT_CHECK -->|no| NEXT_DB
    HEARTBEAT_CHECK -->|yes| SEND_HB["POST Empty Batch<br/>(Heartbeat)"]
    SEND_HB --> HB_RESULT{"Success?"}
    HB_RESULT -->|yes| HB_OK["Update Last Success"]
    HB_RESULT -->|no| HB_WARN["Log Warning"]
    HB_OK --> NEXT_DB
    HB_WARN --> NEXT_DB
    
    NEXT_DB -->|yes| DB_LOOP
    NEXT_DB -->|no| TICKER
    
    style START fill:#4a90d9,color:#fff
    style UPDATE_CURSOR fill:#27ae60,color:#fff
    style FAIL_LOG fill:#e74c3c,color:#fff
    style RETRY_BACKOFF fill:#f39c12,color:#fff
    style SEND_HB fill:#9b59b6,color:#fff
```

**Key to diagram colors:**
- **Blue** — Startup and initialization
- **Green** — Successful cursor advancement (progress checkpoint)
- **Red** — Error paths where the cursor is NOT advanced (safe retry on next cycle)
- **Orange** — Retry backoff loop (transient failures)
- **Purple** — Heartbeat path (idle Source Database still alive)

## Push Cycle Lifecycle

Each push cycle (one `iterate()` call) runs every **Poll Interval** (default: 60 seconds). Below is a step-by-step walkthrough.

### Phase 1: Discovery and Identity

1. **Database Discovery** — The collector scans the configured SQLite directory (or uses a single explicit path) for `*.db` files. The default directory is platform-specific (`~/.local/share/opencode/` on Linux, `%APPDATA%/OpenCode/` on Windows). This scan is performed every cycle, so newly created databases are picked up automatically.

2. **Open and Inspect** — Each candidate file is opened in read-only mode and validated:
   - Checks that the file is a valid SQLite database.
   - Verifies the `message` and `session` tables exist.
   - Reads row counts and schema version metadata.
   - Files that fail inspection are skipped with a warning — they do not halt the cycle.

3. **Identity Resolution** — For each valid Source Database, `GetOrCreateIdentity` assigns a stable UUID v4. The UUID is persisted on disk under `.collector-id/` (keyed by a SHA-256 hash of the absolute database path) so the same database gets the same identity across collector restarts.

### Phase 2: Incremental Reading

4. **Cursor Lookup** — The collector retrieves the last-sent `OccurredAt` timestamp for the database from the state tracker (`.collector-state` file). On a first-ever run, this returns a zero time, meaning all records will be backfilled.

5. **Read Records** — The SQLite reader runs a prepared query joining `message` and `session` tables, filtering for `message.time_updated > cursor` and only assistant messages that contain `tokens.input` in their `JSON` data blob. Records are returned sorted by `time_updated` ascending, limited to the batch limit (default: 500).

### Phase 3: Ingestion

6. **Batch Construction** — Each raw record is mapped to the canonical wire format defined by the Gateway's `/ingest` schema: `source_record_id`, `session_id`, `model`, `provider`, token breakdown, cost, and timestamp. The entire batch carries the **Schema Version**, **Collector Version**, **Client Hostname**, and **Source Database ID**.

7. **POST to Gateway** — The Ingest Batch is POSTed to `{GATEWAY_BASE_URL}/ingest` with the bearer **Collector Token** in the `Authorization` header. The Gateway applies first-write-wins deduplication using the Idempotency Key.

8. **Retry Logic** — On connection errors or 5xx responses, the collector retries up to 3 times with exponential backoff (starting at 1 second, doubling to 30 seconds max, with ±25% jitter to avoid thundering herd). 4xx responses are NOT retried — they indicate a configuration or data problem.

9. **Cursor Update** — Only after a successful 2xx response is the cursor advanced. The new cursor is set to the maximum `OccurredAt` timestamp among the sent records. This ensures that failed sends are retried on the next cycle — no records are skipped or lost.

### Phase 4: Heartbeat

10. **Heartbeat Check** — If no new records were found for a Source Database, the collector checks two conditions:
    - Has at least one prior successful POST been recorded for this database? (Prevents backfilling with heartbeats.)
    - Has the **Heartbeat Interval** (default: 120 seconds) elapsed since the last success?

11. **Heartbeat Send** — If both conditions are met, the collector POSTs an empty Ingest Batch (zero records) to the Gateway. This updates the Source Database's `last_seen_at` timestamp on the Gateway, confirming the collector and database are alive, without inserting any usage rows.

## ADR References

The collector's design is grounded in five architectural decisions. Each ADR contributes to the workflow as described below.

### ADR-0001: Use Go for Collector

The collector is implemented in Go, compiled to a **single static binary** with no runtime dependencies. It uses `modernc.org/sqlite` for CGO-free SQLite access, enabling cross-compilation from a single build machine to Linux, macOS, and Windows. This means zero installation steps beyond placing the binary — there is no JVM, Python interpreter, or Node runtime required on the target host.

> **Workflow impact:** The entire discovery, reading, and ingestion pipeline runs in-process with no subprocess or external service coordination. The binary is lightweight enough to run as a background daemon alongside any OpenCode installation.

### ADR-0002: Per-Message Usage Ingestion

Each assistant message in an OpenCode Source Database produces **exactly one Usage Record** (user messages are ignored). The record is derived from the `message.data` JSON blob, which contains the most complete and reliable per-call token counts and cost breakdown. Session-level aggregation is deferred to the Gateway, where it can be computed at query time — preserving full per-message granularity for time-series, per-model, and per-provider reporting.

The canonical record shape includes: `source_record_id` (the message UUID), `session_id`, `model`, `provider`, `mode`, `input_tokens`, `output_tokens`, `cached_tokens` (cache read + cache write), `estimated_cost_usd`, and `reported_at` (ISO 8601 timestamp).

> **Workflow impact:** The Idempotency Key `(client_id, source_database_id, message.id)` is naturally stable because each message has a unique ID. The Gateway's first-write-wins deduplication ensures that if a batch is retried after a partial success, already-accepted records are silently ignored.

### ADR-0003: Collector Bearer Token Auth

Authentication to the Gateway uses a **pre-provisioned bearer token** supplied via the `GATEWAY_COLLECTOR_TOKEN` environment variable. The Gateway stores tokens as SHA-256 hashes in a `collector_credentials` table and performs a hash lookup on each request. There are no sessions to manage, no secrets in code or state files, and token rotation requires only an environment variable update and collector restart.

> **Workflow impact:** The token is loaded once at startup and attached to every `Authorization: Bearer` header. Because the token is never logged, credentials are safe from accidental exposure in logs. The Gateway's per-collector client identity derives from the token lookup, which feeds into the Idempotency Key's `client_id` component.

### ADR-0004: Cursor-Based Incremental Reading

The collector persists a **Cursor** per Source Database — a timestamp representing the last `message.time_updated` value that was successfully sent to the Gateway. Cursors are stored in a JSON state file (`.collector-state`) keyed by a SHA-256 hash of the absolute database path.

Benefits of the cursor approach:
- **Incremental reads**: Only new records since the cursor are read each cycle, keeping scans fast.
- **Restart-safe**: The cursor survives collector restarts — after a crash or deliberate shutdown, the collector resumes where it left off.
- **Backfill on first run**: A new Source Database starts with a zero-time cursor, so all historical messages are backfilled in the first cycle (batched, up to 500 per cycle).
- **No duplicates**: The cursor only advances after a successful Gateway POST. If the Gateway is unreachable or returns an error, the same records are retried on the next cycle.

> **Workflow impact:** The cursor is the core correctness mechanism. It is never advanced on failure, and the read query uses `time_updated > cursor` (strictly greater than), preventing gaps or double-counting.

### ADR-0005: Client Hostname in Payload

Every Ingest Batch includes a `client_hostname` field containing the machine's hostname, resolved once at startup via `os.Hostname()`. This is **separate from `client_id`** — `client_id` is a stable instance identifier used in the Idempotency Key, while `client_hostname` is a human-readable machine name that allows operators to trace records back to a specific host without cross-referencing IP addresses.

Multiple collectors may share the same `client_id` if deployed from the same configuration (e.g., in a containerized environment). The hostname provides the additional dimension needed to distinguish which instance generated a given record.

> **Workflow impact:** The hostname is injected into every `IngestRequest` by the Gateway client's `SendBatch` method. It is resolved once in `NewCollector` and reused for the lifetime of the process. If hostname resolution fails at startup, the collector exits with an error — it will not run without identifying itself.

## Domain Language

This document uses the following terms consistently, as defined in [CONTEXT.md](../CONTEXT.md):

| Term | Definition |
|------|------------|
| **Collector** | The long-lived daemon process running on each host that reads Source Databases and pushes Usage Records to the Gateway. |
| **Gateway** | The central observability service that ingests, deduplicates, stores, and reports usage telemetry. |
| **Source Database** | A local OpenCode SQLite `.db` file containing sessions, messages, and usage data. |
| **Usage Record** | A single normalized record derived from one assistant `message.data` usage JSON blob. |
| **Ingest Batch** | A set of Usage Records POSTed to the Gateway's `/ingest` endpoint in a single HTTP request. |
| **Heartbeat** | An empty Ingest Batch signaling the collector is alive. |
| **Cursor** | A persisted timestamp indicating the last processed `message.time_updated` value for a Source Database. |
| **Canonical Record** | The single authoritative shape for a Usage Record, defined in ADR-0002. |
| **Client Hostname** | The machine hostname attached to each Usage Record for operational visibility. |
| **Collector Token** | The pre-provisioned bearer token used to authenticate to the Gateway. |
| **Idempotency Key** | The tuple `(client_id, source_database_id, source_record_id)` that uniquely identifies a Usage Record. |

## Code Structure Map

The collector's `internal/` packages map to each phase of the workflow as follows:

```
internal/
├── config/         → Phase 0 (Startup)
│                     Loads configuration from environment variables:
│                     GATEWAY_COLLECTOR_TOKEN, GATEWAY_BASE_URL,
│                     poll interval, heartbeat interval, SQLite paths,
│                     log level, cursor directory.
│
├── sqlite/         → Phase 1 (Discovery & Reading)
│   ├── discovery.go → DiscoverDatabases — scans directories for *.db files
│   │                  OpenAndInspect — validates SQLite schema & table presence
│   ├── reader.go    → OpenCodeReader — cursor-based incremental reads
│   │                  ReadRecords — parameterized query with cursor and limit
│   └── types.go     → UsageRecord, DatabaseInfo types
│
├── identity/       → Phase 1 (Identity)
│   └── identity.go  → Store.GetOrCreateIdentity — assigns stable UUID v4
│                      per Source Database, persisted in .collector-id/
│
├── state/          → Phase 2 (Cursor Persistence)
│   └── tracker.go   → Tracker.GetCursor / SetCursor — reads and writes
│                      per-database cursors in .collector-state JSON file
│
├── gateway/        → Phase 3 (Ingestion)
│   ├── client.go    → Client.SendBatch — HTTP POST to /ingest with
│   │                  exponential backoff retry logic (1s → 2s → 4s → 30s)
│   │                  Attaches Client Hostname to every request
│   └── types.go     → UsageRecord, IngestRecord, IngestRequest,
│                      IngestResponse, BatchResult types
│
├── heartbeat/      → Phase 4 (Heartbeat)
│   └── heartbeat.go → BuildHeartbeat — constructs an empty IngestRequest
│                      for idle Source Databases
│
└── collector/      → Orchestration (All Phases)
    └── collector.go → NewCollector — wires all components together
                       Run — main ticker loop (poll interval)
                       iterate — one full scan-and-push cycle
                       resolveDatabases — discovery + inspection + identity
                       processDatabase — cursor read → send or heartbeat
                       sendRecords — convert → POST → advance cursor
                       maybeSendHeartbeat — heartbeat eligibility check
```

The entry point `cmd/opencode-collector/main.go` loads configuration via `config.Load()`, creates a `Collector` via `collector.NewCollector()`, and calls `Run()` with OS signal handling for graceful shutdown.

## Key Invariants

The collector's design enforces several critical invariants:

1. **Cursor only advances on confirmed success.** If the Gateway POST fails for any reason (network error, 5xx, timeout), the cursor is NOT updated. The same records will be retried on the next cycle. This prevents data loss at the cost of potential re-sends, which are handled by Gateway-side idempotency.

2. **Heartbeats never precede the first successful POST.** A Source Database must have at least one successful record send before heartbeats begin. This prevents a new, never-before-sent database from flooding the Gateway with empty batches during backfill.

3. **Records are read strictly AFTER the cursor.** The read query uses `time_updated > cursor` (strict inequality), ensuring no record is double-counted even if its timestamp equals the cursor value.

4. **Graceful shutdown drains in-flight requests.** The collector uses `context.WithoutCancel(ctx)` for the operation context while honoring `ctx.Done()` for the main loop, allowing in-flight POSTs to complete during shutdown without aborting mid-request.

5. **Failed databases do not halt the cycle.** If a single Source Database fails inspection, identity resolution, or reading, the error is logged and the collector moves on to the next database. Other databases continue to be processed normally.

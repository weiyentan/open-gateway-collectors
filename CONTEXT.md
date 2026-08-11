# OpenCode Gateway Collectors — Domain Language

*Lightweight collectors that read OpenCode SQLite databases and push usage telemetry to the Gateway.*

**Foreword:** This document captures the domain language used across this project and related projects (opencode-gateway). New work should use these terms consistently.

## Language

**Collector** — A lightweight per-host process that reads local OpenCode SQLite databases and pushes usage records to the Gateway. Runs as a long-lived daemon with a periodic push loop.

**Gateway** — The central OpenCode observability service that ingests, deduplicates, stores, and reports usage telemetry. *Avoid: "backend", "server"*

**Source Database** — A local OpenCode SQLite `.db` file containing sessions, messages, and usage data. Each source database is identified by a stable UUID generated and persisted by the collector.

**Usage Record** — A single normalized record derived from one assistant `message.data` usage JSON blob. Contains tokens, cost, model, provider, and timestamps. *Avoid: "usage event", "telemetry point"*

**Session Context** — Descriptive metadata read from an OpenCode `session` row in a Source Database. Includes facts such as title, agent, external project ID, parent external session ID, workspace ID, and model. It is read-only telemetry forwarded to the Gateway; the Collector must not write it back to the Source Database. *Avoid: "session usage", "collector enrichment"*

**Project Snapshot** — A read-only snapshot of OpenCode project metadata (title, worktree path) read from the `project` table in a Source Database. Forwarded alongside usage records in the ingest batch for Gateway agent-run reporting. *Avoid: "project context", "project enrichment"*

**Project Directory Snapshot** — A read-only mapping from a project to a directory path, read from the optional `project_directory` table. Forwarded alongside usage records in the ingest batch. Blank or whitespace-only paths — including NULL `project_directory.path` rows that surface as empty strings — are filtered out client-side before the ingest batch is built, so the Gateway never receives an empty `directory` value (which it rejects with HTTP 422). The Source Database is never modified. *Avoid: "project directory context"*

**Todo Snapshot** — The latest observed set of OpenCode `todo` rows for a Session in a Source Database. It is read-only telemetry forwarded to the Gateway for agent run reporting. *Avoid: "todo events", "task timeline"*

**Agent Run Summary** — A Gateway-facing summary of what happened during an OpenCode agent or subagent session, composed from Usage Records, Session Context, Todo Snapshots, project snapshots, and parent/child session relationships. It is not a transcript or event replay.

**Ingest Batch** — A set of usage records POSTed to the Gateway's `/ingest` endpoint in a single HTTP request. May be empty (heartbeat).

**Heartbeat** — An empty ingest batch that communicates the collector is alive. Updates the source database's `last_seen_at` timestamp on the Gateway without inserting usage rows.

**Cursor** — A persisted timestamp indicating the last processed `message.time_updated` value for a source database. Enables incremental reads across collector restarts. *Avoid: "checkpoint", "watermark"*

**Replay** — An explicit collector mode that re-reads Source Database history past the Cursor and re-sends records and projections through the normal ingest pipeline. Used to backfill fields that were dropped or misnamed by an older collector version; the Gateway's idempotent upserts make re-sending safe. Replay runs within a bounded window: the lower bound (`since`, strict) is the replay start — full history when unset, or `time.Now().Add(-duration)` for a configured `GATEWAY_COLLECTOR_REPLAY_SINCE` Go duration — never the stored Cursor, which is used only as a clamp so the final Cursor never regresses below it. Records whose `time_updated` equals the lower bound are excluded; only records strictly newer are re-read. The optional upper bound (`until`, inclusive) includes records with `time_updated <= until`, with an unset `until` meaning no upper bound; `until` must be a strict RFC3339 timestamp (e.g. `2026-08-11T12:00:00Z`; UTC recommended) and an invalid value fails startup. Replay never runs without an explicit trigger (the `-replay` CLI flag or `GATEWAY_COLLECTOR_REPLAY=true`). The Cursor advances only after Replay completes, clamped to `until` and never regressing below the pre-replay stored Cursor; on a failed batch the Cursor is rewound to the replay start so the window is re-read, and normal incremental runs resume from the Cursor once the window is exhausted. Under `GATEWAY_COLLECTOR_TRANSPORT=kafka`, the batch is produced before the Gateway validates it, so a 422 moves the message to the consumer DLQ and the Cursor has already advanced — run Replay after deploying the fix to backfill. Under the default `http` transport, a 422 is non-retryable, the Cursor is not advanced, and the batch is re-sent by normal incremental reads (no DLQ, no Replay needed). *Avoid: "rebuild", "resync"*

**Canonical Record** — The single authoritative shape for a usage record, derived from the OpenCode assistant `message.data` JSON. Defined in ADR-0002.

**Client Hostname** — The machine hostname (`os.Hostname()`) attached to each usage record for operational visibility. Resolved once at collector startup. Distinguished from `client_id` which is a stable instance identifier used in the idempotency key tuple. Allows operators to identify which machine generated a record without relying on IP addresses.

**Collector Token** — A pre-provisioned bearer token used by the collector to authenticate to the Gateway. SHA-256 hashed server-side. Must pass the Gateway's **Two-Layer Auth**: (1) match `GATEWAY_API_KEY` in the `ApiKeyMiddleware`, and (2) have its hash registered in the Gateway's `collector_credentials` table (see Gateway ADR-0007). Provisioned via the Gateway's `/admin/clients/{id}/tokens` endpoint, or bootstrapped by registering the Admin API Key's hash directly.

**Idempotency Key** — The tuple `(client_id, source_database_id, source_record_id)` that uniquely identifies a usage record. The Gateway applies first-write-wins semantics to prevent duplicates.

## Relationships

- A **Collector** manages **0..N Source Databases**, each with its own **Identity**.
- A **Source Database** contains **0..N Sessions**, each containing **0..N Messages**.
- An assistant **Message** produces **0..1 Usage Records** (user messages have none).
- A **Session** may provide **0..1 Session Context** snapshots for Gateway reporting.
- **Session Context** is sent as a separate batch-level collection, not duplicated onto each **Usage Record**.
- A **Source Database** may contain **0..N Projects**, each providing **0..1 Project Snapshot** for Gateway reporting.
- **Project Snapshots** are sent as a separate batch-level collection, not duplicated onto each **Usage Record**.
- A **Project** may have **0..N Project Directory Snapshots** for Gateway reporting.
- **Project Directory Snapshots** are sent as a separate batch-level collection, not duplicated onto each **Usage Record**.
- A **Session** may provide **0..N Todo Snapshot** items for Gateway reporting.
- **Todo Snapshots** are sent as a separate batch-level collection, not duplicated onto each **Usage Record**.
- A **Usage Record** is sent in an **Ingest Batch** to the **Gateway**.
- An **Ingest Batch** is identified by a **Batch ID** (UUID) returned by the Gateway.
- The **Idempotency Key** spans **Collector** (via client identity) → **Source Database** → **Usage Record**.
- A **Replay** window is bounded by an explicit **since** time (strict lower bound, never the stored **Cursor** — the stored **Cursor** is only a clamp so the final **Cursor** never regresses) and an optional **until** timestamp (inclusive upper bound); the **Cursor** advances only after **Replay** completes.

## Example dialogue

**Dev:** The collector found a new `.db` file in the scan directory. What happens?

**Domain:** First, `OpenAndInspect` verifies it's a real OpenCode database by checking for `message` and `session` tables. If it passes, `GetOrCreateIdentity` generates a UUID and persists it as the source database's stable identity. The collector adds it to the iteration list. On the next push cycle, the cursor starts from zero (never-before-sent), so all messages with usage data are backfilled.

**Dev:** What if the Gateway is down during a push cycle?

**Domain:** The batch fails with a 5xx or connection error. The collector retries with exponential backoff (1s → 2s → 4s → max 30s). The cursor is NOT updated — the same records will be retried on the next cycle. The heartbeat doesn't fire until at least one successful POST has happened (to avoid backfilling with heartbeats).

**Dev:** How do we handle schema changes in OpenCode's SQLite?

**Domain:** The `OpenAndInspect` function checks for the expected columns. If a future OpenCode version changes the schema, the inspection fails gracefully — the collector logs a warning and skips that database. A new collector release would update the expected schema and add a migration path for the cursor.

## Flagged ambiguities

- **"Database" vs "Source Database":** Always use "Source Database" to distinguish the local OpenCode SQLite file from the Gateway's PostgreSQL. Never say "database" alone in domain discussion.
- **"Record" vs "Usage Record":** Use "Usage Record" on first mention, then "record" is unambiguous within a usage context.

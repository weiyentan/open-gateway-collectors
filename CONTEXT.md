# OpenCode Gateway Collectors — Domain Language

*Lightweight collectors that read OpenCode SQLite databases and push usage telemetry to the Gateway.*

**Foreword:** This document captures the domain language used across this project and related projects (opencode-gateway). New work should use these terms consistently.

## Language

**Collector** — A lightweight per-host process that reads local OpenCode SQLite databases and pushes usage records to the Gateway. Runs as a long-lived daemon with a periodic push loop.

**Gateway** — The central OpenCode observability service that ingests, deduplicates, stores, and reports usage telemetry. *Avoid: "backend", "server"*

**Source Database** — A local OpenCode SQLite `.db` file containing sessions, messages, and usage data. Each source database is identified by a stable UUID generated and persisted by the collector.

**Usage Record** — A single normalized record derived from one assistant `message.data` usage JSON blob. Contains tokens, cost, model, provider, and timestamps. *Avoid: "usage event", "telemetry point"*

**Ingest Batch** — A set of usage records POSTed to the Gateway's `/ingest` endpoint in a single HTTP request. May be empty (heartbeat).

**Heartbeat** — An empty ingest batch that communicates the collector is alive. Updates the source database's `last_seen_at` timestamp on the Gateway without inserting usage rows.

**Cursor** — A persisted timestamp indicating the last processed `message.time_updated` value for a source database. Enables incremental reads across collector restarts. *Avoid: "checkpoint", "watermark"*

**Canonical Record** — The single authoritative shape for a usage record, derived from the OpenCode assistant `message.data` JSON. Defined in ADR-0002.

**Collector Token** — A pre-provisioned bearer token used by the collector to authenticate to the Gateway. SHA-256 hashed server-side. Provisioned via the Gateway's `/admin/clients/{id}/tokens` endpoint.

**Idempotency Key** — The tuple `(client_id, source_database_id, source_record_id)` that uniquely identifies a usage record. The Gateway applies first-write-wins semantics to prevent duplicates.

## Relationships

- A **Collector** manages **0..N Source Databases**, each with its own **Identity**.
- A **Source Database** contains **0..N Sessions**, each containing **0..N Messages**.
- An assistant **Message** produces **0..1 Usage Records** (user messages have none).
- A **Usage Record** is sent in an **Ingest Batch** to the **Gateway**.
- An **Ingest Batch** is identified by a **Batch ID** (UUID) returned by the Gateway.
- The **Idempotency Key** spans **Collector** (via client identity) → **Source Database** → **Usage Record**.

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

# ADR-0002: Per-message usage ingestion from OpenCode SQLite

**Status:** Accepted  
**Date:** 2026-07-18  
**Context:** Issue opencode-gateway #198 discovered that OpenCode stores LLM usage data in assistant `message.data` JSON. Session-level aggregates (`session.cost`, `session.tokens_*`) also exist but are derived from messages. The Gateway's `POST /ingest` endpoint accepts batches of individual usage records.

**Decision:** Ingest one normalized usage record per assistant message with usage data.

**Rationale:**
- Message-level records preserve full granularity for time-series and per-model reporting
- Session aggregates can be computed query-time on the Gateway side
- The `message.data` JSON contains the most complete and reliable cost/token breakdown
- The Gateway's ingest contract expects per-record payloads with `source_record_id = message.id`
- First-write-wins idempotency on `(client_id, source_database_id, message.id)` is natural and stable
- User messages and non-usage-bearing messages are cleanly excluded by checking `tokens.input`

**Canonical mapping (per-message):**
- `source_record_id` = `message.id`
- `session_id` = `message.session_id`
- `model` = `message.data.modelID`
- `input_tokens` = `message.data.tokens.input`
- `output_tokens` = `message.data.tokens.output`
- `cached_tokens` = `message.data.tokens.cache.read` + `message.data.tokens.cache.write`
- `estimated_cost_usd` = `message.data.cost`
- `reported_at` = `message.data.time.completed`
- Additional context stored in traversal fields: `providerID`, `agent`, `mode`, `finish_reason`

**Consequences:**
- Requires JOIN between `message` and `session` tables
- Privacy boundary: never transmit `message.text`, `part.data`, or raw `message.data` blob
- Batch size limited to avoid oversized payloads (default 500 records)
- Cursor tracking on `message.time_updated` for incremental reads

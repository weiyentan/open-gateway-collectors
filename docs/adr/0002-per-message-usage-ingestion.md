# ADR-0002: Per-message usage ingestion

We ingest one normalized usage record per assistant OpenCode message (rather than per session) because the wayfinder research in opencode-gateway #198 confirmed that assistant `message.data` JSON contains the most complete and reliable per-call token and cost breakdown, and session-level aggregates can be computed query-time on the Gateway side — preserving full granularity for time-series and per-model reporting while keeping the idempotency key `(client_id, source_database_id, message.id)` naturally stable.

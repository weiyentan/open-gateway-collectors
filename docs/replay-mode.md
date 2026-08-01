# Replay Mode

Replay is an explicitly-triggered, one-shot operational mode for the collector. When enabled, the collector ignores the stored Cursor and forces a re-read of Source Database history, re-sending the re-derived **Usage Records** together with their batch-level projection snapshots — **Session Context**, **Project Snapshot**, **Project Directory Snapshot**, and **Todo Snapshot** — through the normal ingest pipeline. This document is the operational guide; the design rationale lives in [ADR-0008](adr/0008-replay-through-pipeline.md).

## Purpose

The collector normally reads incrementally. Each push cycle reads only records newer than the persisted Cursor, and the Cursor advances after every successful Ingest Batch, so once history is sent it is never re-read. A wire-format bug in an older collector version — for example, a field that was dropped or misnamed on the wire — therefore leaves the affected historical records permanently wrong in the Gateway, with no incremental mechanism to correct them.

Replay fixes that. It re-reads Source Database history past the stored Cursor and re-sends the records and projection snapshots through the same validated path used during normal operation (read → map → transport → ingest upsert). Because the re-send exercises exactly what production exercises, it automatically benefits from the field-mapping fix shipped in the same release, and the Gateway's idempotent upserts make re-sending safe.

Once the replay pass completes, the Cursor advances so normal incremental runs resume.

## When to use it

Use replay after deploying a collector fix that changes the wire format of already-sent records — for example, a field that was dropped, misnamed, or mapped to the wrong destination in an earlier version. Replay re-derives those records from the Source Databases and re-sends them so the Gateway stores the corrected shape.

**Prerequisites and trade-offs (ADR-0008):**

- **Deploy the Gateway-side fix first.** Replay re-sends data through the same ingest pipeline; if the Gateway still applies the old mapping, the re-sent data is dropped again. Deploy the Gateway fix, then run replay.
- **Re-sends are idempotent.** The Gateway deduplicates on the Idempotency Key `(client_id, source_database_id, source_record_id)` with first-write-wins semantics, so re-sending records that were already accepted is safe — no duplicates are inserted.
- **Replay re-sends more than strictly necessary.** The replay pass reads and re-sends the entire window (everything past the Cursor by default). Bound the window with `GATEWAY_COLLECTOR_REPLAY_SINCE` when you only need a recent range.
- **Replay never runs without an explicit trigger.** It is not part of normal operation; it only runs when the flag or environment variable is set.

## How to trigger it

Replay is enabled per process start by any of the following:

| Trigger | Description |
|---------|-------------|
| `-replay` CLI flag | Force-enables replay. It can only turn replay **on** — it cannot disable a replay already enabled via `GATEWAY_COLLECTOR_REPLAY=true`. |
| `GATEWAY_COLLECTOR_REPLAY=true` | Environment variable equivalent. Any truthy value enables replay. |
| `GATEWAY_COLLECTOR_REPLAY_SINCE` | Optional Go duration string (e.g. `720h`, `30m`) bounding the replay window to records newer than `time.Now().Add(-duration)`. Default `0` means full history. Only consulted when replay is enabled. |

Examples:

```bash
# Replay full history for all Source Databases.
./bin/opencode-collector -replay

# Replay the last 30 days, via environment variables.
GATEWAY_COLLECTOR_REPLAY=true \
GATEWAY_COLLECTOR_REPLAY_SINCE=720h \
./bin/opencode-collector

# Replay a small window for a quick verification run.
GATEWAY_COLLECTOR_REPLAY=true \
GATEWAY_COLLECTOR_REPLAY_SINCE=30m \
./bin/opencode-collector
```

## Semantics

This section documents the implemented behavior of a replay pass, verified against the collector source.

### One-shot per process lifetime

Replay runs at most once per Source Database per process lifetime. A per-Source-Database latch (`replayCompleted`) is set when the pass completes; on subsequent poll cycles the replay pass is skipped even if the flag or environment variable is still set, and normal incremental reads resume. Restarting the collector clears the latch, so a restart with replay still enabled triggers it again.

### Batch paging

Replay reads history in batches up to the configured batch limit (`GATEWAY_COLLECTOR_BATCH_LIMIT`) and pages across them. Paging is tie-safe: each page advances on a composite key `(time_updated, id)` — the last record's timestamp **and** its ID — so records that share a timestamp with the final record of a page are read strictly after it on the next page. No record is dropped at a batch boundary, even when many records share the same `time_updated`.

### Cursor persistence

During a replay pass the persisted Cursor is **not** advanced per batch; cursor persistence is deferred. The Cursor is persisted only once the full pass completes:

- If the pass ends with no more records, the Cursor is set to the effective replay start (the last batch's max `time_updated`, or `replaySince` if no batch was sent).
- If the final batch was short of the batch limit, the Cursor is set to the final batch's max `time_updated`.

In both cases the Cursor is clamped so that:

- it **never advances past** records in `(storedCursor, replaySince]` that were neither previously ingested nor re-read by the replay — when the replay window starts after the stored Cursor, the Cursor stays at the stored Cursor so those records remain readable by normal incremental mode; and
- it **never regresses below** the stored Cursor.

### Failure handling

If a batch POST fails mid-replay:

- the pass aborts immediately;
- the read window does **not** advance — the failed records are retried on the next attempt;
- the persisted Cursor is rewound (to `replaySince`, clamped to the stored Cursor when the replay window starts after it) so a subsequent restart — even without replay — re-reads the window;
- the one-shot latch is **not** set, so replay re-enters and retries the failed batch on the next poll cycle;
- if the rewind write itself fails, an error is logged — the persisted Cursor then stays at its pre-replay position, which may be ahead of the failed batch, so an operator must intervene before records are permanently skipped.

Rewinding is safe: the earliest batches that already succeeded are re-sent idempotently and ignored by the Gateway.

### Heartbeat

A Source Database with no records still gets a **Heartbeat** on the replay pass (gated by the usual prior-success and heartbeat-interval conditions), so its `last_seen_at` stays fresh on the Gateway even when the replay window is empty.

### Configuration validation

A negative `GATEWAY_COLLECTOR_REPLAY_SINCE` is rejected at startup with `GATEWAY_COLLECTOR_REPLAY_SINCE must not be negative`.

## Operational notes / troubleshooting

- **Verify the Gateway is reachable before replaying a large window.** A replay pass can re-send many batches in quick succession; confirm `GET /health` and the Collector Token are working first.
- **Bound the window when possible.** Replay re-sends more data than strictly necessary, so set `GATEWAY_COLLECTOR_REPLAY_SINCE` to the smallest window that covers the affected records.
- **Replay without an explicit trigger never runs.** If you expect a replay and nothing happens, confirm `GATEWAY_COLLECTOR_REPLAY` is truthy and the `-replay` flag (or environment variable) is actually present — the flag cannot turn an environment-enabled replay off.
- **After replay, operation is incremental again.** The Cursor advances on completion, so subsequent runs resume normal incremental reads and do not re-run the pass.
- **Inspect the logs.** At startup the collector logs `replay mode enabled` (with the effective `replay_since` when a window is set); per Source Database it logs `replay active` and `replay complete for database`. On a mid-pass failure it logs a rewind error — watch for it, because a failed rewind can mean a restart without replay would skip the failed batch.

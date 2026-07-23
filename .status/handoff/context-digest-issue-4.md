# Context Digest — Issue #4

## Issue Title
Gateway HTTP client — auth, POST /ingest, retry

## Summary
Implement the HTTP client that communicates with the Gateway's POST /ingest endpoint. This is the collector's primary outbound path — it sends usage record batches to the Gateway and handles the response. Includes type definitions for the ingest API shape (IngestRecord, IngestRequest, IngestResponse, BatchResult), the Client struct with SendBatch() method, retry logic with exponential backoff and jitter, and a MapToIngestRecord converter function. Tests use httptest.NewServer to mock the Gateway.

## Labels
- `afk` — suitable for autonomous AFK implementation

## Expected Changed Files / Paths
| File | Purpose |
|------|---------|
| `internal/gateway/types.go` | IngestRecord, IngestRequest, BatchResult, IngestResponse structs |
| `internal/gateway/client.go` | Client struct with constructor (NewClient), SendBatch method, retry logic, MapToIngestRecord converter |
| `internal/gateway/client_test.go` | Tests with httptest.NewServer mock Gateway — success, partial success, retry, 4xx-stop, timeout/cancellation |

## Dependency Layer
**Layer 1** — Blocked by:
- Issue #1 (Foundation — needs config for base URL and token)

## Routing
| Field | Value |
|-------|-------|
| Tier | **T2** — single package (internal/gateway/), follows established Go patterns, clear mechanical specification |
| required_skill | *none* (no `skill_hints` in `.opencode-workflow.yaml`) |
| review_mode | auto |
| escalation_path | stop |

## Flags
| Flag | Value |
|------|-------|
| user_facing | false |
| docs_impact | false |
| breaking_change | false |
| estimated_complexity | medium |

## Notes
- Uses `Authorization: Bearer <collector_token>` header — token passed into constructor, never logged.
- Retry on: connection errors, 5xx, timeout. Do NOT retry on: 4xx, context cancellation.
- Exponential backoff: 1s → 2s → 4s → 8s → max 30s (configurable via GATEWAY_COLLECTOR_MAX_RETRY_INTERVAL/MAX_RETRIES env vars from issue #1).
- Jitter: ±25% random per backoff interval via `rand.Float64()`.
- client_hostname is resolved once at construction time via `os.Hostname()` and stored in the Client struct.
- MapToIngestRecord: cached_tokens = TokensCacheRead + TokensCacheWrite; estimated_cost_usd = formatted decimal string or nil; reported_at = ISO8601 from OccurredAt.

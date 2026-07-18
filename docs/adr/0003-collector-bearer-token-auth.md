# ADR-0003: Collector authentication via bearer tokens

The collector authenticates to the Gateway via a pre-provisioned bearer token in the `GATEWAY_COLLECTOR_TOKEN` environment variable because the Gateway's auth model (SHA-256 hashed lookup in `collector_credentials` table) is already designed for this flow and environment variables match the rest of the collector's configuration surface — no session management, no secrets in code or state files, and rotation requires an env var update and restart.

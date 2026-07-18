# ADR-0003: Collector authentication via bearer tokens

**Status:** Accepted  
**Date:** 2026-07-18  
**Context:** The Gateway (PR #216) requires authentication on the `POST /ingest` endpoint via a bearer token validated against the `collector_credentials` table (SHA-256 hashed). Tokens are provisioned through the Gateway's `POST /admin/clients/{id}/tokens` API.

**Decision:** The collector authenticates to the Gateway using a pre-provisioned bearer token passed via environment variable.

**Rationale:**
- The Gateway's auth model is already designed and implemented for this exact flow
- Bearer tokens are stateless on the client side — no session management needed
- Environment variable (`GATEWAY_COLLECTOR_TOKEN`) follows the same pattern as the rest of the config
- Token is never stored in code, written to logs, or included in state files
- Token is SHA-256 hashed server-side; the raw token is only known to the provisioner and the collector

**Consequences:**
- Token must be provisioned via Gateway admin API before the collector can start
- Token is a single point of failure if compromised — rotation requires updating both the Gateway and the collector's env
- No mutual TLS or client certificates (deferred — can be added later if needed)
- The collector does not implement any token refresh logic; token rotation requires a restart

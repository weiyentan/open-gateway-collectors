// Package gateway implements the HTTP client for communicating with the
// OpenCode Gateway's ingestion endpoint.
package gateway

import "time"

// UsageRecord is a single normalized usage record derived from one assistant
// message.data usage JSON blob. This is the internal representation before
// mapping to the wire format (IngestRecord).
type UsageRecord struct {
	SourceRecordID   string    `json:"source_record_id"`
	SessionID        string    `json:"session_id"`
	Model            string    `json:"model"`
	InputTokens      int64     `json:"input_tokens"`
	OutputTokens     int64     `json:"output_tokens"`
	TokensCacheRead  int64     `json:"tokens_cache_read"`
	TokensCacheWrite int64     `json:"tokens_cache_write"`
	EstimatedCostUSD float64   `json:"estimated_cost_usd"`
	OccurredAt       time.Time `json:"occurred_at"`
}

// IngestRecord is the wire-format usage record sent to the Gateway's
// POST /ingest endpoint. It is derived from UsageRecord via MapToIngestRecord.
type IngestRecord struct {
	SourceRecordID   string  `json:"source_record_id"`
	SessionID        string  `json:"session_id"`
	Model            string  `json:"model"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	CachedTokens     int64   `json:"cached_tokens"`
	EstimatedCostUSD *string `json:"estimated_cost_usd"`
	ReportedAt       string  `json:"reported_at"`
}

// IngestRequest is the full payload sent in a POST /ingest request.
type IngestRequest struct {
	SchemaVersion    string         `json:"schema_version"`
	CollectorVersion string         `json:"collector_version"`
	ClientHostname   string         `json:"client_hostname"`
	SourceDatabaseID string         `json:"source_database_id"`
	Records          []IngestRecord `json:"records"`
}

// BatchResult describes the outcome for a single record in an ingest batch.
type BatchResult struct {
	Index  int    `json:"index"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// IngestResponse is the Gateway's response to a POST /ingest request.
type IngestResponse struct {
	BatchID        string        `json:"batch_id"`
	AcceptedCount  int           `json:"accepted_count"`
	RejectedCount  int           `json:"rejected_count"`
	Results        []BatchResult `json:"results"`
}

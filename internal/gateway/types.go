// Package gateway implements the HTTP client for communicating with the
// OpenCode Gateway's ingestion endpoint.
package gateway

import (
	"time"

	"github.com/opencode-gateway/collectors/internal/sqlite"
)

// UsageRecord is a single normalized usage record derived from one assistant
// message.data usage JSON blob. This is the internal representation before
// mapping to the wire format (IngestRecord).
type UsageRecord struct {
	SourceRecordID   string    `json:"source_record_id"`
	SessionID        string    `json:"session_id"`
	Model            string    `json:"model"`
	ProviderID       string    `json:"provider_id"`
	Mode             string    `json:"mode"`
	Agent            string    `json:"agent"`
	ProjectID        string    `json:"project_id"`
	WorkspaceID      string    `json:"workspace_id"`
	ParentSessionID  string    `json:"parent_session_id"`
	ReasoningTokens  int64     `json:"reasoning_tokens"`
	FinishReason     string    `json:"finish_reason"`
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
	Provider         string  `json:"provider"`
	Mode             string  `json:"mode"`
	Agent            string  `json:"agent"`
	ProjectID        string  `json:"project_id"`
	WorkspaceID      string  `json:"workspace_id"`
	ParentSessionID  string  `json:"parent_session_id"`
	ReasoningTokens  int64   `json:"reasoning_tokens"`
	FinishReason     string  `json:"finish_reason"`
	CacheReadTokens  int64   `json:"cache_read_tokens"`
	CacheWriteTokens int64   `json:"cache_write_tokens"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	CachedTokens     int64   `json:"cached_tokens"`
	EstimatedCostUSD *string `json:"estimated_cost_usd"`
	ReportedAt       string  `json:"reported_at"`
}

// ---------------------------------------------------------------------------
// Batch-level projection snapshot types (wire format)
// ---------------------------------------------------------------------------

// SessionContext is a batch-level snapshot of OpenCode session metadata
// included alongside usage records in the ingest payload. One per distinct
// external session ID in the batch.
type SessionContext struct {
	ExternalSessionID string `json:"external_session_id"`
	Title             string `json:"title,omitempty"`
	Agent             string `json:"agent,omitempty"`
	ProjectID         string `json:"project_id,omitempty"`
	ParentSessionID   string `json:"parent_session_id,omitempty"`
	WorkspaceID       string `json:"workspace_id,omitempty"`
	Model             string `json:"model,omitempty"`
}

// ProjectSnapshot is a batch-level snapshot of OpenCode project metadata
// included alongside usage records. One per distinct project referenced
// by sessions in the batch.
type ProjectSnapshot struct {
	ExternalProjectID string `json:"external_project_id"`
	Name              string `json:"name,omitempty"`
	Worktree          string `json:"worktree,omitempty"`
}

// ProjectDirectorySnapshot is a batch-level snapshot of a project directory
// mapping from the OpenCode source database.
type ProjectDirectorySnapshot struct {
	ExternalProjectID string `json:"external_project_id"`
	Directory         string `json:"directory"`
}

// TodoSnapshot is a batch-level snapshot of an OpenCode todo item for a
// session in the current batch.
type TodoSnapshot struct {
	ExternalSessionID string `json:"external_session_id"`
	Content           string `json:"content"`
	Status            string `json:"status,omitempty"`
}

// IngestRequest is the full payload sent in a POST /ingest request.
type IngestRequest struct {
	SchemaVersion     string                    `json:"schema_version"`
	CollectorVersion  string                    `json:"collector_version"`
	ClientHostname    string                    `json:"client_hostname"`
	SourceDatabaseID  string                    `json:"source_database_id"`
	Records           []IngestRecord            `json:"records"`
	SessionContexts   []SessionContext           `json:"session_contexts,omitempty"`
	Projects          []ProjectSnapshot          `json:"projects,omitempty"`
	ProjectDirectories []ProjectDirectorySnapshot `json:"project_directories,omitempty"`
	SessionTodos      []TodoSnapshot             `json:"session_todos,omitempty"`
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

// ---------------------------------------------------------------------------
// Projection mapping functions — convert sqlite projection data to wire types
// ---------------------------------------------------------------------------

// MapToSessionContext converts a sqlite.SessionContextData to a wire-format
// SessionContext for inclusion in the ingest payload.
func MapToSessionContext(data sqlite.SessionContextData) SessionContext {
	return SessionContext{
		ExternalSessionID: data.ExternalSessionID,
		Title:             data.Title,
		Agent:             data.Agent,
		ProjectID:         data.ProjectID,
		ParentSessionID:   data.ParentSessionID,
		WorkspaceID:       data.WorkspaceID,
		Model:             data.Model,
	}
}

// MapToProjectSnapshot converts a sqlite.ProjectData to a wire-format
// ProjectSnapshot.
func MapToProjectSnapshot(data sqlite.ProjectData) ProjectSnapshot {
	return ProjectSnapshot{
		ExternalProjectID: data.ExternalProjectID,
		Name:              data.Title,
		Worktree:          data.Worktree,
	}
}

// MapToProjectDirectorySnapshot converts a sqlite.ProjectDirectoryData to a
// wire-format ProjectDirectorySnapshot.
func MapToProjectDirectorySnapshot(data sqlite.ProjectDirectoryData) ProjectDirectorySnapshot {
	return ProjectDirectorySnapshot{
		ExternalProjectID: data.ExternalProjectID,
		Directory:         data.Path,
	}
}

// MapToTodoSnapshot converts a sqlite.TodoData to a wire-format TodoSnapshot.
func MapToTodoSnapshot(data sqlite.TodoData) TodoSnapshot {
	return TodoSnapshot{
		ExternalSessionID: data.ExternalSessionID,
		Content:           data.Description,
		Status:            data.Status,
	}
}

// Package sqlite provides source database discovery and inspection for
// OpenCode SQLite databases.
package sqlite

import "time"

// UsageRecord represents a single normalized usage record derived from an
// assistant message.data JSON blob in an OpenCode source database.
type UsageRecord struct {
	// SourceRecordID is the message.id from the source database.
	SourceRecordID string

	// SourceSessionID is the session.id that this message belongs to.
	SourceSessionID string

	// SourceProjectID is the session.project_id, if any.
	SourceProjectID string

	// ParentSessionID is the session.parent_id, if any.
	ParentSessionID string

	// WorkspaceID is the session.workspace_id, if any.
	WorkspaceID string

	// OccurredAt is the message.time_updated converted to a time.Time.
	OccurredAt time.Time

	// MessageCreatedAt is the message.time_created value (Unix ms).
	MessageCreatedAt int64

	// SessionCreatedAt is the session.time_created value (Unix ms).
	SessionCreatedAt int64

	// SessionUpdatedAt is the session.time_updated value (Unix ms).
	SessionUpdatedAt int64

	// Agent is the session.agent identifier.
	Agent string

	// ProviderID is the LLM provider identifier from message.data.
	ProviderID string

	// ModelID is the LLM model identifier from message.data.
	ModelID string

	// Mode is the chat mode from message.data (e.g. "chat", "agent", "edit").
	Mode string

	// FinishReason is the completion reason from message.data (e.g. "stop", "length").
	FinishReason string

	// TokensInput is the number of input/prompt tokens.
	TokensInput int64

	// TokensOutput is the number of output/completion tokens.
	TokensOutput int64

	// TokensReasoning is the number of reasoning tokens.
	TokensReasoning int64

	// TokensCacheRead is the number of cache read tokens.
	TokensCacheRead int64

	// TokensCacheWrite is the number of cache write tokens.
	TokensCacheWrite int64

	// TokensTotal is the total number of tokens.
	TokensTotal int64

	// OpenCodeReportedCost is the cost as reported by OpenCode's message.data.
	OpenCodeReportedCost float64

	// CostCurrency is the currency for the cost value (default "USD").
	CostCurrency string

	// CostSource describes the origin of the cost data.
	CostSource string
}

// ---------------------------------------------------------------------------
// Projection types — read-only snapshots from optional OpenCode SQLite tables
// ---------------------------------------------------------------------------

// SessionContextData holds descriptive metadata read from an OpenCode session
// row. It is read-only telemetry forwarded to the Gateway.
type SessionContextData struct {
	ExternalSessionID string // session.id
	Title             string // session.title or empty string
	Agent             string // session.agent
	ProjectID         string // session.project_id (external project ID)
	ParentSessionID   string // session.parent_id
	WorkspaceID       string // session.workspace_id
	Model             string // session.model
}

// ProjectData holds snapshot data from an OpenCode project row. Fields
// correspond to columns in the project table.
type ProjectData struct {
	ExternalProjectID string // project.id
	Name              string // project.name (or project.title in older databases)
	Worktree          string // project.worktree (path to local checkout)
}

// ProjectDirectoryData holds a single directory mapping from the
// project_directory table.
type ProjectDirectoryData struct {
	ExternalProjectID string // project_directory.project_id
	Path              string // project_directory.path
}

// TodoData holds a single todo item snapshot from the todo table.
type TodoData struct {
	ExternalSessionID string // todo.session_id
	Description       string // todo.description
	Status            string // todo.status (e.g. "pending", "completed")
}

// DatabaseInfo holds metadata about a discovered OpenCode source database.
type DatabaseInfo struct {
	// Path is the absolute filesystem path to the database file.
	Path string

	// Size is the file size in bytes at the time of inspection.
	Size int64

	// LastModified is the file modification timestamp at the time of inspection.
	LastModified time.Time

	// MessageCount is the number of rows in the message table.
	MessageCount int

	// SessionCount is the number of rows in the session table.
	SessionCount int

	// SchemaVersion is a version identifier derived from the schema. Currently
	// set to the SQLite user_version pragma value.
	SchemaVersion string

	// HasProjectTable indicates whether the project table exists in the
	// source database. Optional — missing tables are handled gracefully.
	HasProjectTable bool

	// HasProjectDirectoryTable indicates whether the project_directory table
	// exists. Optional — handled gracefully.
	HasProjectDirectoryTable bool

	// HasTodoTable indicates whether the todo table exists. Optional —
	// handled gracefully.
	HasTodoTable bool

	// ProjectColumns lists the detected column names in the project table.
	ProjectColumns []string

	// ProjectDirectoryColumns lists the detected column names in the
	// project_directory table.
	ProjectDirectoryColumns []string

	// TodoColumns lists the detected column names in the todo table.
	TodoColumns []string

	// SessionColumns lists the detected column names in the session table.
	// Used for schema-aware projection reading (e.g. title column presence).
	SessionColumns []string
}

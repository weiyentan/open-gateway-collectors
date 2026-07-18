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
}

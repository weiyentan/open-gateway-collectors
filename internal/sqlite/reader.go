package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Reader defines the interface for reading usage records from an OpenCode
// source database. Implementations must be safe for concurrent use if the
// underlying database driver supports it.
type Reader interface {
	// ReadRecords returns usage records with message.time_updated strictly
	// greater than since, ordered by time_updated ascending, up to limit
	// records. User messages without tokens.input in message.data are skipped.
	ReadRecords(since time.Time, limit int) ([]UsageRecord, error)
}

// OpenCodeReader reads usage records from an OpenCode SQLite source database.
// It uses a read-only connection with a prepared statement for efficient
// cursor-based incremental reads.
type OpenCodeReader struct {
	db   *sql.DB
	stmt *sql.Stmt
}

// NewOpenCodeReader opens an OpenCode SQLite database in read-only mode,
// sets PRAGMA query_only = 1, and prepares the read statement.
func NewOpenCodeReader(dbPath string) (*OpenCodeReader, error) {
	dsn := dbPath + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database %s: %w", dbPath, err)
	}

	// Enforce read-only at the connection level.
	if _, err := db.Exec("PRAGMA query_only = 1"); err != nil {
		db.Close()
		return nil, fmt.Errorf("setting PRAGMA query_only: %w", err)
	}

	stmt, err := db.Prepare(`
		SELECT
			m.id, m.session_id, m.time_created, m.time_updated, m.data,
			s.time_created, s.time_updated, s.project_id, s.parent_id,
			s.workspace_id, s.agent
		FROM message m
		JOIN session s ON s.id = m.session_id
		WHERE m.time_updated > ?
		  AND json_extract(m.data, '$.tokens.input') IS NOT NULL
		ORDER BY m.time_updated ASC
		LIMIT ?`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("preparing read statement: %w", err)
	}

	return &OpenCodeReader{db: db, stmt: stmt}, nil
}

// ReadRecords implements Reader.ReadRecords.
func (r *OpenCodeReader) ReadRecords(since time.Time, limit int) ([]UsageRecord, error) {
	sinceMs := since.UnixMilli()

	rows, err := r.stmt.Query(sinceMs, limit)
	if err != nil {
		return nil, fmt.Errorf("querying records: %w", err)
	}
	defer rows.Close()

	var records []UsageRecord
	for rows.Next() {
		var (
			msgID, sessionID     string
			msgCreated, msgUpdated int64
			dataJSON               string
			sessCreated, sessUpdated int64
			projectID, parentID, workspaceID, agent sql.NullString
		)

		if err := rows.Scan(
			&msgID, &sessionID, &msgCreated, &msgUpdated, &dataJSON,
			&sessCreated, &sessUpdated,
			&projectID, &parentID, &workspaceID, &agent,
		); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		rec, err := mapRecord(msgID, sessionID, msgCreated, msgUpdated, dataJSON,
			sessCreated, sessUpdated, projectID, parentID, workspaceID, agent)
		if err != nil {
			return nil, fmt.Errorf("mapping record %s: %w", msgID, err)
		}

		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return records, nil
}

// Close releases the database connection and prepared statement.
func (r *OpenCodeReader) Close() error {
	if r.stmt != nil {
		_ = r.stmt.Close()
	}
	return r.db.Close()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// messageData maps the relevant fields from an OpenCode message.data JSON blob.
type messageData struct {
	ProviderID string  `json:"providerID"`
	ModelID    string  `json:"modelID"`
	Cost       float64 `json:"cost"`
	Finish     string  `json:"finish"`
	Mode       string  `json:"mode"`
	Tokens     struct {
		Input      int64 `json:"input"`
		Output     int64 `json:"output"`
		Reasoning  int64 `json:"reasoning"`
		CacheRead  int64 `json:"cache_read"`
		CacheWrite int64 `json:"cache_write"`
		Total      int64 `json:"total"`
	} `json:"tokens"`
}

// mapRecord converts a scanned SQL row plus parsed JSON data into a UsageRecord.
func mapRecord(
	msgID, sessionID string,
	msgCreated, msgUpdated int64,
	dataJSON string,
	sessCreated, sessUpdated int64,
	projectID, parentID, workspaceID, agent sql.NullString,
) (UsageRecord, error) {
	var md messageData
	if err := json.Unmarshal([]byte(dataJSON), &md); err != nil {
		return UsageRecord{}, fmt.Errorf("parsing message data JSON: %w", err)
	}

	return UsageRecord{
		SourceRecordID:       msgID,
		SourceSessionID:      sessionID,
		SourceProjectID:      projectID.String,
		ParentSessionID:      parentID.String,
		WorkspaceID:          workspaceID.String,
		OccurredAt:           time.UnixMilli(msgUpdated),
		MessageCreatedAt:     msgCreated,
		SessionCreatedAt:     sessCreated,
		SessionUpdatedAt:     sessUpdated,
		Agent:                agent.String,
		ProviderID:           md.ProviderID,
		ModelID:              md.ModelID,
		Mode:                 md.Mode,
		FinishReason:         md.Finish,
		TokensInput:          md.Tokens.Input,
		TokensOutput:         md.Tokens.Output,
		TokensReasoning:      md.Tokens.Reasoning,
		TokensCacheRead:      md.Tokens.CacheRead,
		TokensCacheWrite:     md.Tokens.CacheWrite,
		TokensTotal:          md.Tokens.Total,
		OpenCodeReportedCost: md.Cost,
		CostCurrency:         "USD",
	}, nil
}

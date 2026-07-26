package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Reader defines the interface for reading usage records and projection
// snapshots from an OpenCode source database. Implementations must be safe
// for concurrent use if the underlying database driver supports it.
type Reader interface {
	// ReadRecords returns usage records with message.time_updated strictly
	// greater than since, ordered by time_updated ascending, up to limit
	// records. User messages without tokens.input in message.data are skipped.
	ReadRecords(since time.Time, limit int) ([]UsageRecord, error)

	// ReadSessionContexts returns session context data for the given session
	// IDs. If the session table lacks expected columns, those fields are
	// left at their zero values. Returns an empty slice (not error) for
	// unknown or empty IDs.
	ReadSessionContexts(sessionIDs []string) ([]SessionContextData, error)

	// ReadProjectData returns project data for the given project IDs from
	// the project table. If the table does not exist, returns an empty slice
	// without error.
	ReadProjectData(projectIDs []string) ([]ProjectData, error)

	// ReadProjectDirectoryData returns project directory mappings for the
	// given project IDs. If the table does not exist, returns an empty
	// slice without error.
	ReadProjectDirectoryData(projectIDs []string) ([]ProjectDirectoryData, error)

	// ReadTodoData returns todo items for the given session IDs. If the
	// todo table does not exist, returns an empty slice without error.
	ReadTodoData(sessionIDs []string) ([]TodoData, error)

	// SchemaInfo returns the DatabaseInfo populated during inspection.
	SchemaInfo() DatabaseInfo
}

// OpenCodeReader reads usage records from an OpenCode SQLite source database.
// It uses a read-only connection with a prepared statement for efficient
// cursor-based incremental reads.
type OpenCodeReader struct {
	db     *sql.DB
	stmt   *sql.Stmt
	dbInfo *DatabaseInfo
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

// WithSchemaInfo attaches the DatabaseInfo from OpenAndInspect so the reader
// can be schema-aware when reading projection tables. Must be called before
// any projection read methods.
func (r *OpenCodeReader) WithSchemaInfo(info *DatabaseInfo) *OpenCodeReader {
	if info != nil {
		r.dbInfo = info
	}
	return r
}

// SchemaInfo returns the DatabaseInfo populated during inspection, or an
// empty zero-value if WithSchemaInfo was never called.
func (r *OpenCodeReader) SchemaInfo() DatabaseInfo {
	if r.dbInfo == nil {
		return DatabaseInfo{}
	}
	return *r.dbInfo
}
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
// Projection reads — read-only snapshots from optional OpenCode tables
// ---------------------------------------------------------------------------

// ReadSessionContexts implements Reader.ReadSessionContexts.
func (r *OpenCodeReader) ReadSessionContexts(sessionIDs []string) ([]SessionContextData, error) {
	if len(sessionIDs) == 0 {
		return nil, nil
	}

	// Determine which columns exist in the session table.
	hasTitle := false
	if r.dbInfo != nil {
		for _, col := range r.dbInfo.SessionColumns {
			if col == "title" {
				hasTitle = true
				break
			}
		}
	}

	// Build query dynamically based on available columns.
	titleCol := "'' as title"
	if hasTitle {
		titleCol = "s.title"
	}

	// Build parameterized IN clause.
	placeholders := make([]string, len(sessionIDs))
	args := make([]any, len(sessionIDs))
	for i, id := range sessionIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT s.id, %s, s.agent, s.project_id, s.parent_id, s.workspace_id, s.model
		FROM session s
		WHERE s.id IN (%s)`, titleCol, strings.Join(placeholders, ","))

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying session contexts: %w", err)
	}
	defer rows.Close()

	var result []SessionContextData
	for rows.Next() {
		var (
			id, title, agent, projectID, parentID, workspaceID, model sql.NullString
		)
		if err := rows.Scan(&id, &title, &agent, &projectID, &parentID, &workspaceID, &model); err != nil {
			return nil, fmt.Errorf("scanning session context: %w", err)
		}
		result = append(result, SessionContextData{
			ExternalSessionID: id.String,
			Title:             title.String,
			Agent:             agent.String,
			ProjectID:         projectID.String,
			ParentSessionID:   parentID.String,
			WorkspaceID:       workspaceID.String,
			Model:             model.String,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating session contexts: %w", err)
	}
	return result, nil
}

// ReadProjectData implements Reader.ReadProjectData.
func (r *OpenCodeReader) ReadProjectData(projectIDs []string) ([]ProjectData, error) {
	if len(projectIDs) == 0 {
		return nil, nil
	}

	if r.dbInfo == nil || !r.dbInfo.HasProjectTable {
		return nil, nil
	}

	// Determine available columns.
	hasTitle := slices.Contains(r.dbInfo.ProjectColumns, "title")
	hasWorktree := slices.Contains(r.dbInfo.ProjectColumns, "worktree")

	// Build SELECT expressions.
	selectCols := []string{"p.id"}
	if hasTitle {
		selectCols = append(selectCols, "p.title")
	} else {
		selectCols = append(selectCols, "'' as title")
	}
	if hasWorktree {
		selectCols = append(selectCols, "p.worktree")
	} else {
		selectCols = append(selectCols, "'' as worktree")
	}

	placeholders := make([]string, len(projectIDs))
	args := make([]any, len(projectIDs))
	for i, id := range projectIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf("SELECT %s FROM project p WHERE p.id IN (%s)",
		strings.Join(selectCols, ", "), strings.Join(placeholders, ","))

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying project data: %w", err)
	}
	defer rows.Close()

	var result []ProjectData
	for rows.Next() {
		var id, title, worktree sql.NullString
		if err := rows.Scan(&id, &title, &worktree); err != nil {
			return nil, fmt.Errorf("scanning project data: %w", err)
		}
		result = append(result, ProjectData{
			ExternalProjectID: id.String,
			Title:             title.String,
			Worktree:          worktree.String,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating project data: %w", err)
	}
	return result, nil
}

// ReadProjectDirectoryData implements Reader.ReadProjectDirectoryData.
func (r *OpenCodeReader) ReadProjectDirectoryData(projectIDs []string) ([]ProjectDirectoryData, error) {
	if len(projectIDs) == 0 {
		return nil, nil
	}

	if r.dbInfo == nil || !r.dbInfo.HasProjectDirectoryTable {
		return nil, nil
	}

	// Determine available columns.
	hasPath := slices.Contains(r.dbInfo.ProjectDirectoryColumns, "path")

	pathCol := "'' as path"
	if hasPath {
		pathCol = "pd.path"
	}

	placeholders := make([]string, len(projectIDs))
	args := make([]any, len(projectIDs))
	for i, id := range projectIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf("SELECT pd.project_id, %s FROM project_directory pd WHERE pd.project_id IN (%s)",
		pathCol, strings.Join(placeholders, ","))

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying project directory data: %w", err)
	}
	defer rows.Close()

	var result []ProjectDirectoryData
	for rows.Next() {
		var projectID, path sql.NullString
		if err := rows.Scan(&projectID, &path); err != nil {
			return nil, fmt.Errorf("scanning project directory data: %w", err)
		}
		result = append(result, ProjectDirectoryData{
			ExternalProjectID: projectID.String,
			Path:              path.String,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating project directory data: %w", err)
	}
	return result, nil
}

// ReadTodoData implements Reader.ReadTodoData.
func (r *OpenCodeReader) ReadTodoData(sessionIDs []string) ([]TodoData, error) {
	if len(sessionIDs) == 0 {
		return nil, nil
	}

	if r.dbInfo == nil || !r.dbInfo.HasTodoTable {
		return nil, nil
	}

	// Determine available columns.
	hasDescription := slices.Contains(r.dbInfo.TodoColumns, "description")
	hasStatus := slices.Contains(r.dbInfo.TodoColumns, "status")

	descCol := "'' as description"
	if hasDescription {
		descCol = "t.description"
	}
	statusCol := "'' as status"
	if hasStatus {
		statusCol = "t.status"
	}

	placeholders := make([]string, len(sessionIDs))
	args := make([]any, len(sessionIDs))
	for i, id := range sessionIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf("SELECT t.session_id, %s, %s FROM todo t WHERE t.session_id IN (%s)",
		descCol, statusCol, strings.Join(placeholders, ","))

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying todo data: %w", err)
	}
	defer rows.Close()

	var result []TodoData
	for rows.Next() {
		var sessionID, description, status sql.NullString
		if err := rows.Scan(&sessionID, &description, &status); err != nil {
			return nil, fmt.Errorf("scanning todo data: %w", err)
		}
		result = append(result, TodoData{
			ExternalSessionID: sessionID.String,
			Description:       description.String,
			Status:            status.String,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating todo data: %w", err)
	}
	return result, nil
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
		Cache      struct {
			Read  int64 `json:"read"`
			Write int64 `json:"write"`
		} `json:"cache"`
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
		TokensCacheRead:      md.Tokens.Cache.Read,
		TokensCacheWrite:     md.Tokens.Cache.Write,
		TokensTotal:          md.Tokens.Total,
		OpenCodeReportedCost: md.Cost,
		CostCurrency:         "USD",
	}, nil
}

package sqlite

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// createTestDB builds a minimal OpenCode SQLite database in a temp directory
// with the given session and message rows for reader tests. Returns the path.
//
//nolint:unparam
func createTestDB(t *testing.T, sessions []sessionRow, messages []messageRow) string {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	// Create tables matching OpenCode schema.
	if _, err := db.Exec(`CREATE TABLE message (
		id TEXT PRIMARY KEY,
		session_id TEXT,
		time_created INTEGER,
		time_updated INTEGER,
		data TEXT
	)`); err != nil {
		t.Fatalf("failed to create message table: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE session (
		id TEXT PRIMARY KEY,
		time_created INTEGER,
		time_updated INTEGER,
		project_id TEXT,
		parent_id TEXT,
		workspace_id TEXT,
		agent TEXT,
		model TEXT
	)`); err != nil {
		t.Fatalf("failed to create session table: %v", err)
	}

	// Insert sessions.
	sessStmt, err := db.Prepare(`INSERT INTO session
		(id, time_created, time_updated, project_id, parent_id, workspace_id, agent, model)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("failed to prepare session insert: %v", err)
	}
	defer sessStmt.Close()

	for _, s := range sessions {
		if _, err := sessStmt.Exec(s.id, s.timeCreated, s.timeUpdated,
			s.projectID, s.parentID, s.workspaceID, s.agent, s.model); err != nil {
			t.Fatalf("failed to insert session %s: %v", s.id, err)
		}
	}

	// Insert messages.
	msgStmt, err := db.Prepare(`INSERT INTO message
		(id, session_id, time_created, time_updated, data)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("failed to prepare message insert: %v", err)
	}
	defer msgStmt.Close()

	for _, m := range messages {
		if _, err := msgStmt.Exec(m.id, m.sessionID, m.timeCreated, m.timeUpdated, m.data); err != nil {
			t.Fatalf("failed to insert message %s: %v", m.id, err)
		}
	}

	return dbPath
}

type sessionRow struct {
	id, projectID, parentID, workspaceID, agent, model string
	timeCreated, timeUpdated                           int64
}

type messageRow struct {
	id, sessionID, data string
	timeCreated, timeUpdated int64
}

// sample timestamps (Unix ms) — spaced to make cursor tests clear.
const (
	tsBase    = 1_700_000_000_000
	tsStep    = 10_000
	sessTimeA = tsBase - 1_000_000 // session 1 created well before
	sessTimeB = tsBase             // session 2 created at base
)

// sample message data JSON blobs.
const (
	assistantFullUsage = `{
		"providerID": "openai",
		"modelID": "gpt-4o",
		"cost": 0.0023,
		"finish": "stop",
		"mode": "chat",
		"tokens": {
			"input": 150,
			"output": 75,
			"reasoning": 0,
			"cache": {
				"read": 20,
				"write": 10
			},
			"total": 255
		}
	}`

	userMessageData = `{"role": "user", "content": "hello"}`

	partialUsage = `{
		"providerID": "anthropic",
		"modelID": "claude-sonnet-4",
		"cost": 0.0015,
		"finish": "stop",
		"mode": "chat",
		"tokens": {
			"input": 200,
			"output": 100,
			"total": 300
		}
	}`

	zeroCostUsage = `{
		"providerID": "local",
		"modelID": "llama-3.1-8b",
		"cost": 0,
		"finish": "stop",
		"mode": "chat",
		"tokens": {
			"input": 50,
			"output": 25,
			"total": 75
		}
	}`

	assistantUsageAnother = `{
		"providerID": "openai",
		"modelID": "gpt-4o-mini",
		"cost": 0.0045,
		"finish": "stop",
		"mode": "agent",
		"tokens": {
			"input": 500,
			"output": 200,
			"reasoning": 0,
			"cache": {
				"read": 100,
				"write": 50
			},
			"total": 850
		}
	}`
)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestReadRecords_ExtractsFullUsage(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: "claude-sonnet-4"},
	}
	messages := []messageRow{
		{id: "msg-1", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: assistantFullUsage},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	rec := records[0]
	tests := []struct {
		name string
		got  any
		want any
	}{
		{"SourceRecordID", rec.SourceRecordID, "msg-1"},
		{"SourceSessionID", rec.SourceSessionID, "sess-a"},
		{"SourceProjectID", rec.SourceProjectID, "proj-1"},
		{"ParentSessionID", rec.ParentSessionID, ""},
		{"WorkspaceID", rec.WorkspaceID, "ws-1"},
		{"Agent", rec.Agent, "claude"},
		{"ProviderID", rec.ProviderID, "openai"},
		{"ModelID", rec.ModelID, "gpt-4o"},
		{"Mode", rec.Mode, "chat"},
		{"FinishReason", rec.FinishReason, "stop"},
		{"TokensInput", rec.TokensInput, int64(150)},
		{"TokensOutput", rec.TokensOutput, int64(75)},
		{"TokensReasoning", rec.TokensReasoning, int64(0)},
		{"TokensCacheRead", rec.TokensCacheRead, int64(20)},
		{"TokensCacheWrite", rec.TokensCacheWrite, int64(10)},
		{"TokensTotal", rec.TokensTotal, int64(255)},
		{"OpenCodeReportedCost", rec.OpenCodeReportedCost, 0.0023},
		{"CostCurrency", rec.CostCurrency, "USD"},
		{"CostSource", rec.CostSource, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", tt.got, tt.got, tt.want, tt.want)
			}
		})
	}

	// Check OccurredAt is derived from msgUpdated.
	expectedTime := time.UnixMilli(tsBase)
	if !rec.OccurredAt.Equal(expectedTime) {
		t.Errorf("OccurredAt = %v, want %v", rec.OccurredAt, expectedTime)
	}
	if rec.MessageCreatedAt != tsBase {
		t.Errorf("MessageCreatedAt = %d, want %d", rec.MessageCreatedAt, tsBase)
	}
	if rec.SessionCreatedAt != sessTimeA {
		t.Errorf("SessionCreatedAt = %d, want %d", rec.SessionCreatedAt, sessTimeA)
	}
	if rec.SessionUpdatedAt != sessTimeA {
		t.Errorf("SessionUpdatedAt = %d, want %d", rec.SessionUpdatedAt, sessTimeA)
	}
}

func TestReadRecords_SkipsUserMessages(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: ""},
	}
	messages := []messageRow{
		{id: "msg-assistant", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: assistantFullUsage},
		{id: "msg-user", sessionID: "sess-a", timeCreated: tsBase + tsStep, timeUpdated: tsBase + tsStep, data: userMessageData},
		{id: "msg-assistant-2", sessionID: "sess-a", timeCreated: tsBase + 2*tsStep, timeUpdated: tsBase + 2*tsStep, data: assistantUsageAnother},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records (user message skipped), got %d", len(records))
	}

	// Verify the user message ID is not present.
	if records[0].SourceRecordID == "msg-user" || records[1].SourceRecordID == "msg-user" {
		t.Error("user message should have been skipped")
	}
}

func TestReadRecords_CursorFiltering(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: ""},
	}
	// Three assistant messages at tsBase, tsBase+step, tsBase+2*step.
	messages := []messageRow{
		{id: "msg-early", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: assistantFullUsage},
		{id: "msg-mid", sessionID: "sess-a", timeCreated: tsBase + tsStep, timeUpdated: tsBase + tsStep, data: partialUsage},
		{id: "msg-late", sessionID: "sess-a", timeCreated: tsBase + 2*tsStep, timeUpdated: tsBase + 2*tsStep, data: zeroCostUsage},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	// Cursor at tsBase (exclusive) — should return msg-mid and msg-late.
	cursor := time.UnixMilli(tsBase)
	records, err := r.ReadRecords(cursor, 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records after cursor, got %d", len(records))
	}

	if records[0].SourceRecordID != "msg-mid" {
		t.Errorf("first record should be msg-mid, got %s", records[0].SourceRecordID)
	}
	if records[1].SourceRecordID != "msg-late" {
		t.Errorf("second record should be msg-late, got %s", records[1].SourceRecordID)
	}

	// Cursor at tsBase+2*step should return nothing.
	cursor2 := time.UnixMilli(tsBase + 2*tsStep)
	records2, err := r.ReadRecords(cursor2, 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}
	if len(records2) != 0 {
		t.Errorf("expected 0 records after last timestamp, got %d", len(records2))
	}
}

func TestReadRecords_BatchLimit(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: ""},
	}
	messages := []messageRow{
		{id: "msg-1", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: assistantFullUsage},
		{id: "msg-2", sessionID: "sess-a", timeCreated: tsBase + tsStep, timeUpdated: tsBase + tsStep, data: partialUsage},
		{id: "msg-3", sessionID: "sess-a", timeCreated: tsBase + 2*tsStep, timeUpdated: tsBase + 2*tsStep, data: zeroCostUsage},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	// Limit to 2.
	records, err := r.ReadRecords(time.UnixMilli(0), 2)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records (limited), got %d", len(records))
	}

	// Limit to 0 should return nothing.
	records0, err := r.ReadRecords(time.UnixMilli(0), 0)
	if err != nil {
		t.Fatalf("ReadRecords with limit=0 failed: %v", err)
	}
	if len(records0) != 0 {
		t.Errorf("expected 0 records with limit=0, got %d", len(records0))
	}
}

func TestReadRecords_MissingJSONFields(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: ""},
	}
	messages := []messageRow{
		{id: "msg-minimal", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase,
			data: `{"providerID": "test", "modelID": "test-model", "cost": 0.01, "finish": "stop", "mode": "chat", "tokens": {"input": 10, "output": 5, "total": 15}}`},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	rec := records[0]

	// Missing fields should default to zero values.
	if rec.TokensReasoning != 0 {
		t.Errorf("TokensReasoning should be 0 for missing field, got %d", rec.TokensReasoning)
	}
	if rec.TokensCacheRead != 0 {
		t.Errorf("TokensCacheRead should be 0 for missing field, got %d", rec.TokensCacheRead)
	}
	if rec.TokensCacheWrite != 0 {
		t.Errorf("TokensCacheWrite should be 0 for missing field, got %d", rec.TokensCacheWrite)
	}

	// Fields that are present should still map correctly.
	if rec.ProviderID != "test" {
		t.Errorf("ProviderID = %q, want %q", rec.ProviderID, "test")
	}
	if rec.TokensInput != 10 {
		t.Errorf("TokensInput = %d, want 10", rec.TokensInput)
	}
	if rec.TokensTotal != 15 {
		t.Errorf("TokensTotal = %d, want 15", rec.TokensTotal)
	}
}

func TestReadRecords_ZeroCost(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "local-agent", model: ""},
	}
	messages := []messageRow{
		{id: "msg-free", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: zeroCostUsage},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record (zero-cost), got %d", len(records))
	}

	if records[0].OpenCodeReportedCost != 0 {
		t.Errorf("OpenCodeReportedCost should be 0, got %f", records[0].OpenCodeReportedCost)
	}
	if records[0].ProviderID != "local" {
		t.Errorf("ProviderID = %q, want %q", records[0].ProviderID, "local")
	}
	if records[0].TokensInput != 50 {
		t.Errorf("TokensInput = %d, want 50", records[0].TokensInput)
	}
}

func TestReadRecords_MultipleSessions(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: "claude-sonnet-4"},
		{id: "sess-b", timeCreated: sessTimeB, timeUpdated: sessTimeB,
			projectID: "proj-2", parentID: "parent-1", workspaceID: "ws-2", agent: "gpt", model: "gpt-4o"},
	}
	messages := []messageRow{
		{id: "msg-sa", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: assistantFullUsage},
		{id: "msg-sb", sessionID: "sess-b", timeCreated: tsBase + tsStep, timeUpdated: tsBase + tsStep, data: assistantUsageAnother},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records across sessions, got %d", len(records))
	}

	// Record from sess-b should carry session-b metadata.
	recB := records[1]
	if recB.SourceRecordID != "msg-sb" {
		t.Errorf("second record id = %s, want msg-sb", recB.SourceRecordID)
	}
	if recB.SourceSessionID != "sess-b" {
		t.Errorf("SourceSessionID = %s, want sess-b", recB.SourceSessionID)
	}
	if recB.SourceProjectID != "proj-2" {
		t.Errorf("SourceProjectID = %s, want proj-2", recB.SourceProjectID)
	}
	if recB.ParentSessionID != "parent-1" {
		t.Errorf("ParentSessionID = %s, want parent-1", recB.ParentSessionID)
	}
	if recB.WorkspaceID != "ws-2" {
		t.Errorf("WorkspaceID = %s, want ws-2", recB.WorkspaceID)
	}
	if recB.Agent != "gpt" {
		t.Errorf("Agent = %s, want gpt", recB.Agent)
	}
	if recB.ProviderID != "openai" {
		t.Errorf("ProviderID = %s, want openai", recB.ProviderID)
	}
	if recB.ModelID != "gpt-4o-mini" {
		t.Errorf("ModelID = %s, want gpt-4o-mini", recB.ModelID)
	}
}

func TestReadRecords_Ordering(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: ""},
	}
	// Insert messages out of time order — reader should return them in asc order.
	messages := []messageRow{
		{id: "msg-late", sessionID: "sess-a", timeCreated: tsBase + 5*tsStep, timeUpdated: tsBase + 5*tsStep, data: assistantFullUsage},
		{id: "msg-early", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: partialUsage},
		{id: "msg-mid", sessionID: "sess-a", timeCreated: tsBase + 2*tsStep, timeUpdated: tsBase + 2*tsStep, data: zeroCostUsage},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// Verify ascending time_updated order.
	expected := []string{"msg-early", "msg-mid", "msg-late"}
	for i, rec := range records {
		if rec.SourceRecordID != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], rec.SourceRecordID)
		}
	}
}

func TestReadRecords_EmptyDatabase(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: ""},
	}
	messages := []messageRow{} // no messages

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("expected 0 records in empty database, got %d", len(records))
	}
}

func TestReadRecords_InvalidDBPath(t *testing.T) {
	_, err := NewOpenCodeReader("/nonexistent/path/that/does/not/exist.db")
	if err == nil {
		t.Error("expected error for nonexistent database path, got nil")
	}
}

// ---------------------------------------------------------------------------
// Projection test helpers
// ---------------------------------------------------------------------------

// createTestDBWithProjections builds a test database with optional project,
// project_directory, and todo tables in addition to the required message and
// session tables. Returns the database path.
func createTestDBWithProjections(t *testing.T, sessions []sessionRow, messages []messageRow, projects []projectRow, projectDirs []projectDirRow, todos []todoRow) string {
	t.Helper()

	dbPath := createTestDB(t, sessions, messages)

	// Re-open to add optional tables.
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to reopen test db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS project (
		id TEXT PRIMARY KEY,
		title TEXT,
		worktree TEXT
	)`); err != nil {
		t.Fatalf("failed to create project table: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS project_directory (
		project_id TEXT,
		path TEXT
	)`); err != nil {
		t.Fatalf("failed to create project_directory table: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS todo (
		session_id TEXT,
		description TEXT,
		status TEXT
	)`); err != nil {
		t.Fatalf("failed to create todo table: %v", err)
	}

	// Insert project rows.
	for _, p := range projects {
		if _, err := db.Exec(`INSERT INTO project (id, title, worktree) VALUES (?, ?, ?)`,
			p.id, p.title, p.worktree); err != nil {
			t.Fatalf("failed to insert project %s: %v", p.id, err)
		}
	}

	// Insert project_directory rows.
	for _, pd := range projectDirs {
		if _, err := db.Exec(`INSERT INTO project_directory (project_id, path) VALUES (?, ?)`,
			pd.projectID, pd.path); err != nil {
			t.Fatalf("failed to insert project_directory: %v", err)
		}
	}

	// Insert todo rows.
	for _, td := range todos {
		if _, err := db.Exec(`INSERT INTO todo (session_id, description, status) VALUES (?, ?, ?)`,
			td.sessionID, td.description, td.status); err != nil {
			t.Fatalf("failed to insert todo: %v", err)
		}
	}

	return dbPath
}

// openReaderWithSchema opens a reader and attaches schema info via
// OpenAndInspect.
func openReaderWithSchema(t *testing.T, dbPath string) *OpenCodeReader {
	t.Helper()

	info, err := OpenAndInspect(dbPath)
	if err != nil {
		t.Fatalf("OpenAndInspect failed: %v", err)
	}

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	r.WithSchemaInfo(info)
	return r
}

type projectRow struct {
	id, title, worktree string
}

type projectDirRow struct {
	projectID, path string
}

type todoRow struct {
	sessionID, description, status string
}

// createTestDBWithCustomProjectTable creates a test database with a
// caller-defined project table schema. This allows tests to control which
// columns exist (name, title, worktree, etc.) independently.
func createTestDBWithCustomProjectTable(t *testing.T, sessions []sessionRow, messages []messageRow, createTable string, projectValues [][]string) string {
	t.Helper()

	dbPath := createTestDB(t, sessions, messages)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to reopen test db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(createTable); err != nil {
		t.Fatalf("failed to create custom project table: %v", err)
	}

	for _, row := range projectValues {
		placeholders := make([]string, len(row))
		args := make([]any, len(row))
		for j, val := range row {
			placeholders[j] = "?"
			args[j] = val
		}
		query := fmt.Sprintf("INSERT INTO project VALUES (%s)", joinStr(placeholders))
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatalf("failed to insert custom project row: %v", err)
		}
	}

	return dbPath
}

// joinStr joins string slices with comma separators for SQL query building.
func joinStr(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += ", " + parts[i]
	}
	return result
}

// ---------------------------------------------------------------------------
// Projection read tests
// ---------------------------------------------------------------------------

func TestReadSessionContexts_HasFields(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "parent-sess", workspaceID: "ws-1",
			agent: "claude", model: "gpt-4o"},
		{id: "sess-b", timeCreated: sessTimeB, timeUpdated: sessTimeB,
			projectID: "proj-2", parentID: "", workspaceID: "ws-2",
			agent: "code-editor", model: "claude-sonnet"},
	}

	dbPath := createTestDB(t, sessions, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	ctxs, err := r.ReadSessionContexts([]string{"sess-a", "sess-b"})
	if err != nil {
		t.Fatalf("ReadSessionContexts failed: %v", err)
	}
	if len(ctxs) != 2 {
		t.Fatalf("expected 2 session contexts, got %d", len(ctxs))
	}

	// Verify sess-a fields.
	if ctxs[0].ExternalSessionID != "sess-a" {
		t.Errorf("ExternalSessionID = %q, want %q", ctxs[0].ExternalSessionID, "sess-a")
	}
	if ctxs[0].Agent != "claude" {
		t.Errorf("Agent = %q, want %q", ctxs[0].Agent, "claude")
	}
	if ctxs[0].ProjectID != "proj-1" {
		t.Errorf("ProjectID = %q, want %q", ctxs[0].ProjectID, "proj-1")
	}
	if ctxs[0].ParentSessionID != "parent-sess" {
		t.Errorf("ParentSessionID = %q, want %q", ctxs[0].ParentSessionID, "parent-sess")
	}
	if ctxs[0].WorkspaceID != "ws-1" {
		t.Errorf("WorkspaceID = %q, want %q", ctxs[0].WorkspaceID, "ws-1")
	}
	if ctxs[0].Model != "gpt-4o" {
		t.Errorf("Model = %q, want %q", ctxs[0].Model, "gpt-4o")
	}
	// Title should be empty since the test schema doesn't include it.
	if ctxs[0].Title != "" {
		t.Errorf("Title should be empty, got %q", ctxs[0].Title)
	}
}

func TestReadSessionContexts_EmptyIDs(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	dbPath := createTestDB(t, sessions, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	// Empty input should return nil/empty without error.
	ctxs, err := r.ReadSessionContexts(nil)
	if err != nil {
		t.Fatalf("ReadSessionContexts with nil input failed: %v", err)
	}
	if ctxs != nil {
		t.Errorf("expected nil result for empty input, got %d items", len(ctxs))
	}
}

func TestReadSessionContexts_UnknownID(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	dbPath := createTestDB(t, sessions, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	ctxs, err := r.ReadSessionContexts([]string{"nonexistent"})
	if err != nil {
		t.Fatalf("ReadSessionContexts failed: %v", err)
	}
	if len(ctxs) != 0 {
		t.Errorf("expected 0 contexts for unknown ID, got %d", len(ctxs))
	}
}

func TestReadProjectData_ReadsExisting(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}
	projects := []projectRow{
		{id: "proj-1", title: "Test Project", worktree: "/tmp/test"},
		{id: "proj-2", title: "Other Project", worktree: "/tmp/other"},
	}

	dbPath := createTestDBWithProjections(t, sessions, nil, projects, nil, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadProjectData([]string{"proj-1", "proj-2"})
	if err != nil {
		t.Fatalf("ReadProjectData failed: %v", err)
	}
	if len(data) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(data))
	}
	if data[0].ExternalProjectID != "proj-1" {
		t.Errorf("ExternalProjectID = %q, want %q", data[0].ExternalProjectID, "proj-1")
	}
	if data[0].Name != "Test Project" {
		t.Errorf("Name = %q, want %q", data[0].Name, "Test Project")
	}
	if data[0].Worktree != "/tmp/test" {
		t.Errorf("Worktree = %q, want %q", data[0].Worktree, "/tmp/test")
	}
}

func TestReadProjectData_TableNotExist(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	// Use DB without optional tables.
	dbPath := createTestDB(t, sessions, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadProjectData([]string{"proj-1"})
	if err != nil {
		t.Fatalf("ReadProjectData should not error when table doesn't exist: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected 0 projects when table doesn't exist, got %d", len(data))
	}
}

func TestReadProjectData_ReadsNameColumn(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	// Project table with name column (no title).
	dbPath := createTestDBWithCustomProjectTable(t, sessions, nil,
		`CREATE TABLE project (
			id TEXT PRIMARY KEY,
			name TEXT,
			worktree TEXT
		)`,
		[][]string{
			{"proj-1", "Name Project", "/tmp/test"},
		},
	)

	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadProjectData([]string{"proj-1"})
	if err != nil {
		t.Fatalf("ReadProjectData failed: %v", err)
	}
	if len(data) != 1 {
		t.Fatalf("expected 1 project, got %d", len(data))
	}
	if data[0].ExternalProjectID != "proj-1" {
		t.Errorf("ExternalProjectID = %q, want %q", data[0].ExternalProjectID, "proj-1")
	}
	if data[0].Name != "Name Project" {
		t.Errorf("Name = %q, want %q", data[0].Name, "Name Project")
	}
	if data[0].Worktree != "/tmp/test" {
		t.Errorf("Worktree = %q, want %q", data[0].Worktree, "/tmp/test")
	}
}

func TestReadProjectData_FallsBackToTitle(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	// Project table with title column (no name).
	dbPath := createTestDBWithCustomProjectTable(t, sessions, nil,
		`CREATE TABLE project (
			id TEXT PRIMARY KEY,
			title TEXT,
			worktree TEXT
		)`,
		[][]string{
			{"proj-1", "Title Project", "/tmp/other"},
		},
	)

	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadProjectData([]string{"proj-1"})
	if err != nil {
		t.Fatalf("ReadProjectData failed: %v", err)
	}
	if len(data) != 1 {
		t.Fatalf("expected 1 project, got %d", len(data))
	}
	if data[0].ExternalProjectID != "proj-1" {
		t.Errorf("ExternalProjectID = %q, want %q", data[0].ExternalProjectID, "proj-1")
	}
	if data[0].Name != "Title Project" {
		t.Errorf("Name (fallback from title) = %q, want %q", data[0].Name, "Title Project")
	}
	if data[0].Worktree != "/tmp/other" {
		t.Errorf("Worktree = %q, want %q", data[0].Worktree, "/tmp/other")
	}
}

func TestReadProjectData_NoNameNoTitle(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	// Project table with neither name nor title.
	dbPath := createTestDBWithCustomProjectTable(t, sessions, nil,
		`CREATE TABLE project (
			id TEXT PRIMARY KEY,
			worktree TEXT
		)`,
		[][]string{
			{"proj-1", "/tmp/minimal"},
		},
	)

	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadProjectData([]string{"proj-1"})
	if err != nil {
		t.Fatalf("ReadProjectData failed: %v", err)
	}
	if len(data) != 1 {
		t.Fatalf("expected 1 project, got %d", len(data))
	}
	if data[0].ExternalProjectID != "proj-1" {
		t.Errorf("ExternalProjectID = %q, want %q", data[0].ExternalProjectID, "proj-1")
	}
	if data[0].Name != "" {
		t.Errorf("Name should be empty when neither name nor title column exists, got %q", data[0].Name)
	}
	if data[0].Worktree != "/tmp/minimal" {
		t.Errorf("Worktree = %q, want %q", data[0].Worktree, "/tmp/minimal")
	}
}

func TestReadProjectDirectoryData_ReadsExisting(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}
	projects := []projectRow{
		{id: "proj-1", title: "Test Project", worktree: "/tmp/test"},
	}
	projectDirs := []projectDirRow{
		{projectID: "proj-1", path: "/tmp/test/src"},
		{projectID: "proj-1", path: "/tmp/test/lib"},
	}

	dbPath := createTestDBWithProjections(t, sessions, nil, projects, projectDirs, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadProjectDirectoryData([]string{"proj-1"})
	if err != nil {
		t.Fatalf("ReadProjectDirectoryData failed: %v", err)
	}
	if len(data) != 2 {
		t.Fatalf("expected 2 project directory entries, got %d", len(data))
	}
	if data[0].ExternalProjectID != "proj-1" {
		t.Errorf("ExternalProjectID = %q, want %q", data[0].ExternalProjectID, "proj-1")
	}
}

func TestReadProjectDirectoryData_TableNotExist(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	dbPath := createTestDB(t, sessions, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadProjectDirectoryData([]string{"proj-1"})
	if err != nil {
		t.Fatalf("ReadProjectDirectoryData should not error when table doesn't exist: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected 0 entries when table doesn't exist, got %d", len(data))
	}
}

func TestReadTodoData_ReadsExisting(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
		{id: "sess-b", timeCreated: sessTimeB, timeUpdated: sessTimeB,
			projectID: "proj-1", agent: "gpt", model: ""},
	}
	todos := []todoRow{
		{sessionID: "sess-a", description: "Write tests", status: "completed"},
		{sessionID: "sess-a", description: "Review PR", status: "pending"},
		{sessionID: "sess-b", description: "Deploy", status: "pending"},
	}

	dbPath := createTestDBWithProjections(t, sessions, nil, nil, nil, todos)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadTodoData([]string{"sess-a", "sess-b"})
	if err != nil {
		t.Fatalf("ReadTodoData failed: %v", err)
	}
	if len(data) != 3 {
		t.Fatalf("expected 3 todo items, got %d", len(data))
	}

	// Count by session.
	sessACount := 0
	sessBCount := 0
	for _, td := range data {
		if td.ExternalSessionID == "sess-a" {
			sessACount++
		} else if td.ExternalSessionID == "sess-b" {
			sessBCount++
		}
	}
	if sessACount != 2 {
		t.Errorf("expected 2 todos for sess-a, got %d", sessACount)
	}
	if sessBCount != 1 {
		t.Errorf("expected 1 todo for sess-b, got %d", sessBCount)
	}
}

func TestReadTodoData_TableNotExist(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	dbPath := createTestDB(t, sessions, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	data, err := r.ReadTodoData([]string{"sess-a"})
	if err != nil {
		t.Fatalf("ReadTodoData should not error when table doesn't exist: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected 0 todos when table doesn't exist, got %d", len(data))
	}
}

func TestSchemaInfo_ReflectsDetectedTables(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}
	projects := []projectRow{
		{id: "proj-1", title: "Test", worktree: "/tmp"},
	}
	todos := []todoRow{
		{sessionID: "sess-a", description: "Task", status: "pending"},
	}

	// DB with project and todo tables; project_directory table also created by helper (empty).
	dbPath := createTestDBWithProjections(t, sessions, nil, projects, nil, todos)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	info := r.SchemaInfo()

	if !info.HasProjectTable {
		t.Error("HasProjectTable should be true")
	}
	// project_directory table is created by the helper even when no rows
	// are inserted — verify it is detected.
	if !info.HasProjectDirectoryTable {
		t.Error("HasProjectDirectoryTable should be true")
	}
	if !info.HasTodoTable {
		t.Error("HasTodoTable should be true")
	}
	if !slices.Contains(info.ProjectColumns, "title") {
		t.Error("ProjectColumns should contain 'title'")
	}
	if !slices.Contains(info.TodoColumns, "status") {
		t.Error("TodoColumns should contain 'status'")
	}
}

func TestSchemaInfo_NoOptionalTables(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", agent: "claude", model: ""},
	}

	dbPath := createTestDB(t, sessions, nil)
	r := openReaderWithSchema(t, dbPath)
	defer r.Close()

	info := r.SchemaInfo()

	if info.HasProjectTable {
		t.Error("HasProjectTable should be false")
	}
	if info.HasProjectDirectoryTable {
		t.Error("HasProjectDirectoryTable should be false")
	}
	if info.HasTodoTable {
		t.Error("HasTodoTable should be false")
	}
}

// ---------------------------------------------------------------------------
// Composite paging tests
// ---------------------------------------------------------------------------

func TestReadRecordsAfterCompositePaging(t *testing.T) {
	sessions := []sessionRow{
		{id: "sess-a", timeCreated: sessTimeA, timeUpdated: sessTimeA,
			projectID: "proj-1", parentID: "", workspaceID: "ws-1", agent: "claude", model: ""},
	}
	// 5 messages at the SAME time_updated, plus 2 more at a later time.
	messages := []messageRow{
		{id: "msg-a1", sessionID: "sess-a", timeCreated: tsBase, timeUpdated: tsBase, data: assistantFullUsage},
		{id: "msg-a2", sessionID: "sess-a", timeCreated: tsBase + 1, timeUpdated: tsBase, data: partialUsage},
		{id: "msg-a3", sessionID: "sess-a", timeCreated: tsBase + 2, timeUpdated: tsBase, data: zeroCostUsage},
		{id: "msg-a4", sessionID: "sess-a", timeCreated: tsBase + 3, timeUpdated: tsBase, data: assistantUsageAnother},
		{id: "msg-a5", sessionID: "sess-a", timeCreated: tsBase + 4, timeUpdated: tsBase, data: assistantFullUsage},
		{id: "msg-b1", sessionID: "sess-a", timeCreated: tsBase + 10*tsStep, timeUpdated: tsBase + 10*tsStep, data: partialUsage},
		{id: "msg-b2", sessionID: "sess-a", timeCreated: tsBase + 11*tsStep, timeUpdated: tsBase + 10*tsStep, data: zeroCostUsage},
	}

	dbPath := createTestDB(t, sessions, messages)

	r, err := NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader failed: %v", err)
	}
	defer r.Close()

	batchLimit := 2
	collected := make(map[string]bool)

	// Page 1: strict time-only (ReadRecords).
	page1, err := r.ReadRecords(time.UnixMilli(0), batchLimit)
	if err != nil {
		t.Fatalf("ReadRecords page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page 1: expected 2 records, got %d", len(page1))
	}
	for _, rec := range page1 {
		collected[rec.SourceRecordID] = true
	}

	// Page 1 max composite: last record's (time_updated, id).
	last1 := page1[len(page1)-1]
	since1 := last1.OccurredAt
	after1 := last1.SourceRecordID

	// Page 2: composite cursor (msg-a2, msg-a3 expected since limit=2).
	page2, err := r.ReadRecordsAfter(since1, after1, batchLimit)
	if err != nil {
		t.Fatalf("ReadRecordsAfter page 2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page 2: expected 2 records, got %d", len(page2))
	}
	for _, rec := range page2 {
		if collected[rec.SourceRecordID] {
			t.Errorf("page 2: duplicate record %s", rec.SourceRecordID)
		}
		collected[rec.SourceRecordID] = true
	}

	// Page 3: composite cursor (msg-a4, msg-a5 expected with limit=2).
	last2 := page2[len(page2)-1]
	since2 := last2.OccurredAt
	after2 := last2.SourceRecordID

	page3, err := r.ReadRecordsAfter(since2, after2, batchLimit)
	if err != nil {
		t.Fatalf("ReadRecordsAfter page 3: %v", err)
	}
	// Should get 1 record (msg-a5) plus 2 from later time = 3? No — limit=2,
	// so we get msg-a5 and msg-b1 (the remaining tied + first of the next time).
	if len(page3) != 2 {
		t.Fatalf("page 3: expected 2 records, got %d", len(page3))
	}
	for _, rec := range page3 {
		if collected[rec.SourceRecordID] {
			t.Errorf("page 3: duplicate record %s", rec.SourceRecordID)
		}
		collected[rec.SourceRecordID] = true
	}

	// Page 4: composite cursor (msg-b1).
	last3 := page3[len(page3)-1]
	since3 := last3.OccurredAt
	after3 := last3.SourceRecordID

	page4, err := r.ReadRecordsAfter(since3, after3, batchLimit)
	if err != nil {
		t.Fatalf("ReadRecordsAfter page 4: %v", err)
	}
	if len(page4) != 1 {
		t.Fatalf("page 4: expected 1 record (msg-b2), got %d", len(page4))
	}
	for _, rec := range page4 {
		if collected[rec.SourceRecordID] {
			t.Errorf("page 4: duplicate record %s", rec.SourceRecordID)
		}
		collected[rec.SourceRecordID] = true
	}

	// Page 5: should return 0 records.
	last4 := page4[len(page4)-1]
	since4 := last4.OccurredAt
	after4 := last4.SourceRecordID

	page5, err := r.ReadRecordsAfter(since4, after4, batchLimit)
	if err != nil {
		t.Fatalf("ReadRecordsAfter page 5: %v", err)
	}
	if len(page5) != 0 {
		t.Errorf("page 5: expected 0 records, got %d", len(page5))
	}

	// Verify all 7 records collected exactly once.
	expectedIDs := []string{"msg-a1", "msg-a2", "msg-a3", "msg-a4", "msg-a5", "msg-b1", "msg-b2"}
	for _, id := range expectedIDs {
		if !collected[id] {
			t.Errorf("record %s was never collected (dropped)", id)
		}
	}
	if len(collected) != len(expectedIDs) {
		t.Errorf("expected %d unique records, got %d (may include unexpected ids)", len(expectedIDs), len(collected))
	}
}



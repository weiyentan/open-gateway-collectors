package sqlite

import (
	"database/sql"
	"path/filepath"
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
			"cache_read": 20,
			"cache_write": 10,
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
			"cache_read": 100,
			"cache_write": 50,
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

package gateway

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/opencode-gateway/collectors/internal/sqlite"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// createIntegrationTestDB builds a minimal OpenCode SQLite database with
// message and session tables populated with all enrichment fields. Returns
// the filesystem path to the database.
func createIntegrationTestDB(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "integration_test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

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

	// Insert a session with all enrichment fields populated.
	if _, err := db.Exec(`INSERT INTO session
		(id, time_created, time_updated, project_id, parent_id, workspace_id, agent, model)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"sess-enriched",
		int64(1700000000000),
		int64(1700000000000),
		"project-alpha",
		"parent-session-42",
		"workspace-bravo",
		"senior-editor",
		"gpt-4o",
	); err != nil {
		t.Fatalf("failed to insert session: %v", err)
	}

	// Insert a message with a data JSON blob containing all enrichment fields.
	// The message.data JSON must match the structure expected by the reader's
	// mapRecord() function (messageData struct): providerID, modelID, cost,
	// finish, mode, tokens: { input, output, reasoning, cache_read, cache_write, total }.
	enrichedDataJSON := `{
		"providerID": "openai",
		"modelID": "gpt-4o",
		"cost": 0.0125,
		"finish": "stop",
		"mode": "agent",
		"tokens": {
			"input": 500,
			"output": 200,
			"reasoning": 75,
			"cache_read": 100,
			"cache_write": 50,
			"total": 925
		}
	}`

	if _, err := db.Exec(`INSERT INTO message
		(id, session_id, time_created, time_updated, data)
		VALUES (?, ?, ?, ?, ?)`,
		"msg-enriched-001",
		"sess-enriched",
		int64(1700001000000),
		int64(1700001000000),
		enrichedDataJSON,
	); err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	return dbPath
}

// sqliteToGatewayUsageRecord replicates the collector's toGatewayUsageRecord()
// mapping inline so the gateway package can exercise the full pipeline without
// importing the collector package (which would create a circular dependency).
func sqliteToGatewayUsageRecord(rec sqlite.UsageRecord) UsageRecord {
	return UsageRecord{
		SourceRecordID:   rec.SourceRecordID,
		SessionID:        rec.SourceSessionID,
		Model:            rec.ModelID,
		ProviderID:       rec.ProviderID,
		Mode:             rec.Mode,
		Agent:            rec.Agent,
		ProjectID:        rec.SourceProjectID,
		WorkspaceID:      rec.WorkspaceID,
		ParentSessionID:  rec.ParentSessionID,
		ReasoningTokens:  rec.TokensReasoning,
		FinishReason:     rec.FinishReason,
		InputTokens:      rec.TokensInput,
		OutputTokens:     rec.TokensOutput,
		TokensCacheRead:  rec.TokensCacheRead,
		TokensCacheWrite: rec.TokensCacheWrite,
		EstimatedCostUSD: rec.OpenCodeReportedCost,
		OccurredAt:       rec.OccurredAt,
	}
}

// ---------------------------------------------------------------------------
// Integration test: full pipeline
// ---------------------------------------------------------------------------

// TestFullPipeline_AllEnrichmentFields tests the end-to-end pipeline from
// SQLite fixture through reader → gateway.UsageRecord → IngestRecord,
// verifying that every enrichment field survives to the wire format.
func TestFullPipeline_AllEnrichmentFields(t *testing.T) {
	// 1. Create a SQLite source database with populated enrichment fields.
	dbPath := createIntegrationTestDB(t)

	// 2. Open and read using the sqlite package's real reader.
	reader, err := sqlite.NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader: %v", err)
	}
	defer reader.Close()

	records, err := reader.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	sqlRec := records[0]

	// 3. Convert to gateway.UsageRecord (replicating toGatewayUsageRecord).
	gwRec := sqliteToGatewayUsageRecord(sqlRec)

	// 4. Convert to wire-format IngestRecord via MapToIngestRecord.
	ingestRec := MapToIngestRecord(gwRec)

	// 5. Assert every enrichment field survived the full pipeline.

	// --- Identity / core fields ---
	if ingestRec.SourceRecordID != "msg-enriched-001" {
		t.Errorf("SourceRecordID = %q, want %q", ingestRec.SourceRecordID, "msg-enriched-001")
	}
	if ingestRec.SessionID != "sess-enriched" {
		t.Errorf("SessionID = %q, want %q", ingestRec.SessionID, "sess-enriched")
	}
	if ingestRec.Model != "gpt-4o" {
		t.Errorf("Model = %q, want %q", ingestRec.Model, "gpt-4o")
	}
	if ingestRec.Provider != "openai" {
		t.Errorf("Provider = %q, want %q", ingestRec.Provider, "openai")
	}
	if ingestRec.Mode != "agent" {
		t.Errorf("Mode = %q, want %q", ingestRec.Mode, "agent")
	}

	// --- Enrichment: session-level fields ---
	if ingestRec.Agent != "senior-editor" {
		t.Errorf("Agent = %q, want %q", ingestRec.Agent, "senior-editor")
	}
	if ingestRec.ProjectID != "project-alpha" {
		t.Errorf("ProjectID = %q, want %q", ingestRec.ProjectID, "project-alpha")
	}
	if ingestRec.WorkspaceID != "workspace-bravo" {
		t.Errorf("WorkspaceID = %q, want %q", ingestRec.WorkspaceID, "workspace-bravo")
	}
	if ingestRec.ParentSessionID != "parent-session-42" {
		t.Errorf("ParentSessionID = %q, want %q", ingestRec.ParentSessionID, "parent-session-42")
	}

	// --- Enrichment: finish reason ---
	if ingestRec.FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want %q", ingestRec.FinishReason, "stop")
	}

	// --- Enrichment: reasoning tokens ---
	if ingestRec.ReasoningTokens != 75 {
		t.Errorf("ReasoningTokens = %d, want %d", ingestRec.ReasoningTokens, 75)
	}

	// --- Token counts (core + enrichment) ---
	if ingestRec.InputTokens != 500 {
		t.Errorf("InputTokens = %d, want %d", ingestRec.InputTokens, 500)
	}
	if ingestRec.OutputTokens != 200 {
		t.Errorf("OutputTokens = %d, want %d", ingestRec.OutputTokens, 200)
	}

	// --- Cache split fields ---
	if ingestRec.CacheReadTokens != 100 {
		t.Errorf("CacheReadTokens = %d, want %d", ingestRec.CacheReadTokens, 100)
	}
	if ingestRec.CacheWriteTokens != 50 {
		t.Errorf("CacheWriteTokens = %d, want %d", ingestRec.CacheWriteTokens, 50)
	}

	// --- Backward-compatible CachedTokens (sum of cache read + write) ---
	expectedCached := int64(100 + 50)
	if ingestRec.CachedTokens != expectedCached {
		t.Errorf("CachedTokens = %d, want %d", ingestRec.CachedTokens, expectedCached)
	}

	// --- Cost ---
	if ingestRec.EstimatedCostUSD == nil {
		t.Fatal("EstimatedCostUSD is nil, want non-nil for non-zero cost")
	}
	if *ingestRec.EstimatedCostUSD != "0.0125" {
		t.Errorf("EstimatedCostUSD = %q, want %q", *ingestRec.EstimatedCostUSD, "0.0125")
	}

	// --- Timestamp ---
	expectedTime := time.UnixMilli(1700001000000)
	if ingestRec.ReportedAt != expectedTime.Format(time.RFC3339) {
		t.Errorf("ReportedAt = %q, want %q", ingestRec.ReportedAt, expectedTime.Format(time.RFC3339))
	}

	// --- Verify the sqlite record's enrichment fields came through correctly ---
	// Check that session-level enrichment made it through the reader.
	if sqlRec.Agent != "senior-editor" {
		t.Errorf("sqlite.Agent = %q, want %q", sqlRec.Agent, "senior-editor")
	}
	if sqlRec.SourceProjectID != "project-alpha" {
		t.Errorf("sqlite.SourceProjectID = %q, want %q", sqlRec.SourceProjectID, "project-alpha")
	}
	if sqlRec.WorkspaceID != "workspace-bravo" {
		t.Errorf("sqlite.WorkspaceID = %q, want %q", sqlRec.WorkspaceID, "workspace-bravo")
	}
	if sqlRec.ParentSessionID != "parent-session-42" {
		t.Errorf("sqlite.ParentSessionID = %q, want %q", sqlRec.ParentSessionID, "parent-session-42")
	}
	if sqlRec.FinishReason != "stop" {
		t.Errorf("sqlite.FinishReason = %q, want %q", sqlRec.FinishReason, "stop")
	}
	if sqlRec.TokensReasoning != 75 {
		t.Errorf("sqlite.TokensReasoning = %d, want %d", sqlRec.TokensReasoning, 75)
	}
	if sqlRec.TokensCacheRead != 100 {
		t.Errorf("sqlite.TokensCacheRead = %d, want %d", sqlRec.TokensCacheRead, 100)
	}
	if sqlRec.TokensCacheWrite != 50 {
		t.Errorf("sqlite.TokensCacheWrite = %d, want %d", sqlRec.TokensCacheWrite, 50)
	}
	if sqlRec.TokensTotal != 925 {
		t.Errorf("sqlite.TokensTotal = %d, want %d", sqlRec.TokensTotal, 925)
	}
	if sqlRec.OpenCodeReportedCost != 0.0125 {
		t.Errorf("sqlite.OpenCodeReportedCost = %f, want %f", sqlRec.OpenCodeReportedCost, 0.0125)
	}
}

// TestFullPipeline_ZeroCost tests the pipeline with a zero-cost record to
// verify EstimatedCostUSD nil handling survives the full pipeline.
func TestFullPipeline_ZeroCost(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "zero_cost.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE message (
		id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT
	)`); err != nil {
		t.Fatalf("create message: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE session (
		id TEXT, time_created INTEGER, time_updated INTEGER,
		project_id TEXT, parent_id TEXT, workspace_id TEXT, agent TEXT, model TEXT
	)`); err != nil {
		t.Fatalf("create session: %v", err)
	}

	// Session with empty enrichment fields.
	if _, err := db.Exec(`INSERT INTO session
		(id, time_created, time_updated, project_id, parent_id, workspace_id, agent, model)
		VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL)`,
		"sess-minimal", int64(1700000000000), int64(1700000000000),
	); err != nil {
		t.Fatalf("insert session: %v", err)
	}

	// Message with zero cost and no enrichment tokens.
	zeroCostJSON := `{
		"providerID": "local",
		"modelID": "llama-3.1-8b",
		"cost": 0,
		"finish": "length",
		"mode": "chat",
		"tokens": {
			"input": 50,
			"output": 25,
			"total": 75
		}
	}`

	if _, err := db.Exec(`INSERT INTO message
		(id, session_id, time_created, time_updated, data)
		VALUES (?, ?, ?, ?, ?)`,
		"msg-zero-cost", "sess-minimal", int64(1700001000000), int64(1700001000000), zeroCostJSON,
	); err != nil {
		t.Fatalf("insert message: %v", err)
	}

	reader, err := sqlite.NewOpenCodeReader(dbPath)
	if err != nil {
		t.Fatalf("NewOpenCodeReader: %v", err)
	}
	defer reader.Close()

	records, err := reader.ReadRecords(time.UnixMilli(0), 100)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	gwRec := sqliteToGatewayUsageRecord(records[0])
	ingestRec := MapToIngestRecord(gwRec)

	// Zero cost → nil EstimatedCostUSD.
	if ingestRec.EstimatedCostUSD != nil {
		t.Errorf("EstimatedCostUSD = %q, want nil for zero cost", *ingestRec.EstimatedCostUSD)
	}

	// Missing enrichment fields should be zero values.
	if ingestRec.ReasoningTokens != 0 {
		t.Errorf("ReasoningTokens = %d, want 0", ingestRec.ReasoningTokens)
	}
	if ingestRec.CacheReadTokens != 0 {
		t.Errorf("CacheReadTokens = %d, want 0", ingestRec.CacheReadTokens)
	}
	if ingestRec.CacheWriteTokens != 0 {
		t.Errorf("CacheWriteTokens = %d, want 0", ingestRec.CacheWriteTokens)
	}
	if ingestRec.CachedTokens != 0 {
		t.Errorf("CachedTokens = %d, want 0", ingestRec.CachedTokens)
	}
	if ingestRec.FinishReason != "length" {
		t.Errorf("FinishReason = %q, want %q", ingestRec.FinishReason, "length")
	}
	if ingestRec.Agent != "" {
		t.Errorf("Agent = %q, want empty", ingestRec.Agent)
	}
	if ingestRec.ProjectID != "" {
		t.Errorf("ProjectID = %q, want empty", ingestRec.ProjectID)
	}
	if ingestRec.WorkspaceID != "" {
		t.Errorf("WorkspaceID = %q, want empty", ingestRec.WorkspaceID)
	}
	if ingestRec.ParentSessionID != "" {
		t.Errorf("ParentSessionID = %q, want empty", ingestRec.ParentSessionID)
	}
	if ingestRec.SessionID != "sess-minimal" {
		t.Errorf("SessionID = %q, want %q", ingestRec.SessionID, "sess-minimal")
	}
	if ingestRec.Provider != "local" {
		t.Errorf("Provider = %q, want %q", ingestRec.Provider, "local")
	}
	if ingestRec.InputTokens != 50 {
		t.Errorf("InputTokens = %d, want 50", ingestRec.InputTokens)
	}
	if ingestRec.OutputTokens != 25 {
		t.Errorf("OutputTokens = %d, want 25", ingestRec.OutputTokens)
	}
}

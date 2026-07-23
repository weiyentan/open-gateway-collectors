package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/opencode-gateway/collectors/internal/config"
	"github.com/opencode-gateway/collectors/internal/gateway"
	"github.com/opencode-gateway/collectors/internal/sqlite"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// createTestDB creates a minimal OpenCode SQLite source database at
// dir/name.db with the required message and session tables.
func createTestDB(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name+".db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS message (
		id TEXT,
		session_id TEXT,
		time_created INTEGER,
		time_updated INTEGER,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("create message table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS session (
		id TEXT,
		time_created INTEGER,
		time_updated INTEGER,
		project_id TEXT,
		parent_id TEXT,
		workspace_id TEXT,
		agent TEXT
	)`)
	if err != nil {
		t.Fatalf("create session table: %v", err)
	}

	return path
}

// testConfig returns a minimal valid config for testing.
func testConfig(baseURL string) *config.Config {
	return &config.Config{
		Token:             "test-token",
		BaseURL:           baseURL,
		PollInterval:      100 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond,
		LogLevel:          "debug",
		CursorDir:         "",
	}
}

// mockReader implements sqlite.Reader for testing.
type mockReader struct {
	records []sqlite.UsageRecord
	err     error
}

func (m *mockReader) ReadRecords(since time.Time, limit int) ([]sqlite.UsageRecord, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []sqlite.UsageRecord
	for _, r := range m.records {
		if r.OccurredAt.After(since) {
			result = append(result, r)
		}
	}
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// makeRecords creates test sqlite.UsageRecord slices.
func makeRecords(ids []string, baseTime time.Time) []sqlite.UsageRecord {
	var out []sqlite.UsageRecord
	for i, id := range ids {
		out = append(out, sqlite.UsageRecord{
			SourceRecordID:       id,
			SourceSessionID:      "sess-" + id,
			ModelID:              "gpt-4",
			TokensInput:          int64(100 + i),
			TokensOutput:         int64(50 + i),
			TokensCacheRead:      int64(10),
			TokensCacheWrite:     int64(5),
			OpenCodeReportedCost: 0.003,
			OccurredAt:           baseTime.Add(time.Duration(i) * time.Second),
		})
	}
	return out
}

// gatewayServer creates a test Gateway server that returns success responses
// and captures the last received request.
func gatewayServer(status int, resp gateway.IngestResponse) (*httptest.Server, *atomic.Value) {
	var lastReq atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req gateway.IngestRequest
		json.NewDecoder(r.Body).Decode(&req)
		lastReq.Store(req)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(resp)
	}))
	return srv, &lastReq
}

// ---------------------------------------------------------------------------
// Tests: Database resolution
// ---------------------------------------------------------------------------

func TestCollector_ResolveDatabases_SinglePath(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := config.Config{
		Token:      "tok",
		BaseURL:    "http://localhost",
		SQLitePath: dbPath,
		LogLevel:   "debug",
		CursorDir:  dir,
		PollInterval:       60 * time.Second,
		HeartbeatInterval:  120 * time.Second,
	}

	c, err := NewCollector(&cfg, "test")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}
	if len(dbs) != 1 {
		t.Fatalf("expected 1 database, got %d", len(dbs))
	}
	if dbs[0].path != dbPath {
		t.Errorf("path = %q, want %q", dbs[0].path, dbPath)
	}
	if dbs[0].id == "" {
		t.Error("identity id is empty")
	}
}

func TestCollector_ResolveDatabases_SkipsNonDB(t *testing.T) {
	dir := t.TempDir()
	createTestDB(t, dir, "good")

	// Create a file that is not a SQLite database.
	badPath := filepath.Join(dir, "bad.db")
	if err := os.WriteFile(badPath, []byte("not a database"), 0o644); err != nil {
		t.Fatalf("write bad file: %v", err)
	}

	cfg := config.Config{
		Token:      "tok",
		BaseURL:    "http://localhost",
		SQLiteDir:  dir,
		LogLevel:   "debug",
		CursorDir:  dir,
		PollInterval:       60 * time.Second,
		HeartbeatInterval:  120 * time.Second,
	}

	c, err := NewCollector(&cfg, "test")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}
	if len(dbs) != 1 {
		t.Fatalf("expected 1 database (bad one skipped), got %d", len(dbs))
	}
}

// ---------------------------------------------------------------------------
// Tests: Record sending
// ---------------------------------------------------------------------------

func TestCollector_SendsRecordsAndUpdatesCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-001",
		AcceptedCount: 2,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Inject mock reader returning 2 records.
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
	}
	c.newReader = func(_ string) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}
	if len(dbs) != 1 {
		t.Fatalf("expected 1 DB, got %d", len(dbs))
	}

	c.processDatabase(context.Background(), dbs[0])

	// Verify the Gateway received the batch.
	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no request received by gateway")
	}
	if req.SchemaVersion != "1.0" {
		t.Errorf("SchemaVersion = %q, want %q", req.SchemaVersion, "1.0")
	}
	if req.CollectorVersion != "0.1.0" {
		t.Errorf("CollectorVersion = %q, want %q", req.CollectorVersion, "0.1.0")
	}
	if len(req.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(req.Records))
	}

	// Verify cursor was updated to the later record's timestamp.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	expectedCursor := now.Add(1 * time.Second) // rec-2 has +1s offset
	if !cursor.Equal(expectedCursor) {
		t.Errorf("cursor = %v, want %v", cursor, expectedCursor)
	}

	// Verify lastSuccess was recorded.
	c.mu.Lock()
	ls, exists := c.lastSuccess[dbPath]
	c.mu.Unlock()
	if !exists {
		t.Error("lastSuccess not recorded after successful send")
	}
	if time.Since(ls) > time.Second {
		t.Errorf("lastSuccess too old: %v", ls)
	}
}

func TestCollector_CursorNotUpdatedOnFailure(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	// Gateway returns 500.
	srv, _ := gatewayServer(http.StatusInternalServerError, gateway.IngestResponse{})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Set a known cursor.
	initialCursor := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, initialCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
	}
	c.newReader = func(_ string) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	// Cursor must NOT have advanced.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.Equal(initialCursor) {
		t.Errorf("cursor advanced to %v on failure, want %v", cursor, initialCursor)
	}
}

func TestCollector_ClientHostnameSetOnRequest(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-002",
		AcceptedCount: 1,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	// Note: hostname is resolved from os.Hostname() — just verify it's not empty.

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1"}, now),
	}
	c.newReader = func(_ string) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no request received")
	}
	if req.ClientHostname == "" {
		t.Error("ClientHostname is empty — should be set by gateway client")
	}
}

// ---------------------------------------------------------------------------
// Tests: Heartbeat
// ---------------------------------------------------------------------------

func TestCollector_HeartbeatSentAfterInterval(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "heartbeat-001",
		AcceptedCount: 0,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.HeartbeatInterval = 10 * time.Millisecond // short interval for test

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Record a prior success so heartbeat is allowed.
	c.mu.Lock()
	c.lastSuccess[dbPath] = time.Now().Add(-100 * time.Millisecond)
	c.mu.Unlock()

	// No records from reader.
	mock := &mockReader{}
	c.newReader = func(_ string) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	// Verify heartbeat was sent (empty records).
	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no heartbeat request received")
	}
	if len(req.Records) != 0 {
		t.Errorf("expected 0 records in heartbeat, got %d", len(req.Records))
	}
	if req.SchemaVersion != "1.0" {
		t.Errorf("SchemaVersion = %q, want %q", req.SchemaVersion, "1.0")
	}
}

func TestCollector_HeartbeatSkippedWithoutPriorSuccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	var reqReceived int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqReceived, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(gateway.IngestResponse{BatchID: "hb"})
	}))

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.HeartbeatInterval = 10 * time.Millisecond

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// No prior success recorded.

	mock := &mockReader{}
	c.newReader = func(_ string) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	if n := atomic.LoadInt32(&reqReceived); n != 0 {
		t.Errorf("expected 0 requests (heartbeat skipped), got %d", n)
	}
}

func TestCollector_HeartbeatSkippedWhenIntervalNotElapsed(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	var reqReceived int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqReceived, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(gateway.IngestResponse{BatchID: "hb"})
	}))

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.HeartbeatInterval = 1 * time.Hour // very long interval

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Prior success recorded just now.
	c.mu.Lock()
	c.lastSuccess[dbPath] = time.Now()
	c.mu.Unlock()

	mock := &mockReader{}
	c.newReader = func(_ string) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	if n := atomic.LoadInt32(&reqReceived); n != 0 {
		t.Errorf("expected 0 requests (interval not elapsed), got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Tests: Graceful shutdown
// ---------------------------------------------------------------------------

func TestCollector_GracefulShutdown(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, _ := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-shutdown",
		AcceptedCount: 1,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.PollInterval = 1 * time.Hour // very slow poll — won't fire during test

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Mock reader with records to ensure one iteration runs.
	mock := &mockReader{
		records: makeRecords([]string{"rec-1"}, time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)),
	}
	c.newReader = func(_ string) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in goroutine.
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	// Wait for the initial iteration to complete, then cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error from Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Tests: toGatewayUsageRecord
// ---------------------------------------------------------------------------

func TestToGatewayUsageRecord_MapsCorrectly(t *testing.T) {
	sqlRec := sqlite.UsageRecord{
		SourceRecordID:       "rec-1",
		SourceSessionID:      "sess-1",
		ProviderID:           "openai",
		ModelID:              "gpt-4",
		Mode:                 "chat",
		TokensInput:          100,
		TokensOutput:         50,
		TokensCacheRead:      10,
		TokensCacheWrite:     5,
		OpenCodeReportedCost: 0.003,
		OccurredAt:           time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC),
	}

	gwRec := toGatewayUsageRecord(sqlRec)

	if gwRec.SourceRecordID != "rec-1" {
		t.Errorf("SourceRecordID = %q, want %q", gwRec.SourceRecordID, "rec-1")
	}
	if gwRec.SessionID != "sess-1" {
		t.Errorf("SessionID = %q, want %q", gwRec.SessionID, "sess-1")
	}
	if gwRec.Model != "gpt-4" {
		t.Errorf("Model = %q, want %q", gwRec.Model, "gpt-4")
	}
	if gwRec.ProviderID != "openai" {
		t.Errorf("ProviderID = %q, want %q", gwRec.ProviderID, "openai")
	}
	if gwRec.Mode != "chat" {
		t.Errorf("Mode = %q, want %q", gwRec.Mode, "chat")
	}
	if gwRec.InputTokens != 100 {
		t.Errorf("InputTokens = %d, want %d", gwRec.InputTokens, 100)
	}
	if gwRec.OutputTokens != 50 {
		t.Errorf("OutputTokens = %d, want %d", gwRec.OutputTokens, 50)
	}
	if gwRec.TokensCacheRead != 10 {
		t.Errorf("TokensCacheRead = %d, want %d", gwRec.TokensCacheRead, 10)
	}
	if gwRec.TokensCacheWrite != 5 {
		t.Errorf("TokensCacheWrite = %d, want %d", gwRec.TokensCacheWrite, 5)
	}
	if gwRec.EstimatedCostUSD != 0.003 {
		t.Errorf("EstimatedCostUSD = %f, want %f", gwRec.EstimatedCostUSD, 0.003)
	}
	expectedTime := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	if !gwRec.OccurredAt.Equal(expectedTime) {
		t.Errorf("OccurredAt = %v, want %v", gwRec.OccurredAt, expectedTime)
	}
}

// ---------------------------------------------------------------------------
// Tests: NewCollector validates hostname
// ---------------------------------------------------------------------------

func TestNewCollector_StoresHostname(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := testConfig("http://localhost:8080")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "1.0.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	host, _ := os.Hostname()
	if c.hostname != host {
		t.Errorf("hostname = %q, want %q", c.hostname, host)
	}
}

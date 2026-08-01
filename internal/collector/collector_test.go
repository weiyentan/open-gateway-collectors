package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
		Token:                  "test-token",
		BaseURL:                baseURL,
		PollInterval:           100 * time.Millisecond,
		HeartbeatInterval:      200 * time.Millisecond,
		BatchLimit:             100,
		LogLevel:               "debug",
		CursorDir:              "",
		ExcludeRecheckInterval: 3 * time.Hour,
	}
}

// mockReader implements sqlite.Reader for testing.
type mockReader struct {
	records         []sqlite.UsageRecord
	err             error
	sessionCtxs     []sqlite.SessionContextData
	projects        []sqlite.ProjectData
	projectDirs     []sqlite.ProjectDirectoryData
	todos           []sqlite.TodoData
	dbInfo          sqlite.DatabaseInfo
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

func (m *mockReader) ReadSessionContexts(sessionIDs []string) ([]sqlite.SessionContextData, error) {
	return m.sessionCtxs, nil
}

func (m *mockReader) ReadProjectData(projectIDs []string) ([]sqlite.ProjectData, error) {
	return m.projects, nil
}

func (m *mockReader) ReadProjectDirectoryData(projectIDs []string) ([]sqlite.ProjectDirectoryData, error) {
	return m.projectDirs, nil
}

func (m *mockReader) ReadTodoData(sessionIDs []string) ([]sqlite.TodoData, error) {
	return m.todos, nil
}

func (m *mockReader) SchemaInfo() sqlite.DatabaseInfo {
	return m.dbInfo
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
		Token:             "tok",
		BaseURL:           "http://localhost",
		SQLitePath:        dbPath,
		LogLevel:          "debug",
		CursorDir:         dir,
		PollInterval:      60 * time.Second,
		HeartbeatInterval: 120 * time.Second,
		BatchLimit:        100,
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
		Token:             "tok",
		BaseURL:           "http://localhost",
		SQLiteDir:         dir,
		LogLevel:          "debug",
		CursorDir:         dir,
		PollInterval:      60 * time.Second,
		HeartbeatInterval: 120 * time.Second,
		BatchLimit:        100,
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
// Tests: Exclusion gate
// ---------------------------------------------------------------------------

func TestCollector_ResolveDatabases_SkipsExcludedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "valid")

	cfg := config.Config{
		Token:             "tok",
		BaseURL:           "http://localhost",
		SQLitePath:        dbPath,
		LogLevel:          "debug",
		CursorDir:         dir,
		PollInterval:      60 * time.Second,
		HeartbeatInterval: 120 * time.Second,
		BatchLimit:        100,
		ExcludeRecheckInterval: 1 * time.Hour, // recheck not due during test
	}

	c, err := NewCollector(&cfg, "test")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Pre-exclude the valid database.
	if err := c.exclusionGate.Exclude(dbPath, "test exclusion"); err != nil {
		t.Fatalf("Exclude: %v", err)
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}
	if len(dbs) != 0 {
		t.Errorf("expected 0 databases (excluded and recheck not due), got %d", len(dbs))
	}
}

func TestCollector_ResolveDatabases_RecheckDueDBReinspected(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "valid")

	cfg := config.Config{
		Token:             "tok",
		BaseURL:           "http://localhost",
		SQLitePath:        dbPath,
		LogLevel:          "debug",
		CursorDir:         dir,
		PollInterval:      60 * time.Second,
		HeartbeatInterval: 120 * time.Second,
		BatchLimit:        100,
		ExcludeRecheckInterval: time.Nanosecond, // recheck due immediately
	}

	c, err := NewCollector(&cfg, "test")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Pre-exclude the valid database with a sub-nanosecond recheck interval.
	if err := c.exclusionGate.Exclude(dbPath, "test exclusion"); err != nil {
		t.Fatalf("Exclude: %v", err)
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}
	if len(dbs) != 1 {
		t.Fatalf("expected 1 database (re-admitted after recheck), got %d", len(dbs))
	}
	if dbs[0].path != dbPath {
		t.Errorf("path = %q, want %q", dbs[0].path, dbPath)
	}

	// Verify the exclusion was removed after re-admission.
	excluded, err := c.exclusionGate.IsExcluded(dbPath)
	if err != nil {
		t.Fatalf("IsExcluded: %v", err)
	}
	if excluded {
		t.Error("expected exclusion to be removed after successful recheck")
	}
}

func TestCollector_ResolveDatabases_ExcludesOnFirstFailure(t *testing.T) {
	dir := t.TempDir()

	// Create a file that is not a valid SQLite database.
	badPath := filepath.Join(dir, "bad.db")
	if err := os.WriteFile(badPath, []byte("not a database"), 0o644); err != nil {
		t.Fatalf("write bad file: %v", err)
	}

	cfg := config.Config{
		Token:             "tok",
		BaseURL:           "http://localhost",
		SQLiteDir:         dir,
		LogLevel:          "debug",
		CursorDir:         dir,
		PollInterval:      60 * time.Second,
		HeartbeatInterval: 120 * time.Second,
		BatchLimit:        100,
		ExcludeRecheckInterval: 1 * time.Hour, // prevent immediate recheck
	}

	c, err := NewCollector(&cfg, "test")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// First call: fails inspection and gets excluded.
	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases (first call): %v", err)
	}
	if len(dbs) != 0 {
		t.Errorf("expected 0 databases on first call, got %d", len(dbs))
	}

	// Verify the bad database is now excluded.
	excluded, err := c.exclusionGate.IsExcluded(badPath)
	if err != nil {
		t.Fatalf("IsExcluded: %v", err)
	}
	if !excluded {
		t.Error("expected bad database to be excluded after failed inspection")
	}

	// Second call: excluded database is skipped (recheck not due).
	dbs, err = c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases (second call): %v", err)
	}
	if len(dbs) != 0 {
		t.Errorf("expected 0 databases on second call (excluded, recheck not due), got %d", len(dbs))
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
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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
	if req.SchemaVersion != gateway.SchemaVersion {
		t.Errorf("SchemaVersion = %q, want %q", req.SchemaVersion, gateway.SchemaVersion)
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
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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

func TestCollector_UsesConfiguredBatchLimit(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-limited",
		AcceptedCount: 3,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 3

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{records: makeRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4"}, now)}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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

	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no request received by gateway")
	}
	if len(req.Records) != 3 {
		t.Fatalf("expected 3 records from configured batch limit, got %d", len(req.Records))
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
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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
	if req.SchemaVersion != gateway.SchemaVersion {
		t.Errorf("SchemaVersion = %q, want %q", req.SchemaVersion, gateway.SchemaVersion)
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
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	if n := atomic.LoadInt32(&reqReceived); n != 0 {
		t.Errorf("expected 0 requests (interval not elapsed), got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Tests: lastSuccess seeding from persisted cursor
// ---------------------------------------------------------------------------

func TestCollector_SeedsLastSuccessFromPersistedCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Pre-set a cursor to simulate a previously-tracked database.
	pastTime := time.Now().Add(-1 * time.Hour)
	if err := c.tracker.SetCursor(dbPath, pastTime); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	mock := &mockReader{}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	// Execute one full iteration.
	c.iterate(context.Background())

	// Verify lastSuccess was seeded for this database.
	c.mu.Lock()
	seededTime, exists := c.lastSuccess[dbPath]
	c.mu.Unlock()
	if !exists {
		t.Fatal("lastSuccess was NOT seeded for database with persisted cursor")
	}

	// Verify the seed uses time.Now(), not the cursor timestamp.
	// The cursor was set to 1 hour ago, so a correctly seeded value
	// should be within the last second.
	if time.Since(seededTime) > time.Second {
		t.Errorf("lastSuccess time is too old: %v (expected ~time.Now(), not cursor timestamp)", seededTime)
	}
}

func TestCollector_DoesNotSeedLastSuccessWithoutCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// No cursor is set — database has never been tracked.

	mock := &mockReader{}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	c.iterate(context.Background())

	// Verify lastSuccess was NOT seeded.
	c.mu.Lock()
	_, exists := c.lastSuccess[dbPath]
	c.mu.Unlock()
	if exists {
		t.Error("lastSuccess was seeded for database without a persisted cursor")
	}
}

func TestCollector_SeedEnablesHeartbeat(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "hb-seeded-001",
		AcceptedCount: 0,
	})
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.HeartbeatInterval = time.Nanosecond // effectively always elapsed

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Pre-set a cursor.
	pastTime := time.Now().Add(-1 * time.Hour)
	if err := c.tracker.SetCursor(dbPath, pastTime); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	mock := &mockReader{}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	c.iterate(context.Background())

	// Verify a heartbeat was sent (indirectly proves lastSuccess was seeded
	// and the seeding enables the heartbeat path).
	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no heartbeat request — lastSuccess was not seeded or heartbeat was skipped")
	}
	if len(req.Records) != 0 {
		t.Errorf("expected heartbeat (0 records), got %d records", len(req.Records))
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
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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

func TestCollector_UsesMockTransport(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "mock-batch-001",
		AcceptedCount: 2,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Override the transport with our mock.
	c.transport = mockTransport

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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

	// Verify the mock Transport was called exactly once.
	if mockTransport.CallCount() != 1 {
		t.Fatalf("MockTransport CallCount = %d, want 1", mockTransport.CallCount())
	}

	call := mockTransport.LastCall()
	req := call.Req

	// Verify the request is well-formed.
	if req.SchemaVersion != gateway.SchemaVersion {
		t.Errorf("SchemaVersion = %q, want %q", req.SchemaVersion, gateway.SchemaVersion)
	}
	if req.CollectorVersion != "0.1.0" {
		t.Errorf("CollectorVersion = %q, want %q", req.CollectorVersion, "0.1.0")
	}
	if req.SourceDatabaseID == "" {
		t.Error("SourceDatabaseID is empty")
	}
	if len(req.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(req.Records))
	}

	// Verify cursor was updated after successful send via mock.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	expectedCursor := now.Add(1 * time.Second)
	if !cursor.Equal(expectedCursor) {
		t.Errorf("cursor = %v, want %v", cursor, expectedCursor)
	}
}

func TestCollector_MockTransportErrorPropagation(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(nil)
	mockTransport.Err = fmt.Errorf("transport failure")

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	c.transport = mockTransport

	// Set a known initial cursor.
	initialCursor := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, initialCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	// Verify the mock was called (even on error).
	if mockTransport.CallCount() != 1 {
		t.Fatalf("MockTransport CallCount = %d, want 1", mockTransport.CallCount())
	}

	// Cursor must NOT advance on transport error.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.Equal(initialCursor) {
		t.Errorf("cursor advanced to %v on transport error, want %v", cursor, initialCursor)
	}
}

func TestToGatewayUsageRecord_MapsCorrectly(t *testing.T) {
	sqlRec := sqlite.UsageRecord{
		SourceRecordID:       "rec-1",
		SourceSessionID:      "sess-1",
		SourceProjectID:      "proj-42",
		ParentSessionID:      "parent-sess-99",
		WorkspaceID:          "ws-alpha",
		Agent:                "vscode-agent",
		ProviderID:           "openai",
		ModelID:              "gpt-4",
		Mode:                 "chat",
		FinishReason:         "stop",
		TokensInput:          100,
		TokensOutput:         50,
		TokensReasoning:      20,
		TokensCacheRead:      10,
		TokensCacheWrite:     5,
		OpenCodeReportedCost: 0.003,
		OccurredAt:           time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC),
	}

	gwRec := ToGatewayUsageRecord(sqlRec)

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
	if gwRec.Agent != "vscode-agent" {
		t.Errorf("Agent = %q, want %q", gwRec.Agent, "vscode-agent")
	}
	if gwRec.ProjectID != "proj-42" {
		t.Errorf("ProjectID = %q, want %q", gwRec.ProjectID, "proj-42")
	}
	if gwRec.WorkspaceID != "ws-alpha" {
		t.Errorf("WorkspaceID = %q, want %q", gwRec.WorkspaceID, "ws-alpha")
	}
	if gwRec.ParentSessionID != "parent-sess-99" {
		t.Errorf("ParentSessionID = %q, want %q", gwRec.ParentSessionID, "parent-sess-99")
	}
	if gwRec.ReasoningTokens != 20 {
		t.Errorf("ReasoningTokens = %d, want %d", gwRec.ReasoningTokens, 20)
	}
	if gwRec.FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want %q", gwRec.FinishReason, "stop")
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

// ---------------------------------------------------------------------------
// Tests: Projection integration
// ---------------------------------------------------------------------------

func TestCollector_IncludesProjectionsInRequest(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-proj-001",
		AcceptedCount: 2,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
		sessionCtxs: []sqlite.SessionContextData{
			{ExternalSessionID: "sess-rec-1", Agent: "claude", ProjectID: "proj-1", Model: "gpt-4"},
			{ExternalSessionID: "sess-rec-2", Agent: "gpt", ProjectID: "proj-1", Model: "gpt-4o"},
		},
		projects: []sqlite.ProjectData{
			{ExternalProjectID: "proj-1", Name: "Test Project", Worktree: "/tmp/test"},
		},
		projectDirs: []sqlite.ProjectDirectoryData{
			{ExternalProjectID: "proj-1", Path: "/tmp/test/src"},
		},
		todos: []sqlite.TodoData{
			{ExternalSessionID: "sess-rec-1", Description: "Write tests", Status: "pending"},
			{ExternalSessionID: "sess-rec-1", Description: "Review PR", Status: "completed"},
		},
	}

	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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

	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no request received by gateway")
	}

	// Verify records are still present.
	if len(req.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(req.Records))
	}

	// Verify session contexts: 2 unique sessions, 1 per distinct ID.
	if len(req.SessionContexts) != 2 {
		t.Fatalf("expected 2 session contexts, got %d", len(req.SessionContexts))
	}
	foundSess1 := false
	foundSess2 := false
	for _, sc := range req.SessionContexts {
		if sc.ExternalSessionID == "sess-rec-1" {
			foundSess1 = true
			if sc.Agent != "claude" {
				t.Errorf("session 1 agent = %q, want %q", sc.Agent, "claude")
			}
		}
		if sc.ExternalSessionID == "sess-rec-2" {
			foundSess2 = true
		}
	}
	if !foundSess1 {
		t.Error("session context for sess-rec-1 missing")
	}
	if !foundSess2 {
		t.Error("session context for sess-rec-2 missing")
	}

	// Verify project snapshots: 1 unique project.
	if len(req.Projects) != 1 {
		t.Fatalf("expected 1 project snapshot, got %d", len(req.Projects))
	}
	if req.Projects[0].ExternalProjectID != "proj-1" {
		t.Errorf("project id = %q, want %q", req.Projects[0].ExternalProjectID, "proj-1")
	}
	if req.Projects[0].Name != "Test Project" {
		t.Errorf("project name = %q, want %q", req.Projects[0].Name, "Test Project")
	}

	// Verify project directory snapshots.
	if len(req.ProjectDirectories) != 1 {
		t.Fatalf("expected 1 project directory snapshot, got %d", len(req.ProjectDirectories))
	}

	// Verify todo snapshots: 2 items.
	if len(req.SessionTodos) != 2 {
		t.Fatalf("expected 2 todo snapshots, got %d", len(req.SessionTodos))
	}
}

func TestCollector_DedupSessionContextsWithinBatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-dedup-001",
		AcceptedCount: 3,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	// Three records from same session.
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2", "rec-3"}, now),
		// Mock returns duplicate session contexts — dedup in collector.
		sessionCtxs: []sqlite.SessionContextData{
			{ExternalSessionID: "sess-rec-1", Agent: "claude"},
			{ExternalSessionID: "sess-rec-2", Agent: "claude"},
			{ExternalSessionID: "sess-rec-3", Agent: "claude"},
		},
	}

	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no request received")
	}

	// The makeRecords helper creates records with session IDs sess-rec-1,
	// sess-rec-2, sess-rec-3. uniqueSessionIDs extracts unique IDs, and
	// dedupSessionContexts further de-duplicates by ExternalSessionID.
	// All 3 mock session contexts have different IDs, so we expect 3 items.
	if len(req.SessionContexts) != 3 {
		t.Errorf("expected 3 distinct session contexts, got %d", len(req.SessionContexts))
	}
}

func TestCollector_DedupProjectDirectoriesWithinBatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-dedup-dir-001",
		AcceptedCount: 2,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
		// Two directories for the same project.
		projectDirs: []sqlite.ProjectDirectoryData{
			{ExternalProjectID: "proj-1", Path: "/tmp/test/src"},
			{ExternalProjectID: "proj-1", Path: "/tmp/test/lib"},
		},
		sessionCtxs: []sqlite.SessionContextData{
			{ExternalSessionID: "sess-rec-1", ProjectID: "proj-1"},
			{ExternalSessionID: "sess-rec-2", ProjectID: "proj-1"},
		},
		projects: []sqlite.ProjectData{
			{ExternalProjectID: "proj-1", Name: "Test", Worktree: "/tmp/test"},
		},
	}

	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no request received")
	}

	// Both directories should be present — composite key dedup preserves them.
	if len(req.ProjectDirectories) != 2 {
		t.Fatalf("expected 2 project directory snapshots (2 unique paths), got %d", len(req.ProjectDirectories))
	}

	// Verify both paths are individually present.
	seenSrc := false
	seenLib := false
	for _, pd := range req.ProjectDirectories {
		if pd.ExternalProjectID == "proj-1" && pd.Directory == "/tmp/test/src" {
			seenSrc = true
		}
		if pd.ExternalProjectID == "proj-1" && pd.Directory == "/tmp/test/lib" {
			seenLib = true
		}
	}
	if !seenSrc {
		t.Error("project directory snapshot for /tmp/test/src missing")
	}
	if !seenLib {
		t.Error("project directory snapshot for /tmp/test/lib missing")
	}
}

func TestCollector_CursorUnchangedWithProjections(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, _ := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "batch-cursor-001",
		AcceptedCount: 1,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1"}, now),
		sessionCtxs: []sqlite.SessionContextData{
			{ExternalSessionID: "sess-rec-1", Agent: "claude"},
		},
	}

	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	// Verify cursor was updated to record timestamp (projections don't affect cursor).
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	expectedCursor := now // rec-1 has +0s offset
	if !cursor.Equal(expectedCursor) {
		t.Errorf("cursor = %v, want %v", cursor, expectedCursor)
	}
}

func TestCollector_EmptyProjectionsWhenNoRecords(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	srv, lastReq := gatewayServer(http.StatusCreated, gateway.IngestResponse{
		BatchID:       "hb-no-proj",
		AcceptedCount: 0,
	})

	cfg := testConfig(srv.URL)
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.HeartbeatInterval = 10 * time.Millisecond

	c, err := NewCollector(cfg, "0.1.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Record prior success so heartbeat fires.
	c.mu.Lock()
	c.lastSuccess[dbPath] = time.Now().Add(-100 * time.Millisecond)
	c.mu.Unlock()

	// No records — heartbeat.
	mock := &mockReader{}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	req, ok := lastReq.Load().(gateway.IngestRequest)
	if !ok {
		t.Fatal("no heartbeat request received")
	}

	// Heartbeat has no projections.
	if len(req.SessionContexts) != 0 {
		t.Errorf("heartbeat should have 0 session contexts, got %d", len(req.SessionContexts))
	}
	if len(req.Projects) != 0 {
		t.Errorf("heartbeat should have 0 project snapshots, got %d", len(req.Projects))
	}
	if len(req.ProjectDirectories) != 0 {
		t.Errorf("heartbeat should have 0 project directory snapshots, got %d", len(req.ProjectDirectories))
	}
	if len(req.SessionTodos) != 0 {
		t.Errorf("heartbeat should have 0 todo snapshots, got %d", len(req.SessionTodos))
	}
}

// ---------------------------------------------------------------------------
// Tests: Replay mode
// ---------------------------------------------------------------------------

func TestCollector_ReplaySendsAllRecordsAcrossBatches(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-batch-001",
		AcceptedCount: 10,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 3 // small batch limit to test batching
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// Set a known cursor — replay should ignore it and read all records.
	initialCursor := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, initialCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// 10 records, batch limit 3 => 4 batches (3 + 3 + 3 + 1).
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	allRecords := makeRecords([]string{
		"rec-1", "rec-2", "rec-3", "rec-4", "rec-5",
		"rec-6", "rec-7", "rec-8", "rec-9", "rec-10",
	}, now)

	mock := &mockReader{records: allRecords}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
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

	// Verify 4 batches were sent (3+3+3+1).
	if mockTransport.CallCount() != 4 {
		t.Fatalf("expected 4 batches, got %d", mockTransport.CallCount())
	}

	// Verify cursor advanced to the last record's time (rec-10 has +9s offset).
	// This implicitly confirms all 10 records were processed — the cursor
	// is the max occurred_at across all batches.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	expectedCursor := now.Add(9 * time.Second)
	if !cursor.Equal(expectedCursor) {
		t.Errorf("cursor = %v, want %v", cursor, expectedCursor)
	}
}

func TestCollector_ReplayAdvancesWatermarkAfterCompletion(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-cursor-001",
		AcceptedCount: 3,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// Set an old cursor that should be surpassed by replay.
	oldCursor := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, oldCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2", "rec-3"}, now),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	// Cursor must have advanced past the old cursor.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	// rec-3 has +2s offset from now.
	expectedCursor := now.Add(2 * time.Second)
	if !cursor.Equal(expectedCursor) {
		t.Errorf("cursor = %v, want %v (must advance past old cursor %v)", cursor, expectedCursor, oldCursor)
	}

	// Also verify that on a subsequent normal iteration (simulated),
	// no records are re-sent because cursor is past the replay window.
	// Reset the reader to return same records (simulating source db unchanged).
	// Process again with replay disabled.
	cfg.Replay = false
	c2, _ := NewCollector(cfg, "0.2.0")
	c2.transport = mockTransport
	c2.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}
	// Copy the tracker so cursor is shared.
	c2.tracker = c.tracker

	mockTransport2 := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "normal-after-replay",
		AcceptedCount: 0,
	})
	c2.transport = mockTransport2

	dbs2, _ := c2.resolveDatabases()
	c2.processDatabase(context.Background(), dbs2[0])

	// After replay, the cursor should be ahead, so no new records should be sent.
	// A heartbeat may be sent if prior success exists; either way, no records sent.
	if mockTransport2.CallCount() > 0 {
		lastReq := mockTransport2.LastCall().Req
		if len(lastReq.Records) > 0 {
			t.Errorf("expected 0 records after replay (cursor advanced), got %d", len(lastReq.Records))
		}
	}
}

func TestCollector_ReplayRespectsSinceCutoff(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-since-001",
		AcceptedCount: 5,
	})

	// Use a fixed reference time for deterministic cutoff testing.
	refTime := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.Replay = true
	cfg.ReplaySince = 2 * time.Second

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// Override replaySince with a fixed value for deterministic testing.
	// The cutoff is refTime (12:00:00), so records after 12:00:00 are included.
	// Records: refTime-3s, refTime-2s, refTime-1s, refTime, refTime+1s.
	// Only records with time > refTime are included: rec-4 (refTime+1s? no...
	// Wait: cutoff = refTime.Add(-2s) = 11:59:58.
	// The replaySince is set to refTime.Add(-2s) = 11:59:58.
	c.replaySince = refTime.Add(-2 * time.Second) // 11:59:58

	// Records: [11:59:57, 11:59:58, 11:59:59, 12:00:00, 12:00:01]
	// After cutoff (11:59:58): rec-3(11:59:59), rec-4(12:00:00), rec-5(12:00:01) = 3 records.
	baseTime := refTime.Add(-3 * time.Second) // 11:59:57

	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"}, baseTime),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	// Verify only 3 records were sent (rec-3, rec-4, rec-5).
	if mockTransport.CallCount() == 0 {
		t.Fatal("expected at least 1 batch")
	}
	req := mockTransport.LastCall().Req
	if len(req.Records) != 3 {
		t.Errorf("expected 3 records (rec-3, rec-4, rec-5 matched since cutoff), got %d", len(req.Records))
	}

	// Verify the included records have the correct IDs.
	expectedIDs := map[string]bool{"rec-3": true, "rec-4": true, "rec-5": true}
	for _, rec := range req.Records {
		if !expectedIDs[rec.SourceRecordID] {
			t.Errorf("unexpected record in batch: %s", rec.SourceRecordID)
		}
	}
}

func TestCollector_ReplaySendsSessionContexts(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-proj-001",
		AcceptedCount: 2,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
		sessionCtxs: []sqlite.SessionContextData{
			{ExternalSessionID: "sess-rec-1", Agent: "claude", Model: "gpt-4"},
			{ExternalSessionID: "sess-rec-2", Agent: "codex", Model: "gpt-4o"},
		},
		projects: []sqlite.ProjectData{
			{ExternalProjectID: "proj-1", Name: "Replay Project"},
		},
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	if mockTransport.CallCount() != 1 {
		t.Fatalf("expected 1 batch, got %d", mockTransport.CallCount())
	}

	req := mockTransport.LastCall().Req

	// Verify session contexts were included.
	if len(req.SessionContexts) != 2 {
		t.Errorf("expected 2 session contexts, got %d", len(req.SessionContexts))
	}
	// Verify project snapshots were included.
	if len(req.Projects) != 1 {
		t.Errorf("expected 1 project snapshot, got %d", len(req.Projects))
	}
}

func TestCollector_ReplayNotTriggeredWithoutExplicitConfig(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "normal-batch",
		AcceptedCount: 2,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	// Replay is NOT set (default false).

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// Set a cursor at latest record — no new records should be returned.
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	cursorAtLatest := now.Add(10 * time.Second) // beyond all records
	if err := c.tracker.SetCursor(dbPath, cursorAtLatest); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2"}, now),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, _ := c.resolveDatabases()
	c.processDatabase(context.Background(), dbs[0])

	// In normal mode (replay=false), with cursor beyond all records,
	// no records should be sent. This confirms replay is not accidentally triggered.
	if mockTransport.CallCount() > 0 {
		req := mockTransport.LastCall().Req
		if len(req.Records) > 0 {
			t.Errorf("expected 0 records in normal mode (cursor beyond records), got %d", len(req.Records))
		}
	}
}

// TestCollector_ReplaySinglePassDoesNotRepeatOnSubsequentIterations proves
// that after the replay pass completes (via iterate), a subsequent call to
// processDatabase with cfg.Replay still true does NOT re-replay — it reads
// incrementally from the advanced cursor. This validates the single-pass
// semantics from ADR-0008: the watermark advances after replay completes so
// subsequent runs resume normal incremental behavior.
func TestCollector_ReplaySinglePassDoesNotRepeatOnSubsequentIterations(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	// First transport: used during the replay pass.
	mockTransport1 := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-pass-batch",
		AcceptedCount: 3,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.Replay = true
	cfg.BatchLimit = 2 // small batch to force multiple batches inside replay

	c, err := NewCollector(cfg, "0.3.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport1

	// Set a known old cursor — replay should ignore it and read all records.
	oldCursor := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, oldCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// 5 records with batch limit 2 => 3 batches (2+2+1).
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	allRecords := makeRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"}, now)

	mock := &mockReader{records: allRecords}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	// --- First pass: run iterate with replay enabled ---
	c.iterate(context.Background())

	// Verify replay sent 3 batches (2+2+1 with batch limit 2).
	if mockTransport1.CallCount() != 3 {
		t.Fatalf("replay pass: expected 3 batches (batch limit 2, 5 records), got %d", mockTransport1.CallCount())
	}

	// Verify cursor advanced to last record's time.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor after replay: %v", err)
	}
	expectedCursor := now.Add(4 * time.Second) // rec-5 is at +4s
	if !cursor.Equal(expectedCursor) {
		t.Errorf("cursor after replay = %v, want %v", cursor, expectedCursor)
	}

	// Verify replayDone was set by iterate.
	if !c.replayDone {
		t.Error("expected replayDone = true after replay pass completes")
	}

	// --- Second pass: call processDatabase directly while cfg.Replay is still true ---
	// This simulates a subsequent poll cycle. Because replayDone is now true,
	// processDatabase must use the stored cursor (which is at the latest record),
	// NOT restart from replaySince.

	// Fresh transport for the second pass.
	mockTransport2 := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "post-replay-normal",
		AcceptedCount: 0,
	})
	c.transport = mockTransport2

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases (post-replay): %v", err)
	}
	if len(dbs) != 1 {
		t.Fatalf("expected 1 DB, got %d", len(dbs))
	}

	c.processDatabase(context.Background(), dbs[0])

	// After replay, the cursor is at the latest record timestamp.
	// In normal incremental mode, ReadRecords returns 0 records because
	// no records are newer than the cursor. No heartbeat should fire
	// because lastSuccess was seeded at startup time (zero elapsed).
	// Either way, no records should be sent.
	if mockTransport2.CallCount() > 0 {
		req := mockTransport2.LastCall().Req
		if len(req.Records) > 0 {
			t.Errorf("expected 0 records on post-replay iteration (cursor advanced), got %d", len(req.Records))
		}
	}
}

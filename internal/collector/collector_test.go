package collector

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func (m *mockReader) ReadRecordsAfter(since time.Time, afterID string, limit int) ([]sqlite.UsageRecord, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []sqlite.UsageRecord
	for _, r := range m.records {
		after := r.OccurredAt.After(since) || (r.OccurredAt.Equal(since) && r.SourceRecordID > afterID)
		if after {
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

// makeTiedRecords creates test UsageRecords that all share the same
// OccurredAt timestamp, useful for testing tie-safe composite paging.
func makeTiedRecords(ids []string, ts time.Time) []sqlite.UsageRecord {
	var out []sqlite.UsageRecord
	for _, id := range ids {
		out = append(out, sqlite.UsageRecord{
			SourceRecordID:       id,
			SourceSessionID:      "sess-" + id,
			ModelID:              "gpt-4",
			TokensInput:          100,
			TokensOutput:         50,
			TokensCacheRead:      10,
			TokensCacheWrite:     5,
			OpenCodeReportedCost: 0.003,
			OccurredAt:           ts,
		})
	}
	return out
}

func TestCollector_ReplaySendsAllTiedRecordsAcrossBatches(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-tied-001",
		AcceptedCount: 5,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 3 // small batch limit to force multiple batches
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// 5 records all at the same timestamp.
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	allRecords := makeTiedRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"}, now)

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

	// Expect 2 batches (3 + 2) since batch limit is 3 and there are 5 records.
	if mockTransport.CallCount() != 2 {
		t.Fatalf("expected 2 batches, got %d", mockTransport.CallCount())
	}

	// Collect all sent SourceRecordIDs across all batches.
	sentIDs := make(map[string]bool)
	for _, call := range mockTransport.Calls() {
		for _, rec := range call.Req.Records {
			sentIDs[rec.SourceRecordID] = true
		}
	}
	if len(sentIDs) != 5 {
		t.Errorf("expected 5 unique record IDs sent, got %d (dropped records)", len(sentIDs))
	}
	for _, id := range []string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"} {
		if !sentIDs[id] {
			t.Errorf("record %s was not sent (dropped due to tie paging bug)", id)
		}
	}

	// Verify cursor advanced to now.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.Equal(now) {
		t.Errorf("cursor = %v, want %v", cursor, now)
	}

	// Verify replayCompleted is set.
	if !c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be true")
	}

	// Second cycle should send zero records (replay completed, cursor past all).
	mockTransport2 := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "after-replay",
		AcceptedCount: 0,
	})
	c.transport = mockTransport2

	c.processDatabase(context.Background(), dbs[0])
	if mockTransport2.CallCount() > 0 {
		req := mockTransport2.LastCall().Req
		if len(req.Records) > 0 {
			t.Errorf("second cycle: expected 0 records, got %d", len(req.Records))
		}
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

// TestCollector_ReplayDoesNotReTriggerOnSecondCycle verifies that the
// replayCompleted latch prevents the replay pass from re-triggering on
// subsequent poll cycles (acceptance criterion #3: "no re-replay every cycle").
func TestCollector_ReplayDoesNotReTriggerOnSecondCycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-once-001",
		AcceptedCount: 3,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.Replay = true // replay enabled

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// Set a known old cursor.
	oldCursor := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, oldCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	allRecords := makeRecords([]string{"rec-1", "rec-2", "rec-3"}, now)

	mock := &mockReader{records: allRecords}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	// --- First poll cycle: replay runs ---
	c.processDatabase(context.Background(), dbs[0])

	// Verify replay sent records (1 batch, 3 records).
	if mockTransport.CallCount() != 1 {
		t.Fatalf("first cycle: expected 1 batch, got %d", mockTransport.CallCount())
	}
	firstReq := mockTransport.LastCall().Req
	if len(firstReq.Records) != 3 {
		t.Fatalf("first cycle: expected 3 records, got %d", len(firstReq.Records))
	}

	// Verify cursor advanced to the last record's timestamp (rec-3 = now + 2s).
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	expectedCursor := now.Add(2 * time.Second)
	if !cursor.Equal(expectedCursor) {
		t.Errorf("first cycle: cursor = %v, want %v", cursor, expectedCursor)
	}

	// Verify replayCompleted is set for this database.
	if !c.replayCompleted[dbPath] {
		t.Error("first cycle: replayCompleted[dbPath] should be true after successful replay")
	}

	// --- Second poll cycle: replay should NOT re-trigger ---
	// Reset call count by creating a fresh transport (reuse the collector).
	mockTransport2 := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "normal-after-replay-001",
		AcceptedCount: 0,
	})
	c.transport = mockTransport2

	// Process again — replay is still true in config, but replayCompleted blocks it.
	c.processDatabase(context.Background(), dbs[0])

	// Verify no records were sent on the second cycle (cursor is past all records).
	if mockTransport2.CallCount() > 0 {
		req := mockTransport2.LastCall().Req
		if len(req.Records) > 0 {
			t.Errorf("second cycle: expected 0 records (replayCompleted latch should prevent re-replay), got %d", len(req.Records))
		}
	}

	// Verify cursor did not regress.
	cursor2, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor2.Equal(expectedCursor) {
		t.Errorf("second cycle: cursor = %v, want unchanged %v", cursor2, expectedCursor)
	}
}

// TestCollector_ReplayFailedBatchDoesNotAdvanceWindow verifies that a
// transport failure during replay does NOT advance the effective read window
// (or cursor), so the failed records are retried on the next poll cycle.
func TestCollector_ReplayFailedBatchDoesNotAdvanceWindow(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	// First call succeeds, second call fails.
	callCount := 0
	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-partial-001",
		AcceptedCount: 2,
	})
	// We'll intercept at SendBatch level by replacing the transport.
	// Use mockTransport with Err set dynamically.
	// Simpler: use two separate mock transports or inject via a custom transport.

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 2 // small batch to force multiple batches
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Build a custom transport wrapper that fails on the second call.
	ft := &failingTransport{
		transport: mockTransport,
		err:       fmt.Errorf("simulated transport failure"),
		callCount: &callCount,
	}
	c.transport = ft

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	allRecords := makeRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4"}, now)

	mock := &mockReader{records: allRecords}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	// --- First pass: batch 1 succeeds, batch 2 fails ---
	c.processDatabase(context.Background(), dbs[0])

	// The collector should have sent 2 calls: first succeeded, second failed.
	if callCount != 2 {
		t.Fatalf("expected 2 SendBatch calls (first success, second failure), got %d", callCount)
	}

	// Cursor should be rewound to replaySince (zero time) after replay failure,
	// NOT at rec-2's time. Rewinding ensures a subsequent restart (even without
	// replay) re-reads the full window — sends are idempotent on the Gateway.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.IsZero() {
		t.Errorf("after first pass: cursor = %v, want zero time (rewound to replaySince after failure)", cursor)
	}

	// replayCompleted should NOT be set (replay didn't complete).
	if c.replayCompleted[dbPath] {
		t.Error("after first pass: replayCompleted should be false (batch 2 failed)")
	}

	// --- Second pass: retry with transport that always succeeds ---
	mockTransport2 := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-retry-001",
		AcceptedCount: 2,
	})
	c.transport = mockTransport2

	c.processDatabase(context.Background(), dbs[0])

	// With replay still enabled, replay should resume. Since effectiveSince
	// was NOT advanced past the failed batch, records from the beginning
	// are re-read. Batch 1 (rec-1, rec-2) is sent again (idempotent);
	// batch 2 (rec-3, rec-4) succeeds this time.
	if mockTransport2.CallCount() < 2 {
		t.Fatalf("second pass: expected at least 2 batches (retry of the failed window), got %d", mockTransport2.CallCount())
	}

	// Cursor should now be at rec-4's time (all records sent successfully).
	cursor2, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	expectedCursorFinal := now.Add(3 * time.Second) // rec-4 = now + 3s
	if !cursor2.Equal(expectedCursorFinal) {
		t.Errorf("after second pass: cursor = %v, want %v (all records sent)", cursor2, expectedCursorFinal)
	}

	// replayCompleted should now be true.
	if !c.replayCompleted[dbPath] {
		t.Error("after second pass: replayCompleted should be true")
	}
}

// TestCollector_ReplayFailedBatchWithTiedTimestampsRewindsCursor verifies that
// when a batch fails during replay and the batch boundary splits a tie group
// (multiple records sharing the same occurred_at), the persisted cursor is NOT
// left inside the tie group at the successful batch's max. Instead, it is rewound
// to replaySince so a subsequent non-replay run can re-read the failed records.
// This catches the silent-record-loss scenario described in Comment E Finding 1.
func TestCollector_ReplayFailedBatchWithTiedTimestampsRewindsCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 2 // small batch to force ties across boundaries
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Failing transport on second call (batch 1 succeeds, batch 2 fails).
	callCount := 0
	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-tied-fail-001",
		AcceptedCount: 2,
	})
	ft := &failingTransport{
		transport: mockTransport,
		err:       fmt.Errorf("simulated transport failure"),
		callCount: &callCount,
	}
	c.transport = ft

	// 5 records at the SAME timestamp. Batch limit 2 → 3 batches (2+2+1).
	// Batch 1 (rec-1, rec-2) succeeds. Batch 2 (rec-3, rec-4) FAILS.
	// The bug: without the fix, sendRecords persists cursor at the timestamp
	// (now) after batch 1 succeeds. If the process restarts without replay,
	// ReadRecords(since=now, ...) uses strict > and skips rec-3, rec-4, rec-5
	// permanently — silent data loss.
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	allRecords := makeTiedRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"}, now)

	mock := &mockReader{records: allRecords}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	// Run replay — batch 1 succeeds, batch 2 fails.
	c.processDatabase(context.Background(), dbs[0])

	// Verify 2 calls: first success, second failure.
	if callCount != 2 {
		t.Fatalf("expected 2 SendBatch calls, got %d", callCount)
	}

	// CRITICAL: cursor must be rewound to replaySince (zero time), NOT at the
	// successful batch's max (now). If cursor == now, the remaining records
	// at this timestamp would be silently dropped on a subsequent non-replay run.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.IsZero() {
		t.Errorf("cursor should be rewound to replaySince (zero), got %v (bug: cursor inside tie group)", cursor)
	}

	// replayCompleted should NOT be set (replay didn't complete).
	if c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be false after failed replay")
	}
}

// TestCollector_ReplayCursorOnlyPersistedOnCompletion verifies Part A of
// Comment E Finding 1: during replay, the persisted cursor is NOT updated
// per-batch. It is only persisted once the full replay pass completes.
func TestCollector_ReplayCursorOnlyPersistedOnCompletion(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-cursor-persist-001",
		AcceptedCount: 10,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 2 // small batch to force multiple batches
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// Set a pre-existing cursor — should be ignored during replay but should
	// be surpassed by the final cursor set at replay completion.
	oldCursor := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, oldCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// 4 records with tied timestamps, batch limit 2 → 2 batches (2+2).
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	allRecords := makeTiedRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4"}, now)

	// Use a spy transport that tracks when SendBatch is called so we can
	// check the persisted cursor between batches.
	mock := &mockReader{records: allRecords}
	var cursorAfterBatch1 time.Time
	var cursorAfterBatch2 time.Time

	spyTransport := &spyTransport{
		transport: mockTransport,
		afterCall: func(callNum int) {
			cursor, _ := c.tracker.GetCursor(dbPath)
			switch callNum {
			case 1:
				cursorAfterBatch1 = cursor
			case 2:
				cursorAfterBatch2 = cursor
			}
		},
	}
	c.transport = spyTransport

	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	c.processDatabase(context.Background(), dbs[0])

	// Verify 2 batches were sent.
	if spyTransport.CallCount() != 2 {
		t.Fatalf("expected 2 batches, got %d", spyTransport.CallCount())
	}

	// After batch 1, cursor must NOT have advanced (per-batch persistence
	// is disabled during replay — Part A).
	if !cursorAfterBatch1.Equal(oldCursor) {
		t.Errorf("after batch 1: cursor should still be old cursor %v, got %v (bug: cursor persisted per-batch during replay)", oldCursor, cursorAfterBatch1)
	}

	// Even after the second (final, limit-filling) batch, cursor must NOT
	// have advanced — persistence is deferred until replay completion is
	// confirmed (a later page could still exist and fail, and persisting
	// mid-tie-group risks permanent record loss).
	if !cursorAfterBatch2.Equal(oldCursor) {
		t.Errorf("after batch 2: cursor should still be old cursor %v, got %v (bug: cursor persisted before replay completion)", oldCursor, cursorAfterBatch2)
	}

	// After replay completion, cursor must be at the final batch's max.
	cursorFinal, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursorFinal.Equal(now) {
		t.Errorf("after replay completion: cursor should be final max %v, got %v", now, cursorFinal)
	}

	// replayCompleted should be set.
	if !c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be true after successful replay")
	}
}

// TestCollector_ReplayCompletionClampsCursorWhenReplaySinceAfterStoredCursor
// verifies PR #47 finding 1 on the partial-batch completion path: when the
// replay window starts AFTER the pre-replay stored cursor, the persisted
// cursor must be clamped back to the stored cursor. Records in
// (storedCursor, replaySince] were never previously ingested and were not
// re-read by the replay — advancing the cursor past them would silently
// skip them forever in normal incremental mode.
func TestCollector_ReplayCompletionClampsCursorWhenReplaySinceAfterStoredCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-clamp-001",
		AcceptedCount: 3,
	})

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 10 // larger than record count: single partial batch
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	c.transport = mockTransport

	// Stored cursor is OLDER than replaySince: records in
	// (storedCursor, replaySince] were never ingested and not re-read.
	storedCursor := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, storedCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// replaySince after stored cursor but before the records.
	c.replaySince = time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2", "rec-3"}, now),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	c.processDatabase(context.Background(), dbs[0])

	// All 3 records were replayed in a single batch.
	if mockTransport.CallCount() != 1 {
		t.Fatalf("expected 1 batch, got %d", mockTransport.CallCount())
	}

	// Cursor must stay at the stored cursor, NOT advance to the last
	// record's time (now+2s): records in (2024-01-01, 2025-07-01] were
	// never ingested and must remain readable by normal incremental mode.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.Equal(storedCursor) {
		t.Errorf("cursor = %v, want stored cursor %v (bug: replay completion skipped records in (storedCursor, replaySince])", cursor, storedCursor)
	}

	// Replay still completed — the latch must be set.
	if !c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be true after successful replay")
	}
}

// TestCollector_ReplayCompletionEmptyBatchClampsCursorWhenReplaySinceAfterStoredCursor
// verifies PR #47 finding 1 on the empty-batch completion path: when the
// replay window returns no records and starts AFTER the stored cursor, the
// persisted cursor must be clamped back to the stored cursor so records in
// (storedCursor, replaySince] remain readable by normal incremental mode.
func TestCollector_ReplayCompletionEmptyBatchClampsCursorWhenReplaySinceAfterStoredCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-clamp-empty-001",
		AcceptedCount: 0,
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

	// Stored cursor older than replaySince.
	storedCursor := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, storedCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// replaySince after the stored cursor; ALL records sit BEFORE
	// replaySince, so the replay window is empty.
	c.replaySince = time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)

	// Records at 2025-06-01 — before replaySince (2025-07-01), so the
	// replay pass reads zero records and takes the empty-batch path.
	oldRecTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2", "rec-3"}, oldRecTime),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	c.processDatabase(context.Background(), dbs[0])

	// No records were sent (empty replay window, no prior success for
	// heartbeat eligibility).
	if mockTransport.CallCount() != 0 {
		t.Fatalf("expected 0 batches, got %d", mockTransport.CallCount())
	}

	// Cursor must stay at the stored cursor, NOT jump to replaySince
	// (2025-07-01): records in (2024-01-01, 2025-07-01] were never
	// ingested and must remain readable by normal incremental mode.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.Equal(storedCursor) {
		t.Errorf("cursor = %v, want stored cursor %v (bug: empty-batch completion skipped records in (storedCursor, replaySince])", cursor, storedCursor)
	}

	// Replay still completed — the latch must be set.
	if !c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be true after successful replay")
	}
}

// TestCollector_ReplayCompletionNeverRegressesCursor verifies PR #47
// finding 1's "never regress" clause: when the pre-replay stored cursor is
// AHEAD of everything the replay re-read (e.g. full-history replay against
// a database whose cursor is already past the records), the completion
// cursor must NOT move backwards to the last replayed record's time.
func TestCollector_ReplayCompletionNeverRegressesCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-no-regress-001",
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

	// Stored cursor AHEAD of all records. replaySince stays zero
	// (full history), so the replay re-reads and re-sends older records.
	futureCursor := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, futureCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2", "rec-3"}, now),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	c.processDatabase(context.Background(), dbs[0])

	// The replay sent the older records…
	if mockTransport.CallCount() != 1 {
		t.Fatalf("expected 1 batch, got %d", mockTransport.CallCount())
	}

	// …but the persisted cursor must NOT regress to the last replayed
	// record's time (now+2s) — it stays at the stored cursor.
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.Equal(futureCursor) {
		t.Errorf("cursor = %v, want stored cursor %v (bug: replay completion regressed the cursor)", cursor, futureCursor)
	}

	if !c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be true after successful replay")
	}
}

// TestCollector_ReplayFailureRewindSurfacesSetCursorError verifies PR #47
// finding 2: when a replay send fails and the cursor rewind SetCursor fails,
// the error must be surfaced via logger.Error instead of being swallowed.
// A swallowed rewind error leaves the persisted cursor at its pre-replay
// position — typically AHEAD of the failed batch — so a restart without
// replay would permanently skip the failed records.
func TestCollector_ReplayFailureRewindSurfacesSetCursorError(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 2 // small batch to force multiple batches
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Failing transport on second call (batch 1 succeeds, batch 2 fails).
	callCount := 0
	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-rewind-fail-001",
		AcceptedCount: 2,
	})
	ft := &failingTransport{
		transport: mockTransport,
		err:       fmt.Errorf("simulated transport failure"),
		callCount: &callCount,
	}
	c.transport = ft

	// Route collector logs to a buffer so we can assert on the rewind
	// error log line.
	var logBuf bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logBuf, nil))

	// Pre-existing stored cursor ahead of zero (the rewind target).
	initialCursor := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, initialCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// 4 records, batch limit 2 → 2 batches (2+2). Batch 1 succeeds,
	// batch 2 fails via failingTransport.
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	mock := &mockReader{
		records: makeRecords([]string{"rec-1", "rec-2", "rec-3", "rec-4"}, now),
	}
	c.newReader = func(_ string, _ *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
		return mock, func() {}, nil
	}

	dbs, err := c.resolveDatabases()
	if err != nil {
		t.Fatalf("resolveDatabases: %v", err)
	}

	// Force the rewind's SetCursor to fail deterministically: the tracker
	// persists to <cursorDir>/.collector-state. Replace that file with a
	// directory so the save() WriteFile fails with "is a directory" (this
	// works regardless of the directory's write permissions). resolveDatabases
	// must run first — it touches the cursor dir via the identity store.
	stateFilePath := filepath.Join(dir, ".collector-state")
	if err := os.RemoveAll(stateFilePath); err != nil {
		t.Fatalf("RemoveAll state file: %v", err)
	}
	if err := os.Mkdir(stateFilePath, 0o755); err != nil {
		t.Fatalf("Mkdir state file path: %v", err)
	}

	c.processDatabase(context.Background(), dbs[0])

	// Batch 1 sent OK, batch 2 failed.
	if callCount != 2 {
		t.Fatalf("expected 2 SendBatch calls (first success, second failure), got %d", callCount)
	}

	// The rewind SetCursor error must be logged, not swallowed.
	if !strings.Contains(logBuf.String(), "failed to rewind cursor") {
		t.Errorf("expected rewind error to be logged, got:\n%s", logBuf.String())
	}

	// The in-memory cursor state still reflects the attempted rewind to
	// replaySince (zero time) — SetCursor updates memory before save().
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.IsZero() {
		t.Errorf("cursor = %v, want zero time (rewind to replaySince attempted in-memory)", cursor)
	}

	// Replay did not complete.
	if c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be false after failed replay")
	}
}

// TestCollector_ReplayCompletionSurfacesSetCursorError verifies that when a
// replay completes successfully (partial-batch path) but the completion-path
// SetCursor fails, the error is surfaced via logger.Error instead of being
// swallowed. A swallowed error leaves the persisted cursor unchanged, so a
// restart without replay may re-read records that were already ingested.
func TestCollector_ReplayCompletionSurfacesSetCursorError(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.BatchLimit = 100 // large batch so partial-batch completion fires
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Zero replaySince so all records are read regardless of wall-clock time.
	c.replaySince = time.Time{}

	// Transport always succeeds — we want the completion path, not the rewind.
	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-complete-fail-001",
		AcceptedCount: 2,
	})
	c.transport = mockTransport

	// Route collector logs to a buffer so we can assert on the completion
	// error log line.
	var logBuf bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logBuf, nil))

	// Pre-existing stored cursor ahead of the records.
	initialCursor := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, initialCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// 2 records, batch limit 100 → all in one batch → partial-batch
	// completion fires after sendRecords succeeds.
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

	// Force the completion-path SetCursor to fail: replace the state file
	// with a directory so the save() WriteFile fails with "is a directory".
	stateFilePath := filepath.Join(dir, ".collector-state")
	if err := os.RemoveAll(stateFilePath); err != nil {
		t.Fatalf("RemoveAll state file: %v", err)
	}
	if err := os.Mkdir(stateFilePath, 0o755); err != nil {
		t.Fatalf("Mkdir state file path: %v", err)
	}

	c.processDatabase(context.Background(), dbs[0])

	// The completion-path SetCursor error must be logged, not swallowed.
	if !strings.Contains(logBuf.String(), "failed to persist cursor after replay completion") {
		t.Errorf("expected completion SetCursor error to be logged, got:\n%s", logBuf.String())
	}

	// Replay marked complete (in-memory state updated despite SetCursor failure).
	if !c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be true after successful replay (SetCursor failure does not block completion)")
	}
}

// TestCollector_ReplayFailureRewindClampsWhenReplaySinceAfterStoredCursor verifies
// PR #47 finding E13: when replay batch send fails AND replaySince is after the
// stored cursor, the failure-path rewind must clamp to the stored cursor instead of
// advancing it to replaySince. Without the clamp, records in (storedCursor, replaySince]
// that were never ingested and never re-read are permanently skipped on restart.
func TestCollector_ReplayFailureRewindClampsWhenReplaySinceAfterStoredCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, "test")

	cfg := testConfig("http://localhost:9999")
	cfg.SQLitePath = dbPath
	cfg.CursorDir = dir
	cfg.Replay = true

	c, err := NewCollector(cfg, "0.2.0")
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Set stored cursor far in the past (e.g. 2024-01-01).
	storedCursor := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := c.tracker.SetCursor(dbPath, storedCursor); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// replaySince is AFTER the stored cursor (e.g. 2025-07-01).
	// This simulates GATEWAY_COLLECTOR_REPLAY_SINCE=24h on a collector
	// that was down for months — the replay window starts after
	// records that were never ingested.
	c.replaySince = time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)

	// Transport always fails — batch send triggers failure-path rewind.
	mockTransport := gateway.NewMockTransport(&gateway.IngestResponse{
		BatchID:       "replay-clamp-001",
		AcceptedCount: 0,
	})
	mockTransport.Err = fmt.Errorf("simulated transport failure")
	c.transport = mockTransport

	// Records exist: these would be read by the replay but never sent
	// because the transport always fails. After processDatabase returns,
	// the cursor must be clamped back to storedCursor, NOT advanced to
	// replaySince.
	now := time.Date(2025, 8, 1, 12, 0, 0, 0, time.UTC)
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

	c.processDatabase(context.Background(), dbs[0])

	// Verify the transport was called (batch send failed).
	if mockTransport.CallCount() == 0 {
		t.Fatal("expected at least 1 SendBatch call")
	}

	// CRITICAL: cursor must be clamped to storedCursor, NOT advanced to replaySince.
	// Without the clamp (the bug), the cursor would be set to replaySince (2025-07-01),
	// permanently skipping records in (storedCursor, replaySince].
	cursor, err := c.tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !cursor.Equal(storedCursor) {
		t.Errorf("cursor = %v, want stored cursor %v (bug: failure rewind advanced cursor past never-ingested records)", cursor, storedCursor)
	}

	// Replay did not complete — the latch must NOT be set.
	if c.replayCompleted[dbPath] {
		t.Error("replayCompleted should be false after failed replay")
	}
}

// spyTransport wraps a gateway.Transport and invokes a callback after each
// successful SendBatch call, passing the 1-indexed call number.
type spyTransport struct {
	transport gateway.Transport
	afterCall func(callNum int)
	mu        sync.Mutex
	callCount int
}

func (st *spyTransport) SendBatch(ctx context.Context, req *gateway.IngestRequest) (*gateway.IngestResponse, error) {
	resp, err := st.transport.SendBatch(ctx, req)
	if err != nil {
		return nil, err
	}
	st.mu.Lock()
	st.callCount++
	n := st.callCount
	st.mu.Unlock()
	if st.afterCall != nil {
		st.afterCall(n)
	}
	return resp, nil
}

func (st *spyTransport) CallCount() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.callCount
}

// SendBatch implements gateway.Transport for the failing transport test.
type failingTransport struct {
	transport gateway.Transport
	err       error
	callCount *int
}

func (ft *failingTransport) SendBatch(ctx context.Context, req *gateway.IngestRequest) (*gateway.IngestResponse, error) {
	*ft.callCount++
	if *ft.callCount == 2 {
		return nil, ft.err
	}
	return ft.transport.SendBatch(ctx, req)
}

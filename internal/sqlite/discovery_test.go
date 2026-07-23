package sqlite

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestDiscoverDatabases(t *testing.T) {
	// Create a temporary directory with .db files (including nested).
	dir := t.TempDir()

	// Create .db files at various levels.
	createFile(t, filepath.Join(dir, "a.db"))
	createFile(t, filepath.Join(dir, "b.db"))
	createFile(t, filepath.Join(dir, "not-a-db.txt")) // should be skipped

	sub := filepath.Join(dir, "subdir")
	err := os.Mkdir(sub, 0o755)
	if err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	createFile(t, filepath.Join(sub, "c.db"))
	createFile(t, filepath.Join(sub, "d.db"))

	// Execute
	paths, err := DiscoverDatabases(dir)
	if err != nil {
		t.Fatalf("DiscoverDatabases failed: %v", err)
	}

	// Sort for deterministic comparison.
	sort.Strings(paths)

	if len(paths) != 4 {
		t.Fatalf("expected 4 .db files, got %d: %v", len(paths), paths)
	}

	// Verify all paths are absolute.
	for _, p := range paths {
		if !filepath.IsAbs(p) {
			t.Errorf("expected absolute path, got %s", p)
		}
	}

	// Verify the expected files exist in the result set.
	expected := []string{
		filepath.Join(dir, "a.db"),
		filepath.Join(dir, "b.db"),
		filepath.Join(sub, "c.db"),
		filepath.Join(sub, "d.db"),
	}
	for i, p := range paths {
		absExpected, err := filepath.Abs(expected[i])
		if err != nil {
			t.Fatalf("failed to get absolute path: %v", err)
		}
		if p != absExpected {
			t.Errorf("expected %s, got %s", absExpected, p)
		}
	}
}

func TestDiscoverDatabases_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	paths, err := DiscoverDatabases(dir)
	if err != nil {
		t.Fatalf("DiscoverDatabases on empty dir failed: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(paths))
	}
}

func TestDiscoverDatabases_NonexistentDirectory(t *testing.T) {
	_, err := DiscoverDatabases("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Error("expected error for nonexistent directory, got nil")
	}
}

// createFile creates an empty file at the given path.
func createFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test file %s: %v", path, err)
	}
	f.Close()
}

// createOpenCodeDB creates a minimal OpenCode-like SQLite database in a temp
// directory and returns its path. The caller should clean up via t.Cleanup.
func createOpenCodeDB(t *testing.T, messageRows, sessionRows int) string {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	// Create message table (OpenCode schema).
	_, err = db.Exec(`CREATE TABLE message (
		id TEXT,
		session_id TEXT,
		data TEXT,
		time_created INTEGER,
		time_updated INTEGER
	)`)
	if err != nil {
		t.Fatalf("failed to create message table: %v", err)
	}

	// Create session table (OpenCode schema).
	_, err = db.Exec(`CREATE TABLE session (
		id TEXT,
		time_created INTEGER,
		time_updated INTEGER,
		project_id TEXT,
		parent_id TEXT,
		workspace_id TEXT,
		agent TEXT,
		model TEXT
	)`)
	if err != nil {
		t.Fatalf("failed to create session table: %v", err)
	}

	// Insert message rows.
	for i := 0; i < messageRows; i++ {
		_, err = db.Exec(`INSERT INTO message (id, session_id, data, time_created, time_updated)
			VALUES (?, ?, ?, ?, ?)`,
			"msg-"+string(rune('a'+i%26)), // simple unique id
			"sess-1",
			"{}",
			time.Now().Unix(),
			time.Now().Unix(),
		)
		if err != nil {
			t.Fatalf("failed to insert message: %v", err)
		}
	}

	// Insert session rows.
	for i := 0; i < sessionRows; i++ {
		_, err = db.Exec(`INSERT INTO session (id, time_created, time_updated, project_id, parent_id, workspace_id, agent, model)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			"sess-"+string(rune('a'+i%26)),
			time.Now().Unix(),
			time.Now().Unix(),
			"project-1",
			"parent-1",
			"ws-1",
			"agent-1",
			"model-1",
		)
		if err != nil {
			t.Fatalf("failed to insert session: %v", err)
		}
	}

	return dbPath
}

func TestOpenAndInspect_ValidOpenCodeDB(t *testing.T) {
	dbPath := createOpenCodeDB(t, 5, 3)

	info, err := OpenAndInspect(dbPath)
	if err != nil {
		t.Fatalf("OpenAndInspect failed: %v", err)
	}

	if info.Path != dbPath {
		t.Errorf("expected path %s, got %s", dbPath, info.Path)
	}
	if info.MessageCount != 5 {
		t.Errorf("expected 5 message rows, got %d", info.MessageCount)
	}
	if info.SessionCount != 3 {
		t.Errorf("expected 3 session rows, got %d", info.SessionCount)
	}
	if info.Size <= 0 {
		t.Errorf("expected positive file size, got %d", info.Size)
	}
	if info.LastModified.IsZero() {
		t.Error("expected non-zero LastModified")
	}
	if info.SchemaVersion == "" {
		t.Log("SchemaVersion is empty (expected for new database)")
	}
}

func TestOpenAndInspect_NonExistentFile(t *testing.T) {
	_, err := OpenAndInspect(filepath.Join(t.TempDir(), "nonexistent.db"))
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}

func TestOpenAndInspect_NotASQLiteDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not-a-db.db")
	createFile(t, path)

	_, err := OpenAndInspect(path)
	if err == nil {
		t.Error("expected error for non-SQLite file, got nil")
	}
}

func TestOpenAndInspect_MissingTables(t *testing.T) {
	// Create a valid SQLite DB but without the expected OpenCode tables.
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.db")

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	_, err = db.Exec("CREATE TABLE unrelated (x INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	_, err = OpenAndInspect(path)
	if err == nil {
		t.Error("expected error for missing OpenCode tables, got nil")
	}
}


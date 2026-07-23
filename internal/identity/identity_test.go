package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetOrCreateIdentity_GeneratesUUIDOnFirstCall(t *testing.T) {
	baseDir := t.TempDir()
	store := NewStore(baseDir)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	id, err := store.GetOrCreateIdentity(dbPath)
	if err != nil {
		t.Fatalf("GetOrCreateIdentity failed: %v", err)
	}

	if id == UUIDZero() {
		t.Error("expected non-zero UUID")
	}
}

func TestGetOrCreateIdentity_SamePathReturnsSameUUID(t *testing.T) {
	baseDir := t.TempDir()
	store := NewStore(baseDir)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	id1, err := store.GetOrCreateIdentity(dbPath)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	id2, err := store.GetOrCreateIdentity(dbPath)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected same UUID for same path, got %s and %s", id1, id2)
	}
}

func TestGetOrCreateIdentity_DifferentPathsDifferentUUIDs(t *testing.T) {
	baseDir := t.TempDir()
	store := NewStore(baseDir)

	db1 := filepath.Join(t.TempDir(), "a.db")
	db2 := filepath.Join(t.TempDir(), "b.db")
	createFile(t, db1)
	createFile(t, db2)

	id1, err := store.GetOrCreateIdentity(db1)
	if err != nil {
		t.Fatalf("GetOrCreateIdentity for db1 failed: %v", err)
	}

	id2, err := store.GetOrCreateIdentity(db2)
	if err != nil {
		t.Fatalf("GetOrCreateIdentity for db2 failed: %v", err)
	}

	if id1 == id2 {
		t.Errorf("expected different UUIDs for different paths, got %s for both", id1)
	}
}

func TestGetOrCreateIdentity_PersistsAcrossStoreInstances(t *testing.T) {
	baseDir := t.TempDir()
	store1 := NewStore(baseDir)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	id1, err := store1.GetOrCreateIdentity(dbPath)
	if err != nil {
		t.Fatalf("first store call failed: %v", err)
	}

	// Create a new store with the same base directory.
	store2 := NewStore(baseDir)
	id2, err := store2.GetOrCreateIdentity(dbPath)
	if err != nil {
		t.Fatalf("second store call failed: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected same UUID across store instances, got %s and %s", id1, id2)
	}
}

func TestGetOrCreateIdentity_PathNormalization(t *testing.T) {
	baseDir := t.TempDir()
	store := NewStore(baseDir)

	// Use a single temp dir for both paths so the file lives in one place.
	workDir := t.TempDir()
	dbPath := filepath.Join(workDir, "test.db")
	createFile(t, dbPath)

	// Use an unclean path pointing to the same file.
	unclean := filepath.Join(workDir, "sub", "..", "test.db")

	id1, err := store.GetOrCreateIdentity(dbPath)
	if err != nil {
		t.Fatalf("clean path call failed: %v", err)
	}

	id2, err := store.GetOrCreateIdentity(unclean)
	if err != nil {
		t.Fatalf("unclean path call failed: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected same UUID for equivalent paths, got %s and %s", id1, id2)
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

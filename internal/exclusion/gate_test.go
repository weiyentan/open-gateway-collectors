package exclusion

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencode-gateway/collectors/internal/pathutil"
)

func TestGate_Exclude_WritesFile(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	reason := "required table 'message' not found — not an OpenCode database"
	if err := gate.Exclude(dbPath, reason); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	// Verify the exclusion file exists.
	pathHash, err := pathutil.HashPath(dbPath)
	if err != nil {
		t.Fatalf("hashPath failed: %v", err)
	}
	entryPath := filepath.Join(gate.gateDir, pathHash+".exclude")
	if _, err := os.Stat(entryPath); os.IsNotExist(err) {
		t.Errorf("exclusion file was not created at %s", entryPath)
	}
}

func TestGate_IsExcluded_ReturnsTrueForExcluded(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test reason"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	excluded, err := gate.IsExcluded(dbPath)
	if err != nil {
		t.Fatalf("IsExcluded failed: %v", err)
	}
	if !excluded {
		t.Error("expected IsExcluded to return true for an excluded path")
	}
}

func TestGate_IsExcluded_ReturnsFalseForUnknown(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	excluded, err := gate.IsExcluded("/nonexistent/path.db")
	if err != nil {
		t.Fatalf("IsExcluded failed: %v", err)
	}
	if excluded {
		t.Error("expected IsExcluded to return false for an unknown path")
	}
}

func TestGate_IsExcluded_ReturnsFalseAfterRemove(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test reason"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	if err := gate.Remove(dbPath); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	excluded, err := gate.IsExcluded(dbPath)
	if err != nil {
		t.Fatalf("IsExcluded after Remove failed: %v", err)
	}
	if excluded {
		t.Error("expected IsExcluded to return false after Remove")
	}
}

func TestGate_RecheckDue_ReturnsTrueWhenPastDue(t *testing.T) {
	dir := t.TempDir()
	// Use a 1-nanosecond interval so NextRecheck is immediately in the past.
	gate := NewGate(dir, time.Nanosecond)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test reason"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	due, err := gate.RecheckDue(dbPath)
	if err != nil {
		t.Fatalf("RecheckDue failed: %v", err)
	}
	if !due {
		t.Error("expected RecheckDue to return true when past next_recheck")
	}
}

func TestGate_RecheckDue_ReturnsFalseWhenNotDue(t *testing.T) {
	dir := t.TempDir()
	// Default 3h interval means NextRecheck is well in the future.
	gate := NewGate(dir, DefaultRecheckInterval)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test reason"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	due, err := gate.RecheckDue(dbPath)
	if err != nil {
		t.Fatalf("RecheckDue failed: %v", err)
	}
	if due {
		t.Error("expected RecheckDue to return false when next_recheck is in the future")
	}
}

func TestGate_PersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	gate1 := NewGate(dir, DefaultRecheckInterval)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate1.Exclude(dbPath, "persist test"); err != nil {
		t.Fatalf("gate1 Exclude failed: %v", err)
	}

	// Create a new gate with the same base directory.
	gate2 := NewGate(dir, DefaultRecheckInterval)

	excluded, err := gate2.IsExcluded(dbPath)
	if err != nil {
		t.Fatalf("gate2 IsExcluded failed: %v", err)
	}
	if !excluded {
		t.Error("expected exclusion to persist across Gate instances")
	}
}

func TestGate_PathNormalization(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	// Use real filesystem paths to test normalization.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	createFile(t, dbPath)

	unclean := filepath.Join(tmpDir, "sub", "..", "test.db")

	if err := gate.Exclude(dbPath, "normalization test"); err != nil {
		t.Fatalf("Exclude with clean path failed: %v", err)
	}

	excluded, err := gate.IsExcluded(unclean)
	if err != nil {
		t.Fatalf("IsExcluded with unclean path failed: %v", err)
	}
	if !excluded {
		t.Error("expected unclean path to resolve to the same exclusion as clean path")
	}
}

func TestGate_CorruptEntry_DoesNotAffectOthers(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	validPath := filepath.Join(t.TempDir(), "valid.db")
	createFile(t, validPath)

	corruptPath := filepath.Join(t.TempDir(), "corrupt.db")
	createFile(t, corruptPath)

	// Exclude the valid path.
	if err := gate.Exclude(validPath, "valid db"); err != nil {
		t.Fatalf("Exclude valid path failed: %v", err)
	}

	// Write a corrupt exclusion file for the corrupt path.
	corruptHash, err := pathutil.HashPath(corruptPath)
	if err != nil {
		t.Fatalf("hashPath for corrupt path failed: %v", err)
	}
	corruptEntryPath := filepath.Join(gate.gateDir, corruptHash+".exclude")
	if err := os.WriteFile(corruptEntryPath, []byte("not valid json"), 0o644); err != nil {
		t.Fatalf("writing corrupt exclusion file failed: %v", err)
	}

	// The valid path should still be excluded.
	excluded, err := gate.IsExcluded(validPath)
	if err != nil {
		t.Fatalf("IsExcluded valid path failed: %v", err)
	}
	if !excluded {
		t.Error("expected valid path to still be excluded despite corrupt entry for other path")
	}

	// The corrupt path should not be considered excluded.
	excluded, err = gate.IsExcluded(corruptPath)
	if err != nil {
		t.Fatalf("IsExcluded corrupt path failed: %v", err)
	}
	if excluded {
		t.Error("expected corrupt entry to result in IsExcluded returning false")
	}
}

func TestGate_Remove_NonExistentReturnsNoError(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	if err := gate.Remove("/nonexistent/path.db"); err != nil {
		t.Errorf("Remove on non-existent path should not error, got: %v", err)
	}
}

func TestGate_RecheckDue_ReturnsFalseForNonExcluded(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	due, err := gate.RecheckDue("/nonexistent/path.db")
	if err != nil {
		t.Fatalf("RecheckDue for non-excluded path failed: %v", err)
	}
	if due {
		t.Error("expected RecheckDue to return false for a non-excluded path")
	}
}

func TestGate_RecheckDue_CorruptEntryReturnsTrue(t *testing.T) {
	dir := t.TempDir()
	gate := NewGate(dir, DefaultRecheckInterval)

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	// Write a corrupt exclusion file.
	pathHash, err := pathutil.HashPath(dbPath)
	if err != nil {
		t.Fatalf("hashPath failed: %v", err)
	}
	if err := os.MkdirAll(gate.gateDir, 0o755); err != nil {
		t.Fatalf("mkdir gate dir failed: %v", err)
	}
	entryPath := filepath.Join(gate.gateDir, pathHash+".exclude")
	if err := os.WriteFile(entryPath, []byte("not valid json"), 0o644); err != nil {
		t.Fatalf("writing corrupt exclusion file failed: %v", err)
	}

	due, err := gate.RecheckDue(dbPath)
	if err != nil {
		t.Fatalf("RecheckDue for corrupt entry failed: %v", err)
	}
	if !due {
		t.Error("expected RecheckDue to return true for a corrupt entry (treat as due)")
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

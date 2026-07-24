package exclusion

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGate_Exclude_ReturnsNoError(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	err = gate.Exclude(dbPath, "required table 'message' not found")
	if err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	// Verify the .exclude file was actually written.
	exPath, err := gate.excludeFilePath(dbPath)
	if err != nil {
		t.Fatalf("excludeFilePath failed: %v", err)
	}
	if _, err := os.Stat(exPath); os.IsNotExist(err) {
		t.Error(".exclude file was not created")
	}
}

func TestGate_IsExcluded_ReturnsTrueForExcludedPath(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test exclusion"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	if !gate.IsExcluded(dbPath) {
		t.Error("IsExcluded returned false for excluded path")
	}
}

func TestGate_IsExcluded_ReturnsFalseForUnknownPath(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	if gate.IsExcluded("/nonexistent/path.db") {
		t.Error("IsExcluded returned true for unknown path")
	}
}

func TestGate_IsExcluded_ReturnsFalseAfterRemove(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test exclusion"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	if err := gate.Remove(dbPath); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if gate.IsExcluded(dbPath) {
		t.Error("IsExcluded returned true after Remove")
	}
}

func TestGate_Remove_ReturnsNoErrorForAlreadyRemoved(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	// Removing a never-excluded path should not error.
	if err := gate.Remove("/nonexistent/path.db"); err != nil {
		t.Errorf("Remove on non-excluded path returned error: %v", err)
	}
}

func TestGate_RecheckDue_ReturnsTrueWhenPastNextRecheck(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test exclusion"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	// Manually overwrite the exclude file with a past next_recheck.
	exPath, err := gate.excludeFilePath(dbPath)
	if err != nil {
		t.Fatalf("excludeFilePath failed: %v", err)
	}
	rec := excludeRecord{
		Path:        dbPath,
		Reason:      "test exclusion",
		ExcludedAt:  time.Now().Add(-48 * time.Hour),
		NextRecheck: time.Now().Add(-1 * time.Hour),
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if err := os.WriteFile(exPath, data, 0o644); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if !gate.RecheckDue(dbPath) {
		t.Error("RecheckDue returned false when next_recheck is in the past")
	}
}

func TestGate_RecheckDue_ReturnsFalseWhenNotYetDue(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	createFile(t, dbPath)

	if err := gate.Exclude(dbPath, "test exclusion"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	if gate.RecheckDue(dbPath) {
		t.Error("RecheckDue returned true when next_recheck is 24h in the future")
	}
}

func TestGate_RecheckDue_ReturnsTrueForUnknownPath(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	if !gate.RecheckDue("/nonexistent/path.db") {
		t.Error("RecheckDue returned false for unknown path")
	}
}

func TestGate_ExclusionsPersistAcrossInstances(t *testing.T) {
	baseDir := t.TempDir()
	gate1, err := NewGate(baseDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewGate (1) failed: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "persist.db")
	createFile(t, dbPath)

	if err := gate1.Exclude(dbPath, "persistent exclusion"); err != nil {
		t.Fatalf("Exclude failed: %v", err)
	}

	// Create a new gate with the same base directory.
	gate2, err := NewGate(baseDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewGate (2) failed: %v", err)
	}

	if !gate2.IsExcluded(dbPath) {
		t.Error("exclusion did not persist across Gate instances")
	}
}

func TestGate_PathNormalization(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	workDir := t.TempDir()
	dbPath := filepath.Join(workDir, "test.db")
	createFile(t, dbPath)

	// Unclean path with ".." component that resolves to the same file.
	unclean := filepath.Join(workDir, "sub", "..", "test.db")

	if err := gate.Exclude(dbPath, "test normalization"); err != nil {
		t.Fatalf("Exclude with clean path failed: %v", err)
	}

	if !gate.IsExcluded(unclean) {
		t.Error("unclean path did not resolve to same exclusion as clean path")
	}

	// Also verify Remove works with unclean path.
	if err := gate.Remove(unclean); err != nil {
		t.Fatalf("Remove with unclean path failed: %v", err)
	}

	if gate.IsExcluded(dbPath) {
		t.Error("Remove with unclean path did not remove clean path exclusion")
	}
}

func TestGate_CorruptExclusionFileDoesNotAffectOtherPaths(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	goodPath := filepath.Join(t.TempDir(), "good.db")
	badPath := filepath.Join(t.TempDir(), "bad.db")
	createFile(t, goodPath)
	createFile(t, badPath)

	if err := gate.Exclude(goodPath, "good database"); err != nil {
		t.Fatalf("Exclude goodPath failed: %v", err)
	}

	// Manually write a corrupt .exclude file for the bad path.
	badHash, err := hashPath(badPath)
	if err != nil {
		t.Fatalf("hashPath failed: %v", err)
	}
	corruptPath := filepath.Join(gate.gateDir, badHash+".exclude")
	if err := os.WriteFile(corruptPath, []byte("{invalid json"), 0o644); err != nil {
		t.Fatalf("writing corrupt file failed: %v", err)
	}

	// The bad path should NOT be considered excluded (corrupt file).
	if gate.IsExcluded(badPath) {
		t.Error("IsExcluded returned true for path with corrupt exclusion file")
	}

	// The good path should still be excluded.
	if !gate.IsExcluded(goodPath) {
		t.Error("corrupt file for one path affected another path's exclusion status")
	}
}

func TestGate_DefaultRecheckInterval(t *testing.T) {
	baseDir := t.TempDir()
	gate, err := NewGate(baseDir, 0)
	if err != nil {
		t.Fatalf("NewGate failed: %v", err)
	}

	if gate.recheckInterval != DefaultRecheckInterval {
		t.Errorf("expected default interval %v, got %v", DefaultRecheckInterval, gate.recheckInterval)
	}
}

// createFile creates an empty file at the given path. The file must not exist
// in a directory that has already been removed (e.g. after t.TempDir cleanup).
func createFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test file %s: %v", path, err)
	}
	f.Close()
}

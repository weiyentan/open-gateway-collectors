package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTracker_GetCursor_UnknownReturnsZero(t *testing.T) {
	dir := t.TempDir()
	tracker, err := NewTracker(dir)
	if err != nil {
		t.Fatalf("NewTracker failed: %v", err)
	}

	cursor, err := tracker.GetCursor("/nonexistent/path.db")
	if err != nil {
		t.Fatalf("GetCursor failed: %v", err)
	}

	if !cursor.IsZero() {
		t.Errorf("expected zero time for unknown db, got %v", cursor)
	}
}

func TestTracker_SetCursor_And_GetCursor(t *testing.T) {
	dir := t.TempDir()
	tracker, err := NewTracker(dir)
	if err != nil {
		t.Fatalf("NewTracker failed: %v", err)
	}

	dbPath := "/path/to/test.db"
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)

	if err := tracker.SetCursor(dbPath, now); err != nil {
		t.Fatalf("SetCursor failed: %v", err)
	}

	got, err := tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor after SetCursor failed: %v", err)
	}

	if !got.Equal(now) {
		t.Errorf("expected %v, got %v", now, got)
	}
}

func TestTracker_SetCursor_UpdatesExisting(t *testing.T) {
	dir := t.TempDir()
	tracker, err := NewTracker(dir)
	if err != nil {
		t.Fatalf("NewTracker failed: %v", err)
	}

	dbPath := "/path/to/test.db"
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)

	if err := tracker.SetCursor(dbPath, t1); err != nil {
		t.Fatalf("first SetCursor failed: %v", err)
	}
	if err := tracker.SetCursor(dbPath, t2); err != nil {
		t.Fatalf("second SetCursor failed: %v", err)
	}

	got, err := tracker.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor failed: %v", err)
	}

	if !got.Equal(t2) {
		t.Errorf("expected updated time %v, got %v", t2, got)
	}
}

func TestTracker_DifferentPathsIndependentCursors(t *testing.T) {
	dir := t.TempDir()
	tracker, err := NewTracker(dir)
	if err != nil {
		t.Fatalf("NewTracker failed: %v", err)
	}

	db1 := "/path/to/db1.db"
	db2 := "/path/to/db2.db"
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)

	if err := tracker.SetCursor(db1, t1); err != nil {
		t.Fatalf("SetCursor db1 failed: %v", err)
	}
	if err := tracker.SetCursor(db2, t2); err != nil {
		t.Fatalf("SetCursor db2 failed: %v", err)
	}

	got1, err := tracker.GetCursor(db1)
	if err != nil {
		t.Fatalf("GetCursor db1 failed: %v", err)
	}
	got2, err := tracker.GetCursor(db2)
	if err != nil {
		t.Fatalf("GetCursor db2 failed: %v", err)
	}

	if !got1.Equal(t1) {
		t.Errorf("db1: expected %v, got %v", t1, got1)
	}
	if !got2.Equal(t2) {
		t.Errorf("db2: expected %v, got %v", t2, got2)
	}
}

func TestTracker_PersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	tracker1, err := NewTracker(dir)
	if err != nil {
		t.Fatalf("NewTracker (1) failed: %v", err)
	}

	dbPath := "/path/to/persist.db"
	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)

	if err := tracker1.SetCursor(dbPath, now); err != nil {
		t.Fatalf("SetCursor failed: %v", err)
	}

	// Create a new tracker with the same directory.
	tracker2, err := NewTracker(dir)
	if err != nil {
		t.Fatalf("NewTracker (2) failed: %v", err)
	}

	got, err := tracker2.GetCursor(dbPath)
	if err != nil {
		t.Fatalf("GetCursor from second tracker failed: %v", err)
	}

	if !got.Equal(now) {
		t.Errorf("expected %v across instances, got %v", now, got)
	}
}

func TestTracker_PathNormalization(t *testing.T) {
	dir := t.TempDir()
	tracker, err := NewTracker(dir)
	if err != nil {
		t.Fatalf("NewTracker failed: %v", err)
	}

	// Use real filesystem paths to test normalization.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	createFile(t, dbPath)

	unclean := filepath.Join(tmpDir, "sub", "..", "test.db")

	now := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	if err := tracker.SetCursor(dbPath, now); err != nil {
		t.Fatalf("SetCursor clean path failed: %v", err)
	}

	got, err := tracker.GetCursor(unclean)
	if err != nil {
		t.Fatalf("GetCursor unclean path failed: %v", err)
	}

	if !got.Equal(now) {
		t.Errorf("expected same cursor for equivalent paths, got %v", got)
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

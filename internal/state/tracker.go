// Package state provides cursor-based state tracking for source databases.
// The Tracker persists the last-processed timestamp for each known database,
// enabling incremental reads across collector restarts.
package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/opencode-gateway/collectors/internal/pathutil"
)

// Tracker manages cursor state across all known source databases. State is
// persisted to a JSON file keyed by database path hash. All access is
// single-process — no locking/concurrency required, but a mutex is used for
// safety.
type Tracker struct {
	mu       sync.Mutex
	state    map[string]time.Time // keyed by path hash
	filePath string
}

// NewTracker creates a new Tracker, loading existing state from the
// .collector-state file in the given directory if it exists.
func NewTracker(dir string) (*Tracker, error) {
	filePath := filepath.Join(dir, ".collector-state")
	t := &Tracker{
		state:    make(map[string]time.Time),
		filePath: filePath,
	}

	if err := t.load(); err != nil {
		// If the file doesn't exist, that's fine — start fresh.
		if os.IsNotExist(err) {
			return t, nil
		}
		return nil, fmt.Errorf("loading state file: %w", err)
	}

	return t, nil
}

// GetCursor returns the last-sent timestamp for the given database path.
// Returns a zero time if the database has no recorded cursor.
func (t *Tracker) GetCursor(dbPath string) (time.Time, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	pathHash, err := pathutil.HashPath(dbPath)
	if err != nil {
		return time.Time{}, err
	}
	cursor, ok := t.state[pathHash]
	if !ok {
		return time.Time{}, nil
	}
	return cursor, nil
}

// SetCursor persists the given timestamp as the cursor for the database path.
// The state file is written immediately.
func (t *Tracker) SetCursor(dbPath string, cursor time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	pathHash, err := pathutil.HashPath(dbPath)
	if err != nil {
		return err
	}
	t.state[pathHash] = cursor

	return t.save()
}

// load reads the state file from disk into memory.
func (t *Tracker) load() error {
	data, err := os.ReadFile(t.filePath)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	// The state file is a JSON object mapping path-hash to RFC 3339 Nano timestamp.
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("parsing state file: %w", err)
	}

	for hash, ts := range raw {
		tm, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			// Skip malformed entries rather than failing entirely.
			continue
		}
		t.state[hash] = tm
	}

	return nil
}

// save writes the in-memory state to the state file.
func (t *Tracker) save() error {
	raw := make(map[string]string, len(t.state))
	for hash, ts := range t.state {
		raw[hash] = ts.Format(time.RFC3339Nano)
	}

	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling state: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(t.filePath), 0o755); err != nil {
		return fmt.Errorf("creating state directory: %w", err)
	}

	if err := os.WriteFile(t.filePath, data, 0o644); err != nil {
		return fmt.Errorf("writing state file: %w", err)
	}

	return nil
}

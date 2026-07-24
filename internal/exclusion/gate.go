// Package exclusion provides a persistent gate for databases that fail schema
// inspection. Excluded databases are recorded as JSON files keyed by path hash
// and are automatically rechecked after a configurable interval.
package exclusion

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/opencode-gateway/collectors/internal/pathutil"
)

// DefaultRecheckInterval is the default duration before an excluded database is
// rechecked.
const DefaultRecheckInterval = 3 * time.Hour

// entry represents a single exclusion record persisted to disk.
type entry struct {
	Path        string    `json:"path"`
	Reason      string    `json:"reason"`
	ExcludedAt  time.Time `json:"excluded_at"`
	NextRecheck time.Time `json:"next_recheck"`
}

// Gate manages persistent exclusions for databases that fail schema inspection.
// Each exclusion is stored as an individual JSON file in the .collector-gate/
// subdirectory under the cursor directory, keyed by sha256 of the cleaned
// absolute path.
type Gate struct {
	mu              sync.Mutex
	cursorDir       string
	gateDir         string
	recheckInterval time.Duration
}

// NewGate creates a new Gate. Exclusion files are stored under
// cursorDir/.collector-gate/. If recheckInterval is zero or negative,
// DefaultRecheckInterval (3 hours) is used. The gate directory is created
// if it does not exist.
func NewGate(cursorDir string, recheckInterval time.Duration) *Gate {
	if recheckInterval <= 0 {
		recheckInterval = DefaultRecheckInterval
	}
	gateDir := filepath.Join(cursorDir, ".collector-gate")
	// Best-effort creation of the gate directory.
	_ = os.MkdirAll(gateDir, 0o755)
	return &Gate{
		cursorDir:       cursorDir,
		gateDir:         gateDir,
		recheckInterval: recheckInterval,
	}
}

// IsExcluded checks whether the given database path has an active exclusion.
// Returns true if an exclusion file exists and can be read successfully.
func (g *Gate) IsExcluded(path string) (bool, error) {
	pathHash, err := pathutil.HashPath(path)
	if err != nil {
		return false, err
	}

	entryPath := filepath.Join(g.gateDir, pathHash+".exclude")
	if _, err := os.Stat(entryPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking exclusion: %w", err)
	}

	// Verify the file is readable and valid.
	var e entry
	data, err := os.ReadFile(entryPath)
	if err != nil {
		return false, fmt.Errorf("reading exclusion file: %w", err)
	}
	if err := json.Unmarshal(data, &e); err != nil {
		// Corrupt file — treat as not excluded but don't delete.
		return false, nil
	}

	return true, nil
}

// Exclude writes an exclusion file for the given database path. The reason is
// recorded for observability. The next_recheck is set to current time plus the
// configured recheck interval.
func (g *Gate) Exclude(path string, reason string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	absPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("resolving absolute path: %w", err)
	}

	pathHash, err := pathutil.HashPath(absPath)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(g.gateDir, 0o755); err != nil {
		return fmt.Errorf("creating gate directory: %w", err)
	}

	now := time.Now()
	e := entry{
		Path:        absPath,
		Reason:      reason,
		ExcludedAt:  now,
		NextRecheck: now.Add(g.recheckInterval),
	}

	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling exclusion entry: %w", err)
	}

	entryPath := filepath.Join(g.gateDir, pathHash+".exclude")
	if err := os.WriteFile(entryPath, data, 0o644); err != nil {
		return fmt.Errorf("writing exclusion file: %w", err)
	}

	return nil
}

// RecheckDue returns true if the current time is at or past the next_recheck
// timestamp for the given path. Returns false if the path is not excluded.
func (g *Gate) RecheckDue(path string) (bool, error) {
	pathHash, err := pathutil.HashPath(path)
	if err != nil {
		return false, err
	}

	entryPath := filepath.Join(g.gateDir, pathHash+".exclude")
	data, err := os.ReadFile(entryPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("reading exclusion file: %w", err)
	}

	var e entry
	if err := json.Unmarshal(data, &e); err != nil {
		// Corrupt file — treat as due for recheck.
		return true, nil
	}

	return !time.Now().Before(e.NextRecheck), nil
}

// Remove deletes the exclusion file for the given database path. No error is
// returned if the exclusion does not exist.
func (g *Gate) Remove(path string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	pathHash, err := pathutil.HashPath(path)
	if err != nil {
		return err
	}
	entryPath := filepath.Join(g.gateDir, pathHash+".exclude")
	if err := os.Remove(entryPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("removing exclusion file: %w", err)
	}

	return nil
}

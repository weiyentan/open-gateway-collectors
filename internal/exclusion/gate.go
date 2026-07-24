// Package exclusion provides persistent tracking for databases that fail schema
// inspection. Excluded databases are recorded on disk and skipped on subsequent
// poll cycles, with automatic recheck after a configurable interval.
package exclusion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultRecheckInterval is the default duration before an excluded database is
// reconsidered.
const DefaultRecheckInterval = 3 * time.Hour

// excludeRecord is the JSON structure persisted for each excluded database.
type excludeRecord struct {
	Path        string    `json:"path"`
	Reason      string    `json:"reason"`
	ExcludedAt  time.Time `json:"excluded_at"`
	NextRecheck time.Time `json:"next_recheck"`
}

// Gate persistently tracks databases that fail schema inspection, allowing the
// collector to skip them without re-opening on subsequent poll cycles. Excluded
// databases are rechecked after a configurable interval.
type Gate struct {
	mu              sync.Mutex
	baseDir         string
	recheckInterval time.Duration
	gateDir         string
}

// NewGate creates a new Gate. Exclusion records are persisted under
// baseDir/.collector-gate/. If recheckInterval is zero, DefaultRecheckInterval
// (3 hours) is used.
func NewGate(baseDir string, recheckInterval time.Duration) (*Gate, error) {
	if recheckInterval == 0 {
		recheckInterval = DefaultRecheckInterval
	}

	gateDir := filepath.Join(baseDir, ".collector-gate")
	if err := os.MkdirAll(gateDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating gate directory: %w", err)
	}

	return &Gate{
		baseDir:         baseDir,
		recheckInterval: recheckInterval,
		gateDir:         gateDir,
	}, nil
}

// IsExcluded returns true if the given path has been excluded and its .exclude
// file exists and is parseable.
func (g *Gate) IsExcluded(path string) bool {
	exPath, err := g.excludeFilePath(path)
	if err != nil {
		return false
	}

	data, err := os.ReadFile(exPath)
	if err != nil {
		return false
	}

	var rec excludeRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return false
	}

	return true
}

// Exclude writes a .exclude file for the given database path, recording the
// reason and setting next_recheck to now + recheckInterval.
func (g *Gate) Exclude(path string, reason string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	absPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("resolving absolute path: %w", err)
	}

	now := time.Now()
	rec := excludeRecord{
		Path:        absPath,
		Reason:      reason,
		ExcludedAt:  now,
		NextRecheck: now.Add(g.recheckInterval),
	}

	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling exclusion record: %w", err)
	}

	exPath, err := g.excludeFilePath(absPath)
	if err != nil {
		return err
	}

	if err := os.WriteFile(exPath, data, 0o644); err != nil {
		return fmt.Errorf("writing exclusion file: %w", err)
	}

	return nil
}

// RecheckDue returns true if the current time is at or past the next_recheck
// timestamp for the given path. Returns true if the path is not excluded or
// the exclusion file is corrupt, so the caller always rechecks when in doubt.
func (g *Gate) RecheckDue(path string) bool {
	exPath, err := g.excludeFilePath(path)
	if err != nil {
		return true
	}

	data, err := os.ReadFile(exPath)
	if err != nil {
		return true
	}

	var rec excludeRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return true
	}

	return !time.Now().Before(rec.NextRecheck)
}

// Remove deletes the .exclude file for the given path, allowing the database
// to be reconsidered on the next poll cycle. Removing an already-removed path
// returns nil.
func (g *Gate) Remove(path string) error {
	exPath, err := g.excludeFilePath(path)
	if err != nil {
		return err
	}

	if err := os.Remove(exPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("removing exclusion file: %w", err)
	}

	return nil
}

// excludeFilePath returns the path to the .exclude file for the given database
// path. The path is normalized (abs + clean) and hashed with SHA-256.
func (g *Gate) excludeFilePath(path string) (string, error) {
	pathHash, err := hashPath(path)
	if err != nil {
		return "", err
	}
	return filepath.Join(g.gateDir, pathHash+".exclude"), nil
}

// hashPath returns the hex-encoded SHA-256 hash of the cleaned, absolute path,
// identical to the hashPath function in internal/state.
func hashPath(path string) (string, error) {
	absPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("resolving absolute path: %w", err)
	}
	hash := sha256.Sum256([]byte(absPath))
	return hex.EncodeToString(hash[:]), nil
}

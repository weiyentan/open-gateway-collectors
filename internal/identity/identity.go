// Package identity provides stable UUID identity management for source databases.
// Each source database is assigned a UUID v4 that is persisted to disk and reused
// across collector restarts.
package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// Store manages per-database identity. It persists UUIDs to a directory using
// path-hash-named files.
type Store struct {
	baseDir string
}

// NewStore creates a new identity Store. Identity files are persisted under
// baseDir/.collector-id/.
func NewStore(baseDir string) *Store {
	return &Store{baseDir: baseDir}
}

// GetOrCreateIdentity returns the stable UUID for the given database path.
// On first call, a new UUID v4 is generated and persisted. Subsequent calls
// return the persisted UUID. The path is cleaned and made absolute before
// hashing, so equivalent paths yield the same identity.
func (s *Store) GetOrCreateIdentity(dbPath string) (uuid.UUID, error) {
	absPath, err := filepath.Abs(filepath.Clean(dbPath))
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolving absolute path: %w", err)
	}

	// Ensure the identity directory exists.
	identityDir := filepath.Join(s.baseDir, ".collector-id")
	if err := os.MkdirAll(identityDir, 0o755); err != nil {
		return uuid.Nil, fmt.Errorf("creating identity directory: %w", err)
	}

	// Compute the path hash for the identity filename.
	pathHash := sha256Hex(absPath)
	identityFile := filepath.Join(identityDir, pathHash+".id")

	// Try to read existing identity.
	data, err := os.ReadFile(identityFile)
	if err == nil {
		id, parseErr := uuid.Parse(strings.TrimSpace(string(data)))
		if parseErr == nil {
			return id, nil
		}
		// If parse fails, fall through to generate a new one.
	}

	if !os.IsNotExist(err) {
		return uuid.Nil, fmt.Errorf("reading identity file: %w", err)
	}

	// Generate and persist a new UUID.
	newID, err := uuid.NewRandom()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generating UUID: %w", err)
	}

	if err := os.WriteFile(identityFile, []byte(newID.String()+"\n"), 0o644); err != nil {
		return uuid.Nil, fmt.Errorf("writing identity file: %w", err)
	}

	return newID, nil
}

// sha256Hex returns the hex-encoded SHA-256 hash of the input string.
func sha256Hex(input string) string {
	hash := sha256.Sum256([]byte(input))
	return hex.EncodeToString(hash[:])
}

// UUIDZero returns the zero-value UUID for comparison in tests.
func UUIDZero() uuid.UUID {
	return uuid.Nil
}

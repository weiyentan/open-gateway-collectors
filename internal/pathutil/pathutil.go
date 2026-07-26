// Package pathutil provides shared path normalization utilities.
package pathutil

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
)

// HashPath returns the hex-encoded SHA-256 hash of the cleaned, absolute path.
// The path is normalized with filepath.Abs after filepath.Clean.
func HashPath(path string) (string, error) {
	absPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("resolving absolute path: %w", err)
	}
	hash := sha256.Sum256([]byte(absPath))
	return hex.EncodeToString(hash[:]), nil
}

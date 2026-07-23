package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	_ "modernc.org/sqlite"
)

// DiscoverDatabases scans the given directory recursively for *.db files and
// returns their absolute paths. Non-.db files are ignored.
func DiscoverDatabases(dir string) ([]string, error) {
	var paths []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(info.Name()) == ".db" {
			absPath, absErr := filepath.Abs(path)
			if absErr != nil {
				return fmt.Errorf("failed to resolve absolute path for %s: %w", path, absErr)
			}
			paths = append(paths, absPath)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking directory %s: %w", dir, err)
	}

	return paths, nil
}

// OpenAndInspect opens a SQLite database at the given path in read-only mode
// and verifies it is a valid OpenCode source database by checking for the
// required message and session tables. It returns metadata about the database.
func OpenAndInspect(path string) (*DatabaseInfo, error) {
	// Resolve to absolute path.
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolving absolute path: %w", err)
	}

	// Get file stats before opening.
	fileInfo, err := os.Stat(absPath)
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	// Open SQLite in read-only mode via query parameter.
	dsn := absPath + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Verify this is actually a SQLite database by running a simple query.
	var dummy int
	if err := db.QueryRow("SELECT 1").Scan(&dummy); err != nil {
		return nil, fmt.Errorf("not a valid SQLite database: %w", err)
	}

	// Check for required OpenCode tables.
	if err := tableExists(db, "message"); err != nil {
		return nil, err
	}
	if err := tableExists(db, "session"); err != nil {
		return nil, err
	}

	// Get row counts.
	msgCount, err := countRows(db, "message")
	if err != nil {
		return nil, fmt.Errorf("counting message rows: %w", err)
	}
	sessCount, err := countRows(db, "session")
	if err != nil {
		return nil, fmt.Errorf("counting session rows: %w", err)
	}

	// Get schema version from user_version pragma.
	schemaVer, err := getSchemaVersion(db)
	if err != nil {
		return nil, fmt.Errorf("reading schema version: %w", err)
	}

	return &DatabaseInfo{
		Path:          absPath,
		Size:          fileInfo.Size(),
		LastModified:  fileInfo.ModTime(),
		MessageCount:  msgCount,
		SessionCount:  sessCount,
		SchemaVersion: schemaVer,
	}, nil
}

// tableExists verifies that the named table exists in the database.
func tableExists(db *sql.DB, name string) error {
	var count int
	err := db.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
		name,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("checking for table %s: %w", name, err)
	}
	if count == 0 {
		return fmt.Errorf("required table %q not found — not an OpenCode database", name)
	}
	return nil
}

// countRows returns the number of rows in the named table.
// tableName MUST be one of the allowed table names to prevent SQL injection.
// Only "message" and "session" are currently allowed.
func countRows(db *sql.DB, tableName string) (int, error) {
	allowed := map[string]bool{"message": true, "session": true}
	if !allowed[tableName] {
		return 0, fmt.Errorf("disallowed table name: %q", tableName)
	}
	var count int
	err := db.QueryRow("SELECT count(*) FROM " + tableName).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// getSchemaVersion reads the user_version pragma as the schema version.
func getSchemaVersion(db *sql.DB) (string, error) {
	var version int
	err := db.QueryRow("PRAGMA user_version").Scan(&version)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(version), nil
}


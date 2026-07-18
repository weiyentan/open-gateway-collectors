// Package sqlite provides source database discovery and inspection for
// OpenCode SQLite databases.
package sqlite

import "time"

// DatabaseInfo holds metadata about a discovered OpenCode source database.
type DatabaseInfo struct {
	// Path is the absolute filesystem path to the database file.
	Path string

	// Size is the file size in bytes at the time of inspection.
	Size int64

	// LastModified is the file modification timestamp at the time of inspection.
	LastModified time.Time

	// MessageCount is the number of rows in the message table.
	MessageCount int

	// SessionCount is the number of rows in the session table.
	SessionCount int

	// SchemaVersion is a version identifier derived from the schema. Currently
	// set to the SQLite user_version pragma value.
	SchemaVersion string
}

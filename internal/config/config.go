// Package config provides configuration loading and validation for the
// opencode-collector application. All configuration is sourced from
// environment variables with sensible defaults.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Config holds all configuration for the collector application.
type Config struct {
	// Token is the bearer token used to authenticate to the Gateway.
	Token string `env:"GATEWAY_COLLECTOR_TOKEN"`

	// BaseURL is the base URL of the Gateway API.
	BaseURL string `env:"GATEWAY_BASE_URL"`

	// PollInterval is how often to poll source databases for new usage records.
	PollInterval time.Duration `env:"GATEWAY_COLLECTOR_POLL_INTERVAL"`

	// HeartbeatInterval is how often to send heartbeats when no new records exist.
	HeartbeatInterval time.Duration `env:"GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL"`

	// SQLitePath is the path to a single OpenCode SQLite database file.
	// If set, SQLiteDir is ignored.
	SQLitePath string `env:"GATEWAY_COLLECTOR_SQLITE_PATH"`

	// SQLiteDir is the directory containing OpenCode SQLite database files.
	// Defaults to the platform-specific OpenCode data directory.
	SQLiteDir string `env:"GATEWAY_COLLECTOR_SQLITE_DIR"`

	// LogLevel controls the logging verbosity (debug, info, warn, error).
	LogLevel string `env:"GATEWAY_COLLECTOR_LOG_LEVEL"`

	// CursorDir is the directory where cursor state files are persisted.
	// Defaults to the working directory.
	CursorDir string `env:"GATEWAY_COLLECTOR_CURSOR_DIR"`

	// ExcludeRecheckInterval is how often to recheck an excluded database.
	// Defaults to 3 hours.
	ExcludeRecheckInterval time.Duration `env:"GATEWAY_COLLECTOR_EXCLUDE_RECHECK_INTERVAL"`
}

// Load reads configuration from environment variables with defaults.
// It returns an error if required fields are missing or invalid.
func Load() (*Config, error) {
	cfg := &Config{
		Token:             os.Getenv("GATEWAY_COLLECTOR_TOKEN"),
		BaseURL:           os.Getenv("GATEWAY_BASE_URL"),
		PollInterval:      getDurationEnv("GATEWAY_COLLECTOR_POLL_INTERVAL", 60*time.Second),
		HeartbeatInterval: getDurationEnv("GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL", 120*time.Second),
		SQLitePath:        os.Getenv("GATEWAY_COLLECTOR_SQLITE_PATH"),
		SQLiteDir:         getEnvWithDefault("GATEWAY_COLLECTOR_SQLITE_DIR", defaultSQLiteDir()),
		LogLevel:               getEnvWithDefault("GATEWAY_COLLECTOR_LOG_LEVEL", "info"),
		CursorDir:              getEnvWithDefault("GATEWAY_COLLECTOR_CURSOR_DIR", defaultCursorDir()),
		ExcludeRecheckInterval: getDurationEnv("GATEWAY_COLLECTOR_EXCLUDE_RECHECK_INTERVAL", 3*time.Hour),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks that required fields are set and optional fields are valid.
func (c *Config) Validate() error {
	if c.Token == "" {
		return fmt.Errorf("GATEWAY_COLLECTOR_TOKEN is required")
	}
	if c.BaseURL == "" {
		return fmt.Errorf("GATEWAY_BASE_URL is required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("GATEWAY_COLLECTOR_POLL_INTERVAL must be positive")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL must be positive")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error", "":
		// valid
	default:
		return fmt.Errorf("GATEWAY_COLLECTOR_LOG_LEVEL must be one of: debug, info, warn, error")
	}
	return nil
}

// defaultSQLiteDir returns the platform-specific default directory for
// OpenCode SQLite database files.
func defaultSQLiteDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "OpenCode")
	default:
		// Linux, macOS, BSD
		return filepath.Join(homeDir, ".local", "share", "opencode")
	}
}

// defaultCursorDir returns the default directory for cursor state files.
// Falls back to the current working directory.
func defaultCursorDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	return dir
}

// getDurationEnv reads a duration environment variable or returns the default.
func getDurationEnv(key string, defaultVal time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return defaultVal
	}
	return d
}

// getEnvWithDefault reads an environment variable or returns the default.
func getEnvWithDefault(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

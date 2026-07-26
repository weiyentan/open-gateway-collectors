// Package config provides configuration loading and validation for the
// opencode-collector application. All configuration is sourced from
// environment variables with sensible defaults.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const defaultBatchLimit = 100

// Config holds all configuration for the collector application.
type Config struct {
	// Token is the bearer token used to authenticate to the Gateway.
	// Required when Transport is "http".
	Token string `env:"GATEWAY_COLLECTOR_TOKEN"`

	// BaseURL is the base URL of the Gateway API.
	// Required when Transport is "http".
	BaseURL string `env:"GATEWAY_BASE_URL"`

	// PollInterval is how often to poll source databases for new usage records.
	PollInterval time.Duration `env:"GATEWAY_COLLECTOR_POLL_INTERVAL"`

	// HeartbeatInterval is how often to send heartbeats when no new records exist.
	HeartbeatInterval time.Duration `env:"GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL"`

	// BatchLimit is the maximum number of usage records sent in one ingest batch.
	BatchLimit int `env:"GATEWAY_COLLECTOR_BATCH_LIMIT"`

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

	// Transport selects the transport mechanism for sending ingest batches.
	// Valid values: "http", "kafka". Default: "kafka".
	Transport string `env:"GATEWAY_COLLECTOR_TRANSPORT"`

	// KafkaBrokers is a comma-separated list of Kafka bootstrap brokers.
	// Required when Transport is "kafka".
	KafkaBrokers []string `env:"GATEWAY_KAFKA_BROKERS"`

	// KafkaTopic is the Kafka topic to produce ingest batches to.
	// Default: "opencode-usage".
	KafkaTopic string `env:"GATEWAY_KAFKA_TOPIC"`

	// KafkaClientID is the Kafka client ID used when connecting to brokers.
	// Defaults to the hostname.
	KafkaClientID string `env:"GATEWAY_KAFKA_CLIENT_ID"`
}

// Load reads configuration from environment variables with defaults.
// It returns an error if required fields are missing or invalid.
func Load() (*Config, error) {
	cfg := &Config{
		Token:             os.Getenv("GATEWAY_COLLECTOR_TOKEN"),
		BaseURL:           os.Getenv("GATEWAY_BASE_URL"),
		PollInterval:      getDurationEnv("GATEWAY_COLLECTOR_POLL_INTERVAL", 60*time.Second),
		HeartbeatInterval: getDurationEnv("GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL", 120*time.Second),
		BatchLimit:        getIntEnv("GATEWAY_COLLECTOR_BATCH_LIMIT", defaultBatchLimit),
		SQLitePath:             os.Getenv("GATEWAY_COLLECTOR_SQLITE_PATH"),
		SQLiteDir:              getEnvWithDefault("GATEWAY_COLLECTOR_SQLITE_DIR", defaultSQLiteDir()),
		LogLevel:               getEnvWithDefault("GATEWAY_COLLECTOR_LOG_LEVEL", "info"),
		CursorDir:              getEnvWithDefault("GATEWAY_COLLECTOR_CURSOR_DIR", defaultCursorDir()),
		ExcludeRecheckInterval: getDurationEnv("GATEWAY_COLLECTOR_EXCLUDE_RECHECK_INTERVAL", 3*time.Hour),
		Transport:              getEnvWithDefault("GATEWAY_COLLECTOR_TRANSPORT", "kafka"),
		KafkaBrokers:           getStringSliceEnv("GATEWAY_KAFKA_BROKERS"),
		KafkaTopic:             getEnvWithDefault("GATEWAY_KAFKA_TOPIC", "opencode-usage"),
		KafkaClientID:          os.Getenv("GATEWAY_KAFKA_CLIENT_ID"),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks that required fields are set and optional fields are valid.
func (c *Config) Validate() error {
	switch c.Transport {
	case "http":
		if c.Token == "" {
			return fmt.Errorf("GATEWAY_COLLECTOR_TOKEN is required for http transport")
		}
		if c.BaseURL == "" {
			return fmt.Errorf("GATEWAY_BASE_URL is required for http transport")
		}
	case "kafka":
		if len(c.KafkaBrokers) == 0 {
			return fmt.Errorf("GATEWAY_KAFKA_BROKERS is required for kafka transport")
		}
	case "":
		// Default to http for backward compatibility (tests, embedded use).
		if c.Token == "" {
			return fmt.Errorf("GATEWAY_COLLECTOR_TOKEN is required")
		}
		if c.BaseURL == "" {
			return fmt.Errorf("GATEWAY_BASE_URL is required")
		}
	default:
		return fmt.Errorf("GATEWAY_COLLECTOR_TRANSPORT must be http or kafka, got: %s", c.Transport)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("GATEWAY_COLLECTOR_POLL_INTERVAL must be positive")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL must be positive")
	}
	if c.BatchLimit <= 0 {
		return fmt.Errorf("GATEWAY_COLLECTOR_BATCH_LIMIT must be positive")
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

// getIntEnv reads an integer environment variable or returns the default.
func getIntEnv(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return i
}

// getEnvWithDefault reads an environment variable or returns the default.
func getEnvWithDefault(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

// getStringSliceEnv reads a comma-separated environment variable and returns
// a slice of trimmed, non-empty strings. Returns nil if the variable is unset.
func getStringSliceEnv(key string) []string {
	val := os.Getenv(key)
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// Clear all env vars and restore them after.
	saved := saveEnv()
	defer restoreEnv(saved)
	clearEnv()

	// Set required fields for kafka transport (the default).
	t.Setenv("GATEWAY_KAFKA_BROKERS", "localhost:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned unexpected error: %v", err)
	}

	if cfg.PollInterval != 60*time.Second {
		t.Errorf("PollInterval = %v, want %v", cfg.PollInterval, 60*time.Second)
	}
	if cfg.HeartbeatInterval != 120*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", cfg.HeartbeatInterval, 120*time.Second)
	}
	if cfg.BatchLimit != 100 {
		t.Errorf("BatchLimit = %d, want %d", cfg.BatchLimit, 100)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.Transport != "kafka" {
		t.Errorf("Transport = %q, want %q", cfg.Transport, "kafka")
	}
	if cfg.KafkaTopic != "opencode-usage" {
		t.Errorf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "opencode-usage")
	}
}

func TestLoadOverrides(t *testing.T) {
	saved := saveEnv()
	defer restoreEnv(saved)
	clearEnv()

	t.Setenv("GATEWAY_COLLECTOR_TRANSPORT", "http")
	t.Setenv("GATEWAY_COLLECTOR_TOKEN", "override-token")
	t.Setenv("GATEWAY_BASE_URL", "https://gateway.example.com")
	t.Setenv("GATEWAY_COLLECTOR_POLL_INTERVAL", "30s")
	t.Setenv("GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL", "60s")
	t.Setenv("GATEWAY_COLLECTOR_BATCH_LIMIT", "25")
	t.Setenv("GATEWAY_COLLECTOR_SQLITE_PATH", "/custom/path/db.sqlite")
	t.Setenv("GATEWAY_COLLECTOR_SQLITE_DIR", "/custom/sqlite")
	t.Setenv("GATEWAY_COLLECTOR_LOG_LEVEL", "debug")
	t.Setenv("GATEWAY_COLLECTOR_CURSOR_DIR", "/custom/cursors")
	t.Setenv("GATEWAY_KAFKA_BROKERS", "kafka1:9092,kafka2:9092")
	t.Setenv("GATEWAY_KAFKA_TOPIC", "custom-usage")
	t.Setenv("GATEWAY_KAFKA_CLIENT_ID", "my-collector")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned unexpected error: %v", err)
	}

	if cfg.Token != "override-token" {
		t.Errorf("Token = %q, want %q", cfg.Token, "override-token")
	}
	if cfg.BaseURL != "https://gateway.example.com" {
		t.Errorf("BaseURL = %q, want %q", cfg.BaseURL, "https://gateway.example.com")
	}
	if cfg.PollInterval != 30*time.Second {
		t.Errorf("PollInterval = %v, want %v", cfg.PollInterval, 30*time.Second)
	}
	if cfg.HeartbeatInterval != 60*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", cfg.HeartbeatInterval, 60*time.Second)
	}
	if cfg.BatchLimit != 25 {
		t.Errorf("BatchLimit = %d, want %d", cfg.BatchLimit, 25)
	}
	if cfg.SQLitePath != "/custom/path/db.sqlite" {
		t.Errorf("SQLitePath = %q, want %q", cfg.SQLitePath, "/custom/path/db.sqlite")
	}
	if cfg.SQLiteDir != "/custom/sqlite" {
		t.Errorf("SQLiteDir = %q, want %q", cfg.SQLiteDir, "/custom/sqlite")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
	if cfg.CursorDir != "/custom/cursors" {
		t.Errorf("CursorDir = %q, want %q", cfg.CursorDir, "/custom/cursors")
	}
	if cfg.Transport != "http" {
		t.Errorf("Transport = %q, want %q", cfg.Transport, "http")
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "kafka1:9092" || cfg.KafkaBrokers[1] != "kafka2:9092" {
		t.Errorf("KafkaBrokers = %v, want [kafka1:9092 kafka2:9092]", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "custom-usage" {
		t.Errorf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "custom-usage")
	}
	if cfg.KafkaClientID != "my-collector" {
		t.Errorf("KafkaClientID = %q, want %q", cfg.KafkaClientID, "my-collector")
	}
}

func TestLoadMissingToken(t *testing.T) {
	saved := saveEnv()
	defer restoreEnv(saved)
	clearEnv()

	// Set BaseURL and transport=http but not Token.
	t.Setenv("GATEWAY_COLLECTOR_TRANSPORT", "http")
	t.Setenv("GATEWAY_BASE_URL", "http://localhost:8080")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for missing token, got nil")
	}
}

func TestLoadMissingBaseURL(t *testing.T) {
	saved := saveEnv()
	defer restoreEnv(saved)
	clearEnv()

	// Set Token and transport=http but not BaseURL.
	t.Setenv("GATEWAY_COLLECTOR_TRANSPORT", "http")
	t.Setenv("GATEWAY_COLLECTOR_TOKEN", "test-token")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for missing BaseURL, got nil")
	}
}

func TestValidateLogLevel(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		wantErr bool
	}{
		{"debug is valid", "debug", false},
		{"info is valid", "info", false},
		{"warn is valid", "warn", false},
		{"error is valid", "error", false},
		{"empty is valid", "", false},
		{"invalid level", "trace", true},
		{"invalid level uppercase", "INFO", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Token:             "t",
				BaseURL:           "http://localhost",
				PollInterval:      60 * time.Second,
				HeartbeatInterval: 120 * time.Second,
				BatchLimit:        100,
				LogLevel:          tt.level,
			}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePollInterval(t *testing.T) {
	cfg := &Config{
		Token:             "t",
		BaseURL:           "http://localhost",
		PollInterval:      0,
		HeartbeatInterval: 120 * time.Second,
		BatchLimit:        100,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for zero poll interval")
	}
}

func TestValidateHeartbeatInterval(t *testing.T) {
	cfg := &Config{
		Token:             "t",
		BaseURL:           "http://localhost",
		PollInterval:      60 * time.Second,
		HeartbeatInterval: -1,
		BatchLimit:        100,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for negative heartbeat interval")
	}
}

func TestValidateBatchLimit(t *testing.T) {
	cfg := &Config{
		Token:             "t",
		BaseURL:           "http://localhost",
		PollInterval:      60 * time.Second,
		HeartbeatInterval: 120 * time.Second,
		BatchLimit:        0,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for zero batch limit")
	}
}

// Test helpers

// saveEnv saves all GATEWAY-related env vars so they can be restored.
func saveEnv() map[string]string {
	keys := []string{
		"GATEWAY_COLLECTOR_TOKEN",
		"GATEWAY_BASE_URL",
		"GATEWAY_COLLECTOR_POLL_INTERVAL",
		"GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL",
		"GATEWAY_COLLECTOR_BATCH_LIMIT",
		"GATEWAY_COLLECTOR_SQLITE_PATH",
		"GATEWAY_COLLECTOR_SQLITE_DIR",
		"GATEWAY_COLLECTOR_LOG_LEVEL",
		"GATEWAY_COLLECTOR_CURSOR_DIR",
		"GATEWAY_COLLECTOR_TRANSPORT",
		"GATEWAY_KAFKA_BROKERS",
		"GATEWAY_KAFKA_TOPIC",
		"GATEWAY_KAFKA_CLIENT_ID",
	}
	saved := make(map[string]string, len(keys))
	for _, k := range keys {
		saved[k] = os.Getenv(k)
	}
	return saved
}

// restoreEnv restores environment variables from a saved map.
func restoreEnv(saved map[string]string) {
	for k, v := range saved {
		if v == "" {
			os.Unsetenv(k)
		} else {
			os.Setenv(k, v)
		}
	}
}

// clearEnv unsets all GATEWAY-related env vars.
func clearEnv() {
	keys := []string{
		"GATEWAY_COLLECTOR_TOKEN",
		"GATEWAY_BASE_URL",
		"GATEWAY_COLLECTOR_POLL_INTERVAL",
		"GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL",
		"GATEWAY_COLLECTOR_BATCH_LIMIT",
		"GATEWAY_COLLECTOR_SQLITE_PATH",
		"GATEWAY_COLLECTOR_SQLITE_DIR",
		"GATEWAY_COLLECTOR_LOG_LEVEL",
		"GATEWAY_COLLECTOR_CURSOR_DIR",
		"GATEWAY_COLLECTOR_TRANSPORT",
		"GATEWAY_KAFKA_BROKERS",
		"GATEWAY_KAFKA_TOPIC",
		"GATEWAY_KAFKA_CLIENT_ID",
	}
	for _, k := range keys {
		os.Unsetenv(k)
	}
}

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

	// Set required fields.
	t.Setenv("GATEWAY_COLLECTOR_TOKEN", "test-token")
	t.Setenv("GATEWAY_BASE_URL", "http://localhost:8080")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned unexpected error: %v", err)
	}

	if cfg.Token != "test-token" {
		t.Errorf("Token = %q, want %q", cfg.Token, "test-token")
	}
	if cfg.BaseURL != "http://localhost:8080" {
		t.Errorf("BaseURL = %q, want %q", cfg.BaseURL, "http://localhost:8080")
	}
	if cfg.PollInterval != 60*time.Second {
		t.Errorf("PollInterval = %v, want %v", cfg.PollInterval, 60*time.Second)
	}
	if cfg.HeartbeatInterval != 120*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", cfg.HeartbeatInterval, 120*time.Second)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
}

func TestLoadOverrides(t *testing.T) {
	saved := saveEnv()
	defer restoreEnv(saved)
	clearEnv()

	t.Setenv("GATEWAY_COLLECTOR_TOKEN", "override-token")
	t.Setenv("GATEWAY_BASE_URL", "https://gateway.example.com")
	t.Setenv("GATEWAY_COLLECTOR_POLL_INTERVAL", "30s")
	t.Setenv("GATEWAY_COLLECTOR_HEARTBEAT_INTERVAL", "60s")
	t.Setenv("GATEWAY_COLLECTOR_SQLITE_PATH", "/custom/path/db.sqlite")
	t.Setenv("GATEWAY_COLLECTOR_SQLITE_DIR", "/custom/sqlite")
	t.Setenv("GATEWAY_COLLECTOR_LOG_LEVEL", "debug")
	t.Setenv("GATEWAY_COLLECTOR_CURSOR_DIR", "/custom/cursors")

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
}

func TestLoadMissingToken(t *testing.T) {
	saved := saveEnv()
	defer restoreEnv(saved)
	clearEnv()

	// Set BaseURL but not Token.
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

	// Set Token but not BaseURL.
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
	}
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for negative heartbeat interval")
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
		"GATEWAY_COLLECTOR_SQLITE_PATH",
		"GATEWAY_COLLECTOR_SQLITE_DIR",
		"GATEWAY_COLLECTOR_LOG_LEVEL",
		"GATEWAY_COLLECTOR_CURSOR_DIR",
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
		"GATEWAY_COLLECTOR_SQLITE_PATH",
		"GATEWAY_COLLECTOR_SQLITE_DIR",
		"GATEWAY_COLLECTOR_LOG_LEVEL",
		"GATEWAY_COLLECTOR_CURSOR_DIR",
	}
	for _, k := range keys {
		os.Unsetenv(k)
	}
}

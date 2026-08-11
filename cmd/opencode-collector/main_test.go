package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildTestBinary builds the collector binary into a temp dir and returns
// its path.
func buildTestBinary(t *testing.T, name string) string {
	t.Helper()
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to determine module root: %v", err)
	}
	binPath := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/opencode-collector")
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build test binary: %v\n%s", err, out)
	}
	return binPath
}

// gatewayEnv returns the inherited environment with all GATEWAY_* variables
// stripped, so tests are hermetic regardless of the host environment.
func gatewayEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GATEWAY_") {
			env = append(env, kv)
		}
	}
	return env
}

func TestVersionFlag(t *testing.T) {
	// Determine the module root: test lives in cmd/opencode-collector/,
	// so module root is ../../ relative to the test file.
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to determine module root: %v", err)
	}

	// Build a test binary with a known version injected via ldflags.
	binPath := filepath.Join(t.TempDir(), "opencode-collector-test")
	cmd := exec.Command("go", "build",
		"-ldflags=-X main.Version=1.0.0-test",
		"-o", binPath,
		"./cmd/opencode-collector",
	)
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build test binary: %v\n%s", err, out)
	}

	// Run the binary with -version flag.
	output, err := exec.Command(binPath, "-version").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to run binary: %v\n%s", err, output)
	}

	expected := "opencode-collector v1.0.0-test\n"
	if string(output) != expected {
		t.Errorf("expected %q, got %q", expected, string(output))
	}
}

func TestVersionFlagDefault(t *testing.T) {
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to determine module root: %v", err)
	}

	// Build without ldflags — Version defaults to "dev".
	binPath := filepath.Join(t.TempDir(), "opencode-collector-default-test")
	cmd := exec.Command("go", "build",
		"-o", binPath,
		"./cmd/opencode-collector",
	)
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build test binary: %v\n%s", err, out)
	}

	output, err := exec.Command(binPath, "-version").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to run binary: %v\n%s", err, output)
	}

	// Without ldflags, the default Version is "dev".
	expected := "opencode-collector vdev\n"
	if string(output) != expected {
		t.Errorf("expected %q, got %q", expected, string(output))
	}
}

func TestNoArgsDoesNotPrintVersion(t *testing.T) {
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to determine module root: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "opencode-collector-noargs-test")
	cmd := exec.Command("go", "build",
		"-ldflags=-X main.Version=test",
		"-o", binPath,
		"./cmd/opencode-collector",
	)
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build test binary: %v\n%s", err, out)
	}

	// Run without args — should exit with error (no config loaded).
	// We just verify it doesn't print the version string.
	output, _ := exec.Command(binPath).CombinedOutput()
	if string(output) == "opencode-collector vtest\n" {
		t.Errorf("expected no version output when running without args, got %q", string(output))
	}
}

func TestReplayUntilEnvInvalidFailsStartup(t *testing.T) {
	binPath := buildTestBinary(t, "opencode-collector-env-invalid-test")

	cmd := exec.Command(binPath)
	cmd.Env = append(gatewayEnv(),
		"GATEWAY_COLLECTOR_TRANSPORT=http",
		"GATEWAY_COLLECTOR_TOKEN=test-token",
		"GATEWAY_BASE_URL=http://localhost:8080",
		"GATEWAY_COLLECTOR_REPLAY_UNTIL=not-a-timestamp",
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit for invalid GATEWAY_COLLECTOR_REPLAY_UNTIL, got success\n%s", output)
	}
	if !strings.Contains(string(output), "GATEWAY_COLLECTOR_REPLAY_UNTIL") {
		t.Errorf("expected startup error to name GATEWAY_COLLECTOR_REPLAY_UNTIL, got:\n%s", output)
	}
}

func TestReplayUntilFlagInvalidFailsStartup(t *testing.T) {
	binPath := buildTestBinary(t, "opencode-collector-flag-invalid-test")

	cmd := exec.Command(binPath, "-replay-until=not-a-timestamp")
	cmd.Env = append(gatewayEnv(),
		"GATEWAY_COLLECTOR_TRANSPORT=http",
		"GATEWAY_COLLECTOR_TOKEN=test-token",
		"GATEWAY_BASE_URL=http://localhost:8080",
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit for invalid -replay-until, got success\n%s", output)
	}
	if !strings.Contains(string(output), "invalid value for -replay-until") {
		t.Errorf("expected startup error to name -replay-until, got:\n%s", output)
	}
}

func TestReplayUntilFlagOverridesEnv(t *testing.T) {
	binPath := buildTestBinary(t, "opencode-collector-override-test")

	// The env value is invalid, so startup only succeeds if the valid
	// -replay-until flag value replaces it (CLI overrides env).
	cmd := exec.Command(binPath, "-replay-until=2026-01-02T15:04:05Z")
	cmd.Env = append(gatewayEnv(),
		"GATEWAY_COLLECTOR_TRANSPORT=http",
		"GATEWAY_COLLECTOR_TOKEN=test-token",
		"GATEWAY_BASE_URL=http://localhost:8080",
		"GATEWAY_COLLECTOR_REPLAY_UNTIL=not-a-timestamp",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start binary: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		// Exited early: the invalid env value was parsed instead of the
		// flag value, so the override did not happen.
		t.Fatalf("binary exited before timeout (err=%v); expected -replay-until to override the invalid env value", err)
	case <-time.After(2 * time.Second):
		// Still running — startup succeeded, flag overrode env.
		if err := cmd.Process.Kill(); err != nil {
			t.Errorf("failed to kill binary: %v", err)
		}
		<-done
	}
}

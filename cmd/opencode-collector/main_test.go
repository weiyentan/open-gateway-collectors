package main

import (
	"os/exec"
	"path/filepath"
	"testing"
)

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

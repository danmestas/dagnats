// cli/sidecar_test.go
// Tests for sidecar CLI command dispatch, help output, config flag
// parsing, status output, and banner formatting. Does not test
// start (blocks and needs real binaries).
package cli

import (
	"os"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/sidecar"
)

// --- Help tests ---

func TestSidecarHelp(t *testing.T) {
	output := captureSidecarOutput(func() {
		runSidecarCmd([]string{"--help"})
	})

	// Positive: should show usage line.
	if !strings.Contains(output, "Usage:") {
		t.Fatalf("expected Usage:, got:\n%s", output)
	}

	// Positive: should mention all subcommands.
	for _, sub := range []string{"start", "install", "status"} {
		if !strings.Contains(output, sub) {
			t.Fatalf("expected %q in help, got:\n%s",
				sub, output)
		}
	}

	// Negative: should not be empty.
	if output == "" {
		t.Fatal("help output should not be empty")
	}
}

func TestSidecarHelpShortFlag(t *testing.T) {
	output := captureSidecarOutput(func() {
		runSidecarCmd([]string{"-h"})
	})

	// Positive: -h should also show usage.
	if !strings.Contains(output, "Usage:") {
		t.Fatalf("expected Usage: with -h, got:\n%s", output)
	}

	// Positive: should mention --config flag.
	if !strings.Contains(output, "--config") {
		t.Fatalf("expected --config in help, got:\n%s", output)
	}
}

// --- Config flag parsing ---

func TestExtractConfigFlagCustom(t *testing.T) {
	args := []string{"--config=/tmp/custom.yaml"}
	got := extractConfigFlag(args)

	// Positive: should extract custom path.
	if got != "/tmp/custom.yaml" {
		t.Fatalf("expected /tmp/custom.yaml, got %q", got)
	}

	// Negative: should not be the default.
	if got == defaultConfigFileName {
		t.Fatal("should use custom path, not default")
	}
}

func TestExtractConfigFlagDefault(t *testing.T) {
	got := extractConfigFlag([]string{})

	// Positive: should return empty when no flag given.
	// Callers apply their own default (e.g. sidecar uses
	// defaultConfigFileName).
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}

	// Negative: should not match any filename.
	if got == defaultConfigFileName {
		t.Fatal("should not return default filename")
	}
}

// --- Status command ---

func TestSidecarStatusCmd(t *testing.T) {
	output := captureSidecarOutput(func() {
		runSidecarStatusCmd([]string{})
	})

	// Positive: should mention both required binaries.
	if !strings.Contains(output, "otelcol") {
		t.Fatalf("expected otelcol in status, got:\n%s", output)
	}
	if !strings.Contains(output, "otlp2parquet") {
		t.Fatalf(
			"expected otlp2parquet in status, got:\n%s", output)
	}
}

func TestSidecarStatusCmdHelp(t *testing.T) {
	output := captureSidecarOutput(func() {
		runSidecarStatusCmd([]string{"--help"})
	})

	// Positive: should show usage.
	if !strings.Contains(output, "Usage:") {
		t.Fatalf("expected Usage:, got:\n%s", output)
	}

	// Negative: should not list binary paths in help mode.
	if strings.Contains(output, "not found") {
		t.Fatalf(
			"help should not show binary status, got:\n%s",
			output)
	}
}

// --- Install command help ---

func TestSidecarInstallCmdHelp(t *testing.T) {
	output := captureSidecarOutput(func() {
		runSidecarInstallCmd([]string{"--help"})
	})

	// Positive: should show usage.
	if !strings.Contains(output, "Usage:") {
		t.Fatalf("expected Usage:, got:\n%s", output)
	}

	// Positive: should mention the bin directory.
	if !strings.Contains(output, ".dagnats/bin") {
		t.Fatalf(
			"expected .dagnats/bin in help, got:\n%s", output)
	}
}

// --- Banner formatting ---

func TestPrintStartBanner(t *testing.T) {
	cfg := sidecar.DefaultConfig()

	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})

	// Positive: should contain collector address.
	if !strings.Contains(output, cfg.Listen) {
		t.Fatalf(
			"expected listen addr in banner, got:\n%s", output)
	}

	// Positive: should contain storage type.
	if !strings.Contains(output, "local") {
		t.Fatalf(
			"expected storage type in banner, got:\n%s", output)
	}

	// Positive: should show stdio for default MCP.
	if !strings.Contains(output, "stdio") {
		t.Fatalf(
			"expected stdio MCP transport, got:\n%s", output)
	}

	// Negative: should not be empty.
	if output == "" {
		t.Fatal("banner should not be empty")
	}
}

func TestPrintStartBannerCustomMCP(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	cfg.MCP.Listen = "localhost:8080"

	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})

	// Positive: should show custom MCP address.
	if !strings.Contains(output, "localhost:8080") {
		t.Fatalf(
			"expected custom MCP listen, got:\n%s", output)
	}

	// Negative: should not show stdio.
	if strings.Contains(output, "stdio") {
		t.Fatalf(
			"should not show stdio with custom listen, got:\n%s",
			output)
	}
}

// --- Dispatch tests ---

func TestSidecarCmdDispatchNoArgs(t *testing.T) {
	// With no args, should default to start which will try to
	// load config. We intercept the exit to verify dispatch.
	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	// Run in a temp dir with no config and no binaries.
	oldDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	os.Chdir(tmpDir)
	defer os.Chdir(oldDir)

	runSidecarCmd([]string{})

	// Positive: should attempt start and fail on missing binaries.
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	// Negative: should not succeed.
	if exitCode == 0 {
		t.Fatal("should not succeed without binaries")
	}
}

func TestSidecarCmdDispatchUnknown(t *testing.T) {
	// Unknown subcommand should error, not attempt start.
	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	// Capture stderr to verify error message.
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	runSidecarCmd([]string{"unknown-flag"})

	w.Close()
	os.Stderr = oldStderr

	buf := make([]byte, 8192)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	// Positive: should exit with code 1.
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	// Negative: should not attempt start.
	if strings.Contains(output, "Sidecar started") ||
		strings.Contains(output, "missing binaries") {
		t.Fatal("should not attempt start for unknown subcommand")
	}
}

// --- Collector YAML path ---

func TestCollectorYAMLPath(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	got := collectorYAMLPath(cfg)

	// Positive: should end with otelcol-config.yaml.
	if !strings.HasSuffix(got, "otelcol-config.yaml") {
		t.Fatalf("expected otelcol-config.yaml suffix, got %q",
			got)
	}

	// Positive: should contain storage path.
	if !strings.Contains(got, cfg.Storage.LocalPath) {
		t.Fatalf("expected storage path in config path, got %q",
			got)
	}
}

func TestSidecarCmdUnknownSubcommand(t *testing.T) {
	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	// Capture stderr to check error output.
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	runSidecarCmd([]string{"bogus"})

	w.Close()
	os.Stderr = oldStderr

	buf := make([]byte, 8192)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	// Positive: should exit with code 1.
	if exitCode != 1 {
		t.Fatalf("expected exit 1, got %d", exitCode)
	}

	// Positive: should show error for unknown command.
	if !strings.Contains(output, "unknown sidecar command") {
		t.Fatalf("expected unknown command error, got:\n%s",
			output)
	}

	// Negative: should not attempt start (no config error).
	if strings.Contains(output, "error: load config") ||
		strings.Contains(output, "missing binaries") {
		t.Fatal("should not attempt start for unknown subcommand")
	}
}

func TestPrintStartBannerExportHint(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})
	if !strings.Contains(output, "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Fatalf("expected OTEL env var in banner, got:\n%s", output)
	}
	if !strings.Contains(output, "localhost") {
		t.Fatalf("expected localhost in export hint, got:\n%s", output)
	}
}

func TestPrintStartBannerBackend(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	cfg.Backend = &sidecar.BackendConfig{
		Endpoint: "https://otel.prod.example.com",
	}
	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})
	if !strings.Contains(output, "https://otel.prod.example.com") {
		t.Fatalf("expected backend endpoint in banner, got:\n%s", output)
	}
	if !strings.Contains(output, "forwarding") {
		t.Fatalf("expected 'forwarding' in banner, got:\n%s", output)
	}
}

func TestPrintStartBannerNoBackend(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})
	if strings.Contains(output, "Backend:") {
		t.Fatalf("should not show Backend when nil, got:\n%s", output)
	}
}

// captureSidecarOutput captures stdout from a function.
func captureSidecarOutput(fn func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 8192)
	n, _ := r.Read(buf)
	return string(buf[:n])
}

// cli/sidecar_test.go
// Tests for sidecar CLI command dispatch, help output, config flag
// parsing, status output, and banner formatting. Does not test
// start (blocks and needs real binaries).
package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	// With no args, dispatch should route to runSidecarStartCmd. The
	// test verifies that routing by feeding an unparseable dagnats.yaml
	// so loadSidecarConfig fails before startSupervisor is reached —
	// otherwise the test would hang spawning real collector processes
	// (sidecar.LoadConfig silently falls back to DefaultConfig when
	// the file is missing, which is fine for production UX but lets
	// execution fall through to startSupervisor in the test override).
	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	oldDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() { _ = os.Chdir(oldDir) }()

	// Unparseable YAML — LoadConfig returns an error, loadSidecarConfig
	// calls exitFunc(1) and returns nil, runSidecarStartCmd bails.
	cfgPath := filepath.Join(tmpDir, defaultConfigFileName)
	if err := os.WriteFile(cfgPath, []byte(":::not yaml:::"), 0o644); err != nil {
		t.Fatalf("write bad config: %v", err)
	}

	runSidecarCmd([]string{})

	// Positive: dispatch reached start, which exited on bad config.
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	// Negative: must not have succeeded (would mean dispatch missed start).
	if exitCode == 0 {
		t.Fatal("should not succeed with malformed config")
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

func TestPrintStartBannerHealthLine(t *testing.T) {
	cfg := sidecar.DefaultConfig()
	output := captureSidecarOutput(func() {
		printStartBanner(cfg)
	})

	// Positive: should show the /healthz path.
	if !strings.Contains(output, "/healthz") {
		t.Fatalf(
			"expected /healthz in banner, got:\n%s", output)
	}

	// Positive: should show the supervisor listen address.
	if !strings.Contains(output, "localhost:4320") {
		t.Fatalf(
			"expected localhost:4320 in banner, got:\n%s",
			output)
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

// --- Dry run ---

func TestSidecarStartDryRun(t *testing.T) {
	dir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldDir)

	output := captureSidecarOutput(func() {
		runSidecarStartCmd([]string{"--dry-run"})
	})

	// Positive: should contain OTel collector config sections.
	if !strings.Contains(output, "receivers:") {
		t.Fatalf("expected receivers:, got:\n%s", output)
	}

	// Positive: should contain OTLP receiver.
	if !strings.Contains(output, "otlp:") {
		t.Fatalf("expected otlp:, got:\n%s", output)
	}

	// Negative: should not actually start the sidecar.
	if strings.Contains(output, "Sidecar started") {
		t.Fatal("dry-run should not start")
	}
}

// --- Init command ---

func TestSidecarInitCreatesFile(t *testing.T) {
	dir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldDir)

	output := captureSidecarOutput(func() {
		runSidecarInitCmd([]string{})
	})

	cfgPath := filepath.Join(dir, "dagnats.yaml")
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("expected dagnats.yaml: %v", err)
	}

	data, _ := os.ReadFile(cfgPath)

	// Positive: file should contain commented listen.
	if !strings.Contains(string(data), "# listen:") {
		t.Fatal("expected commented listen")
	}

	// Positive: output should mention the filename.
	if !strings.Contains(output, "dagnats.yaml") {
		t.Fatal("expected confirmation")
	}

	// Negative: config file should not be empty.
	if len(data) == 0 {
		t.Fatal("config file should not be empty")
	}
}

func TestSidecarInitRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldDir)

	cfgPath := filepath.Join(dir, "dagnats.yaml")
	os.WriteFile(cfgPath, []byte("existing"), 0o600)

	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	runSidecarInitCmd([]string{})

	// Positive: should exit with code 1.
	if exitCode != 1 {
		t.Fatalf("expected exit 1, got %d", exitCode)
	}

	// Negative: should not overwrite existing content.
	data, _ := os.ReadFile(cfgPath)
	if string(data) != "existing" {
		t.Fatal("should not overwrite")
	}
}

// --- Health endpoint status ---

func TestSidecarStatusWithHealthEndpoint(t *testing.T) {
	handler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"status":"ok",`+
				`"uptime_seconds":3621,`+
				`"processes":[{"name":"otelcol",`+
				`"status":"running","pid":12345,`+
				`"restarts":0,"uptime_seconds":3621}],`+
				`"storage":{"path":"./telemetry-data",`+
				`"type":"local"}}`)
		},
	)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	output := captureSidecarOutput(func() {
		printHealthStatus(srv.URL)
	})

	// Positive: should show the process name.
	if !strings.Contains(output, "otelcol") {
		t.Fatalf("expected otelcol, got:\n%s", output)
	}

	// Positive: should show running status.
	if !strings.Contains(output, "running") {
		t.Fatalf("expected running, got:\n%s", output)
	}

	// Negative: should not say "not running".
	if strings.Contains(output, "not running") {
		t.Fatalf(
			"should not say not running, got:\n%s", output)
	}
}

func TestSidecarStatusFallback(t *testing.T) {
	output := captureSidecarOutput(func() {
		printHealthStatus("http://127.0.0.1:19999")
	})

	// Positive: should indicate sidecar is not running.
	if !strings.Contains(output, "not running") {
		t.Fatalf("expected 'not running', got:\n%s", output)
	}

	// Positive: should show binary names in fallback.
	if !strings.Contains(output, "otelcol") {
		t.Fatalf(
			"expected otelcol in fallback, got:\n%s", output)
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

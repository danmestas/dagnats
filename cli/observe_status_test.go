// cli/observe_status_test.go
// Tests for the observe status command. Unit tests cover formatting,
// flag parsing, and host extraction. Integration tests use an embedded
// NATS server to verify TELEMETRY stream info collection.
package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// --- Unit tests: extractHostPort ---

func TestExtractHostPort(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"http://localhost:4318", "localhost:4318"},
		{"https://otel.example.com:443", "otel.example.com:443"},
		{"grpc://collector:4317", "collector:4317"},
		{"localhost:4318", "localhost:4318"},
		{"http://localhost:4318/v1/traces", "localhost:4318"},
		{"collector.local", "collector.local:4318"},
	}

	for _, tt := range tests {
		got := extractHostPort(tt.input)

		// Positive: should match expected
		if got != tt.want {
			t.Errorf("extractHostPort(%q) = %q, want %q",
				tt.input, got, tt.want)
		}

		// Negative: should not be empty
		if got == "" {
			t.Errorf("extractHostPort(%q) must not be empty",
				tt.input)
		}
	}
}

// --- Unit tests: flag parsing ---

func TestExtractNatsURL(t *testing.T) {
	args := []string{"--nats-url=nats://custom:4222"}
	got := extractNatsURL(args)

	// Positive: should extract the URL
	if got != "nats://custom:4222" {
		t.Fatalf("expected nats://custom:4222, got %q", got)
	}

	// Negative: should not be the default
	if got == "nats://127.0.0.1:4222" {
		t.Fatal("should use custom URL, not default")
	}
}

func TestExtractNatsURLDefault(t *testing.T) {
	// Clear env vars to ensure default is used.
	t.Setenv("DAGNATS_NATS_URL", "")
	t.Setenv("NATS_URL", "")

	got := extractNatsURL([]string{})

	// Positive: should return the NATS default
	if got == "" {
		t.Fatal("should return a default NATS URL")
	}

	// Negative: should not contain custom
	if strings.Contains(got, "custom") {
		t.Fatal("should not contain custom")
	}
}

func TestStripNatsURLFlag(t *testing.T) {
	args := []string{"--json", "--nats-url=x", "status"}
	got := stripNatsURLFlag(args)

	// Positive: --nats-url removed
	if len(got) != 2 {
		t.Fatalf("expected 2 args, got %d", len(got))
	}

	// Negative: no arg should start with --nats-url
	for _, arg := range got {
		if strings.HasPrefix(arg, "--nats-url=") {
			t.Fatal("should have stripped --nats-url")
		}
	}
}

// --- Unit tests: OTLP status ---

func TestCollectOTLPStatusNotConfigured(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	got := collectOTLPStatus()

	// Positive: should report not configured
	if got.Status != "not configured" {
		t.Fatalf("expected not configured, got %q", got.Status)
	}

	// Negative: endpoint should be empty
	if got.Endpoint != "" {
		t.Fatal("endpoint should be empty when not configured")
	}
}

func TestCollectOTLPStatusConfigured(t *testing.T) {
	// Use an unreachable endpoint to test configuration detection
	// without actually connecting.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT",
		"http://192.0.2.1:4318")

	got := collectOTLPStatus()

	// Positive: should report configured
	if !strings.HasPrefix(got.Status, "configured") {
		t.Fatalf("expected configured status, got %q",
			got.Status)
	}

	// Positive: endpoint should be set
	if got.Endpoint != "http://192.0.2.1:4318" {
		t.Fatalf("expected endpoint, got %q", got.Endpoint)
	}

	// Negative: should not be "not configured"
	if got.Status == "not configured" {
		t.Fatal("should not be not configured")
	}
}

// --- Unit tests: sidecar status ---

func TestCollectSidecarStatusNotDetected(t *testing.T) {
	// Default sidecar address is unlikely to be running in tests.
	got := collectSidecarStatus()

	// Positive: should have an address
	if got.Address == "" {
		t.Fatal("address should not be empty")
	}

	// Positive: status should be a known value
	if got.Status != "detected" && got.Status != "not detected" {
		t.Fatalf("unexpected status %q", got.Status)
	}
}

// --- Unit tests: buildTelemetryStatus ---

func TestBuildTelemetryStatus(t *testing.T) {
	info := jetstream.StreamInfo{
		State: jetstream.StreamState{
			Msgs:      100,
			Bytes:     2048,
			Consumers: 1,
		},
	}

	got := buildTelemetryStatus(info)

	// Positive: status is ok
	if got.Status != "ok" {
		t.Fatalf("expected ok, got %q", got.Status)
	}

	// Positive: messages match
	if got.Messages != 100 {
		t.Fatalf("expected 100 messages, got %d", got.Messages)
	}

	// Negative: first msg should be empty for zero time
	if got.FirstMsg != "" {
		t.Fatal("first msg should be empty for zero time")
	}
}

// --- Unit tests: observe command dispatch ---

func TestRunObserveCmdHelp(t *testing.T) {
	output := captureObserveOutput(func() {
		runObserveCmd([]string{"--help"})
	})

	// Positive: should show usage
	if !strings.Contains(output, "Usage:") {
		t.Fatalf("expected usage text, got:\n%s", output)
	}

	// Positive: should mention status subcommand
	if !strings.Contains(output, "status") {
		t.Fatalf("should mention status, got:\n%s", output)
	}
}

func TestRunObserveCmdNoArgs(t *testing.T) {
	output := captureObserveOutput(func() {
		runObserveCmd([]string{})
	})

	// Positive: should show usage
	if !strings.Contains(output, "Usage:") {
		t.Fatalf("expected usage text, got:\n%s", output)
	}

	// Negative: should not be empty
	if output == "" {
		t.Fatal("output should not be empty")
	}
}

func TestRunObserveCmdUnknown(t *testing.T) {
	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	runObserveCmd([]string{"bogus"})

	// Positive: should exit with code 1
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	// Negative: should not succeed
	if exitCode == 0 {
		t.Fatal("should not succeed with unknown subcommand")
	}
}

// --- Health-based sidecar status ---

func TestCollectSidecarStatusFromHealth(t *testing.T) {
	handler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(
				"Content-Type", "application/json",
			)
			fmt.Fprint(w, `{"status":"ok","processes":[`+
				`{"name":"a","status":"running"},`+
				`{"name":"b","status":"running"},`+
				`{"name":"c","status":"running"}]}`)
		},
	)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	got := collectSidecarStatusFromAddr(addr)

	// Positive: should contain the process count.
	if !strings.Contains(got.Status, "3") {
		t.Fatalf(
			"expected count in status, got %q", got.Status,
		)
	}

	// Negative: should not be "not detected".
	if got.Status == "not detected" {
		t.Fatal("should detect running sidecar")
	}
}

// --- Integration test: TELEMETRY stream info ---

func TestObserveStatusTelemetryIntegration(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	ts := collectTelemetryStatus(srv.ClientURL())

	// Positive: should report ok
	if ts.Status != "ok" {
		t.Fatalf("expected ok, got %q", ts.Status)
	}

	// Positive: messages starts at 0 for empty stream
	if ts.Messages != 0 {
		t.Fatalf("expected 0 messages, got %d", ts.Messages)
	}

	// Negative: status should not be an error state
	if ts.Status == "stream not found" ||
		ts.Status == "nats disconnected" {
		t.Fatalf("should not be error state: %q", ts.Status)
	}
}

func TestObserveStatusTelemetryNoStream(t *testing.T) {
	srv, _ := natsutil.StartTestServer(t)
	// Do NOT call SetupAll — stream does not exist.

	ts := collectTelemetryStatus(srv.ClientURL())

	// Positive: should report stream not found
	if ts.Status != "stream not found" {
		t.Fatalf("expected stream not found, got %q", ts.Status)
	}

	// Negative: should not be ok
	if ts.Status == "ok" {
		t.Fatal("should not be ok when stream missing")
	}
}

func TestObserveStatusNATSDisconnected(t *testing.T) {
	// Use a port that nothing is listening on.
	ts := collectTelemetryStatus("nats://127.0.0.1:19998")

	// Positive: should report disconnected
	if ts.Status != "nats disconnected" {
		t.Fatalf("expected nats disconnected, got %q",
			ts.Status)
	}

	// Negative: should not be ok
	if ts.Status == "ok" {
		t.Fatal("should not be ok when NATS unreachable")
	}
}

// --- Integration test: full JSON output ---

func TestObserveStatusJSONIntegration(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("DAGNATS_NATS_URL", srv.ClientURL())
	t.Setenv("NATS_URL", "")

	var buf bytes.Buffer
	status := collectObserveStatus(srv.ClientURL())
	err := FormatJSON(&buf, status)
	if err != nil {
		t.Fatalf("FormatJSON: %v", err)
	}

	output := buf.String()

	// Positive: should be valid JSON
	var parsed observeStatus
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v\noutput:\n%s", err, output)
	}

	// Positive: telemetry status should be ok
	if parsed.Telemetry.Status != "ok" {
		t.Fatalf("expected telemetry ok, got %q",
			parsed.Telemetry.Status)
	}

	// Positive: OTLP should be not configured
	if parsed.OTLP.Status != "not configured" {
		t.Fatalf("expected OTLP not configured, got %q",
			parsed.OTLP.Status)
	}

	// Negative: telemetry should not be nil
	if parsed.Telemetry == nil {
		t.Fatal("telemetry should not be nil")
	}
}

// --- Integration test: human-readable output ---

func TestObserveStatusHumanOutputIntegration(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	status := collectObserveStatus(srv.ClientURL())

	output := captureObserveOutput(func() {
		printObserveStatus(status)
	})

	// Positive: should contain header
	if !strings.Contains(output, "Observability Status") {
		t.Fatalf("expected header, got:\n%s", output)
	}

	// Positive: should contain TELEMETRY section
	if !strings.Contains(output, "TELEMETRY Stream:") {
		t.Fatalf("expected TELEMETRY section, got:\n%s", output)
	}

	// Positive: should contain OTLP section
	if !strings.Contains(output, "OTLP Export:") {
		t.Fatalf("expected OTLP section, got:\n%s", output)
	}

	// Positive: should contain Sidecar section
	if !strings.Contains(output, "Sidecar:") {
		t.Fatalf("expected Sidecar section, got:\n%s", output)
	}

	// Negative: should not be empty
	if output == "" {
		t.Fatal("output should not be empty")
	}
}

// captureObserveOutput captures stdout from a function.
func captureObserveOutput(fn func()) string {
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

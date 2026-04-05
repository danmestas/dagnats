// cli/status_test.go
// Tests for the status command.
// Methodology: integration tests with embedded NATS. Verify health
// output reflects actual NATS state. JSON tests verify structured output.
package cli

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestStatusCommandShowsConnected(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	nc.Close()

	// Point CLI at embedded server.
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	output := captureOutput(func() {
		runSystemStatusCmd([]string{})
	})

	// Positive: output must report healthy state.
	if !strings.Contains(output, "connected") {
		t.Fatalf("expected 'connected' in output, got: %s", output)
	}
	if !strings.Contains(output, "available") {
		t.Fatalf("expected 'available' in output, got: %s", output)
	}

	// Negative: must not report unhealthy state.
	if strings.Contains(output, "disconnected") {
		t.Fatal("output should not contain 'disconnected'")
	}
	if strings.Contains(output, "unavailable") {
		t.Fatal("output should not contain 'unavailable'")
	}
}

func TestStatusCommandJSONOutput(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	output := captureOutput(func() {
		runSystemStatusCmd([]string{"--json"})
	})

	// Positive: output must be valid JSON with expected fields.
	var status systemStatus
	if err := json.Unmarshal(
		[]byte(output), &status,
	); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, output)
	}
	if status.NATS != "connected" {
		t.Fatalf("expected nats=connected, got %q", status.NATS)
	}
	if status.JetStream != "available" {
		t.Fatalf(
			"expected jetstream=available, got %q",
			status.JetStream,
		)
	}

	// Negative: JSON output must not contain human-readable labels.
	if strings.Contains(output, "NATS:") {
		t.Fatal("JSON output should not contain human labels")
	}
	if strings.Contains(output, "Active runs:") {
		t.Fatal("JSON output should not contain human labels")
	}
}

func TestStatusCommandJSONHasStreams(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	output := captureOutput(func() {
		runSystemStatusCmd([]string{"--json"})
	})

	var status systemStatus
	if err := json.Unmarshal(
		[]byte(output), &status,
	); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, output)
	}

	// Positive: streams should be populated.
	if len(status.StreamInfo) == 0 {
		t.Fatal("expected at least one stream in JSON output")
	}
	if status.Streams == 0 {
		t.Fatal("expected non-zero stream_count")
	}

	// Negative: stream names must not be empty.
	for _, s := range status.StreamInfo {
		if s.Name == "" {
			t.Fatal("stream name must not be empty")
		}
	}
}

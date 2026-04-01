// cli/status_test.go
// Tests for the status command.
// Methodology: integration tests with embedded NATS. Verify health
// output reflects actual NATS state.
package cli

import (
	"os"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/natsutil"
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

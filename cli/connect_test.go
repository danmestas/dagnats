// cli/connect_test.go
// Tests for connectService error handling.
// Methodology: verify that connection failures produce friendly errors
// instead of panics. Uses a mock exit function to capture exit behavior.
package cli

import (
	"os"
	"testing"

	"github.com/danmestas/dagnats/natsutil"
)

func TestConnectServiceFriendlyErrorOnBadURL(t *testing.T) {
	// This test validates the exitFunc indirection works for connection
	// failures. The existing code already handled this case with os.Exit,
	// but the new exitFunc var makes it testable.
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", "nats://127.0.0.1:19999")
	defer os.Setenv("NATS_URL", oldURL)

	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	svc, nc := connectService()

	// Positive: exit was called with code 1
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	// Negative: no service or connection returned
	if svc != nil {
		t.Fatal("expected nil service on connection failure")
	}
	if nc != nil {
		t.Fatal("expected nil nc on connection failure")
	}
}

func TestConnectServiceFriendlyErrorOnMissingBuckets(t *testing.T) {
	// Connect to a real NATS but without SetupAll — this is the critical
	// test. Previously this would panic with a raw stack trace.
	srv, nc := natsutil.StartTestServer(t)
	defer nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	var exitCode int
	oldExit := exitFunc
	exitFunc = func(code int) { exitCode = code }
	defer func() { exitFunc = oldExit }()

	svc, nc2 := connectService()

	// Positive: exit was called with code 1
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	// Negative: no service returned
	if svc != nil {
		t.Fatal("expected nil service when buckets missing")
	}
	_ = nc2
}

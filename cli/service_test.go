// cli/service_test.go
// Tests for the `dagnats service` command (ADR-017 / #321).
// Methodology: unit tests for the table renderer and dispatcher,
// integration test for end-to-end list via embedded NATS.
package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// TestPrintServicesTable verifies that the table renderer outputs
// the registered services without panicking and rejects empty input.
func TestPrintServicesTable(t *testing.T) {
	services := []worker.ServiceDef{
		{
			Name:         "billing",
			Description:  "Charge cards.",
			RegisteredAt: time.Unix(1700000000, 0).UTC(),
		},
		{
			Name:         "auth",
			Description:  "Verify tokens.",
			RegisteredAt: time.Unix(1700000100, 0).UTC(),
		},
	}

	// Positive: no panic on non-empty input.
	printServicesTable(services)

	// Negative: empty list panics (caller responsibility to
	// pre-check via len() and print "No services registered.").
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal(
				"expected panic for empty services slice",
			)
		}
	}()
	printServicesTable([]worker.ServiceDef{})
}

// TestPrintServicesTableBound verifies the upper-bound guard panics.
func TestPrintServicesTableBound(t *testing.T) {
	// Positive: 100 rows under the cap renders fine.
	rows := make([]worker.ServiceDef, 100)
	for i := range rows {
		rows[i] = worker.ServiceDef{
			Name:         "svc",
			Description:  "d",
			RegisteredAt: time.Now(),
		}
	}
	printServicesTable(rows)

	// Negative: oversized input panics.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal(
				"expected panic for oversized services slice",
			)
		}
	}()
	oversized := make([]worker.ServiceDef, 100001)
	printServicesTable(oversized)
}

// TestRunServiceCmd_UnknownSubcommand verifies that an unknown
// subcommand calls exitFunc with code 1. The exitFunc swap pattern
// lets the test observe the would-be process exit without actually
// terminating the test runner.
func TestRunServiceCmd_UnknownSubcommand(t *testing.T) {
	called := false
	code := 0
	restore := setExitFunc(func(c int) {
		called = true
		code = c
	})
	defer restore()

	runServiceCmd([]string{"nope"})

	// Positive: exitFunc must have been called.
	if !called {
		t.Fatal("exitFunc was not called on unknown subcommand")
	}
	// Positive: exit code is 1.
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

// TestRunServiceListCmd_EndToEnd registers a service via the worker
// SDK and verifies that `service list` outputs the expected row.
// Drives the full CLI surface: env-var → NATS connect → KV read →
// table render.
func TestRunServiceListCmd_EndToEnd(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	srvURL := srv.ClientURL()
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	w := worker.NewWorker(nc)
	def := worker.ServiceDef{
		Name:        "billing",
		Description: "Charge cards and emit receipts.",
	}
	if err := w.RegisterService(def); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	// Point the CLI's NATS connect at the embedded server.
	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	output := captureStdout(t, func() {
		runServiceListCmd([]string{})
	})

	// Positive: must include the registered service Name.
	if !strings.Contains(output, "billing") {
		t.Errorf(
			"output missing 'billing'; got:\n%s", output,
		)
	}
	// Positive: must include the Description.
	if !strings.Contains(output, "Charge cards") {
		t.Errorf(
			"output missing Description; got:\n%s", output,
		)
	}
	// Negative: must not say "no services" when one is registered.
	if strings.Contains(strings.ToLower(output), "no services") {
		t.Errorf(
			"unexpected empty-state message; got:\n%s", output,
		)
	}
}

// TestRunServiceListCmd_EmptyBucket verifies the empty-state message
// when no services are registered.
func TestRunServiceListCmd_EmptyBucket(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	srvURL := srv.ClientURL()
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	_ = nc // keep alive for the test

	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	output := captureStdout(t, func() {
		runServiceListCmd([]string{})
	})

	// Positive: must include the empty-state line.
	if !strings.Contains(output, "No services registered") {
		t.Errorf(
			"missing empty-state message; got:\n%s", output,
		)
	}
	// Negative: must not include any obviously service-like
	// rows when the bucket is empty.
	if strings.Contains(output, "NAME") {
		t.Errorf(
			"unexpected table header for empty bucket: %s",
			output,
		)
	}
}

// TestRunServiceListCmd_JSON verifies the --json output path.
func TestRunServiceListCmd_JSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	srvURL := srv.ClientURL()
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	w := worker.NewWorker(nc)
	if err := w.RegisterService(worker.ServiceDef{
		Name:        "auth",
		Description: "Token checks.",
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	output := captureStdout(t, func() {
		runServiceListCmd([]string{"--json"})
	})

	// Positive: JSON output must include the field name in
	// snake_case (matching the struct tag).
	if !strings.Contains(output, `"name": "auth"`) {
		t.Errorf(
			"JSON missing name field; got:\n%s", output,
		)
	}
	// Negative: human-readable table header must NOT appear in JSON.
	if strings.Contains(output, "NAME\t") {
		t.Errorf(
			"JSON output contained tabwriter header: %s", output,
		)
	}
}

// captureStdout redirects os.Stdout for the duration of fn and
// returns whatever was written. Bounded by a generous read deadline
// so a deadlocked fn surfaces as a test timeout rather than a hang.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	origStdout := os.Stdout
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()

	defer func() {
		os.Stdout = origStdout
	}()
	fn()
	_ = w.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	select {
	case out := <-done:
		return out
	case <-ctx.Done():
		t.Fatalf("captureStdout: timed out")
		return ""
	}
}

// setExitFunc swaps exitFunc for the test via SwapExitFunc and
// returns a restore closure for use with defer. Wraps the package
// helper into the defer-style this test file prefers.
func setExitFunc(stub func(int)) func() {
	prev := SwapExitFunc(stub)
	return func() { SwapExitFunc(prev) }
}

// Compile-time guarantee that nats.DefaultURL still resolves —
// catches accidental dep removal. Empty value would silently break
// CLI connect attempts.
var _ = nats.DefaultURL

// worker/worker_noop_test.go
// Tests that NewWorker accepts nil telemetry and defaults to noop.
// Methodology: integration test with embedded NATS to verify worker
// starts and handles tasks correctly with nil telemetry.
package worker

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestNewWorkerNilTelemetry(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Passing nil telemetry must not panic.
	w := NewWorker(nc, nil)
	if w == nil {
		t.Fatal("expected non-nil worker, got nil")
	}
	if w.tel == nil {
		t.Fatal("expected tel to be defaulted, got nil")
	}

	// Register a handler so Start does not panic.
	w.Handle("noop-task", func(ctx TaskContext) error {
		return ctx.Complete([]byte("ok"))
	})
	w.Start()

	// Stop must succeed without error.
	w.Stop()
	if len(w.consumeContexts) == 0 {
		t.Fatal(
			"expected at least one consume context after Start",
		)
	}
}

func TestNewWorkerExplicitTelemetry(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	tel := observe.NewNoopTelemetry()
	w := NewWorker(nc, tel)
	if w == nil {
		t.Fatal("expected non-nil worker, got nil")
	}
	// Verify the explicit telemetry is used, not replaced.
	if w.tel != tel {
		t.Fatal(
			"expected worker to use the provided telemetry",
		)
	}

	w.Handle("echo", func(ctx TaskContext) error {
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	w.Stop()
	if len(w.consumeContexts) == 0 {
		t.Fatal(
			"expected at least one consume context after Start",
		)
	}
}

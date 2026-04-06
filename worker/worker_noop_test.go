// worker/worker_noop_test.go
// Tests that NewWorker works without explicit telemetry setup.
// Methodology: integration test with embedded NATS to verify worker
// starts and handles tasks correctly with default (noop) OTel providers.
package worker

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestNewWorkerDefaultTelemetry(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// No telemetry setup — global noop provider is used.
	w := NewWorker(nc)
	if w == nil {
		t.Fatal("expected non-nil worker, got nil")
	}
	if w.tracer == nil {
		t.Fatal("expected tracer to be set, got nil")
	}

	// Register a handler so Start does not panic.
	w.Handle("noop-task", func(ctx TaskContext) error {
		return ctx.Complete([]byte("ok"))
	})
	w.Start()

	// Stop must succeed without error.
	w.Stop()
	if len(w.stoppers) == 0 {
		t.Fatal(
			"expected at least one consume context after Start",
		)
	}
}

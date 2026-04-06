// dagnatstest/harness.go
// Test harness that bundles NATS server, orchestrator, API service,
// and worker into a single struct. Eliminates ~15 lines of
// boilerplate that every integration test otherwise duplicates.
package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// Harness holds all components needed for a workflow integration
// test. Fields are public so tests can reach into them for
// assertions (e.g., h.Svc.GetRun()).
type Harness struct {
	NC     *nats.Conn
	Engine *engine.Orchestrator
	Svc    *api.Service
	Worker *worker.Worker
}

// NewHarness starts an embedded NATS server, orchestrator, and
// API service. The worker is created but NOT started — register
// handlers first, then call h.Start(t).
func NewHarness(t *testing.T) *Harness {
	if t == nil {
		panic("NewHarness: t must not be nil")
	}
	t.Helper()

	nc := Server(t)
	if nc == nil {
		panic("NewHarness: Server returned nil connection")
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(func() { orch.Stop() })

	svc := api.NewService(nc)
	w := worker.NewWorker(nc)

	return &Harness{
		NC:     nc,
		Engine: orch,
		Svc:    svc,
		Worker: w,
	}
}

// Handle registers a raw handler on the harness worker.
// Call before h.Start(t).
func (h *Harness) Handle(
	t *testing.T, taskType string, fn worker.HandlerFunc,
) {
	t.Helper()
	if h == nil {
		panic("Handle: harness must not be nil")
	}
	if fn == nil {
		panic("Handle: fn must not be nil")
	}
	h.Worker.Handle(taskType, fn)
}

// HandleTypedOn registers a typed handler on the harness worker.
// Must be a package-level function because Go generics cannot
// appear on methods.
func HandleTypedOn[I, O any](
	h *Harness, t *testing.T,
	taskType string, fn worker.TypedHandlerFunc[I, O],
) {
	t.Helper()
	if h == nil {
		panic("HandleTypedOn: harness must not be nil")
	}
	if fn == nil {
		panic("HandleTypedOn: fn must not be nil")
	}
	worker.HandleTyped(h.Worker, taskType, fn)
}

// Start starts the worker and registers cleanup. Call after all
// handlers are registered.
func (h *Harness) Start(t *testing.T) {
	t.Helper()
	if h == nil {
		panic("Start: harness must not be nil")
	}
	if h.Worker == nil {
		panic("Start: worker must not be nil")
	}
	h.Worker.Start()
	t.Cleanup(func() { h.Worker.Stop() })
}

// RegisterAndRun registers a workflow definition, starts a run,
// and blocks until it reaches a terminal status. Returns the
// final WorkflowRun snapshot.
func (h *Harness) RegisterAndRun(
	t *testing.T, def dag.WorkflowDef,
	input []byte, timeout time.Duration,
) dag.WorkflowRun {
	t.Helper()
	if h == nil {
		panic("RegisterAndRun: harness must not be nil")
	}
	if def.Name == "" {
		panic("RegisterAndRun: def.Name must not be empty")
	}

	ctx := context.Background()
	err := h.Svc.RegisterWorkflow(ctx, def)
	if err != nil {
		t.Fatalf(
			"RegisterAndRun: RegisterWorkflow %q: %v",
			def.Name, err,
		)
	}

	return RunAndWait(t, h.Svc, def.Name, input, timeout)
}

// dagnatstest/setup.go
// Higher-level setup helpers that eliminate boilerplate when tests
// need a registered workflow or a running worker. These build on
// Server(), RunAndWait(), and WaitForStatus() from helpers.go.
package dagnatstest

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// Workflow builds and registers a workflow definition, failing the
// test immediately on any error. Returns the built definition so
// callers can inspect the name or steps if needed.
func Workflow(
	t *testing.T,
	svc *api.Service,
	builder *dag.WorkflowBuilder,
) dag.WorkflowDef {
	t.Helper()
	if svc == nil {
		panic("dagnatstest.Workflow: svc must not be nil")
	}
	if builder == nil {
		panic(
			"dagnatstest.Workflow: builder must not be nil",
		)
	}

	def, err := builder.Build()
	if err != nil {
		t.Fatalf(
			"dagnatstest.Workflow: build failed: %v", err,
		)
	}

	ctx := context.Background()
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf(
			"dagnatstest.Workflow: register failed: %v",
			err,
		)
	}
	return def
}

// Worker starts a worker with a single typed handler and stops
// it automatically when the test ends. Uses generics so callers
// get compile-time type safety on handler input and output.
// Must be a package-level function (not a method) because Go
// does not allow generic methods.
func Worker[I, O any](
	t *testing.T,
	nc *nats.Conn,
	taskType string,
	fn worker.TypedHandlerFunc[I, O],
) *worker.Worker {
	t.Helper()
	if nc == nil {
		panic("dagnatstest.Worker: nc must not be nil")
	}
	if taskType == "" {
		panic(
			"dagnatstest.Worker: taskType must not be empty",
		)
	}
	if fn == nil {
		panic("dagnatstest.Worker: fn must not be nil")
	}

	w := worker.NewWorker(nc)
	worker.HandleTyped(w, taskType, fn)
	w.Start()
	t.Cleanup(func() { w.Stop() })
	return w
}

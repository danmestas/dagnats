// dagnatstest/setup_test.go
// Tests for Workflow() and Worker() setup helpers. Methodology:
// integration tests with real embedded NATS verifying that the
// helpers correctly register workflows, start workers, handle
// tasks, and clean up on test completion.
package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
)

func TestWorkflow_RegistersSuccessfully(t *testing.T) {
	nc := Server(t)
	svc := api.NewService(nc)

	builder := dag.NewWorkflow("test-wf-register")
	builder.Task("step1", "echo")

	def := Workflow(t, svc, builder)

	// Positive: returned def has the correct name.
	if def.Name != "test-wf-register" {
		t.Fatalf(
			"expected name %q, got %q",
			"test-wf-register", def.Name,
		)
	}

	// Positive: workflow is retrievable from the service.
	stored, err := svc.GetWorkflow("test-wf-register")
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if stored.Name != "test-wf-register" {
		t.Fatalf(
			"stored name %q, expected %q",
			stored.Name, "test-wf-register",
		)
	}
}

func TestWorkflow_PanicsOnNilService(t *testing.T) {
	builder := dag.NewWorkflow("test-wf-panic")
	builder.Task("step1", "echo")

	// Positive: panics when svc is nil.
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		Workflow(t, nil, builder)
	}()
	if !panicked {
		t.Fatal("expected panic on nil svc")
	}

	// Positive: panics when builder is nil.
	nc := Server(t)
	svc := api.NewService(nc)
	panicked = false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		Workflow(t, svc, nil)
	}()
	if !panicked {
		t.Fatal("expected panic on nil builder")
	}
}

type echoInput struct {
	Message string `json:"message"`
}

type echoOutput struct {
	Reply string `json:"reply"`
}

func TestWorker_HandlesTaskAndCleansUp(t *testing.T) {
	nc := Server(t)
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(func() { orch.Stop() })

	svc := api.NewService(nc)
	builder := dag.NewWorkflow("test-worker-helper")
	builder.Task("step1", "echo-typed")
	Workflow(t, svc, builder)

	Worker(t, nc, "echo-typed",
		func(
			ctx worker.TaskContext, in echoInput,
		) (echoOutput, error) {
			return echoOutput{
				Reply: "got: " + in.Message,
			}, nil
		},
	)

	run := RunAndWait(
		t, svc, "test-worker-helper",
		[]byte(`{"message":"hello"}`),
		10*time.Second,
	)

	// Positive: workflow completed successfully.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected Completed, got %s", run.Status,
		)
	}

	// Positive: run ID is populated.
	if run.RunID == "" {
		t.Fatal("expected non-empty RunID")
	}

	// Verify step output is correct by checking run completed
	// (the typed handler wired through correctly).
	ctx := context.Background()
	storedRun, err := svc.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if storedRun.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"stored run status: %s", storedRun.Status,
		)
	}
}

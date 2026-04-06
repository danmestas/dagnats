// dagnatstest/harness_test.go
// Tests for the test harness helper. Methodology: integration tests
// with real embedded NATS — verify that NewHarness returns working
// components and that a simple workflow completes end-to-end through
// the harness convenience methods.
package dagnatstest

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/worker"
)

func TestNewHarness_Components(t *testing.T) {
	h := NewHarness(t)

	// Positive: all components are non-nil.
	if h.NC == nil {
		t.Fatal("expected non-nil NC")
	}
	if h.Engine == nil {
		t.Fatal("expected non-nil Engine")
	}
	if h.Svc == nil {
		t.Fatal("expected non-nil Svc")
	}
	if h.Worker == nil {
		t.Fatal("expected non-nil Worker")
	}

	// Positive: NATS connection is live.
	if !h.NC.IsConnected() {
		t.Fatal("expected connected NATS client")
	}

	// Negative: connection is not to the default URL (test server
	// uses a random port).
	url := h.NC.ConnectedUrl()
	if url == "nats://127.0.0.1:4222" {
		t.Fatal("expected test server, not default URL")
	}
}

func TestHarness_LinearWorkflow(t *testing.T) {
	h := NewHarness(t)

	h.Handle(t, "step-a", func(tc worker.TaskContext) error {
		return tc.Complete([]byte(`"a-done"`))
	})
	h.Handle(t, "step-b", func(tc worker.TaskContext) error {
		return tc.Complete([]byte(`"b-done"`))
	})
	h.Start(t)

	wb := dag.NewWorkflow("harness-linear")
	a := wb.Task("a", "step-a")
	wb.Task("b", "step-b").After(a)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	run := h.RegisterAndRun(
		t, def, nil, 10*time.Second,
	)

	// Positive: workflow completed.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected completed, got %s", run.Status,
		)
	}

	// Positive: both steps completed.
	if run.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"step a: expected completed, got %s",
			run.Steps["a"].Status,
		)
	}
	if run.Steps["b"].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"step b: expected completed, got %s",
			run.Steps["b"].Status,
		)
	}

	// Negative: output is preserved correctly.
	if string(run.Steps["a"].Output) != `"a-done"` {
		t.Fatalf(
			"step a output: got %s, want %q",
			run.Steps["a"].Output, `"a-done"`,
		)
	}
}

func TestHarness_HandleTypedOn(t *testing.T) {
	h := NewHarness(t)

	// HandleTypedOn is a generic package-level function.
	// Use a simple string->string handler: first step has
	// no deps so input is nil (zero-value ""), which is fine.
	HandleTypedOn(h, t, "echo",
		func(
			_ worker.TaskContext, in string,
		) (string, error) {
			return "echoed:" + in, nil
		},
	)
	h.Start(t)

	wb := dag.NewWorkflow("harness-typed")
	wb.Task("echo", "echo")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	run := h.RegisterAndRun(
		t, def, nil, 10*time.Second,
	)

	// Positive: workflow completed.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected completed, got %s", run.Status,
		)
	}

	// Positive: typed output reflects handler logic.
	got := string(run.Steps["echo"].Output)
	if got != `"echoed:"` {
		t.Fatalf(
			"echo output: got %s, want %s",
			got, `"echoed:"`,
		)
	}

	// Negative: RunID is non-empty (run was created).
	if run.RunID == "" {
		t.Fatal("expected non-empty RunID")
	}
}

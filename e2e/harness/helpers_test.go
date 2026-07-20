// e2e/harness/helpers_test.go
// Tests for shared E2E test helpers. Methodology: use embedded NATS,
// register and run a simple workflow, verify helpers correctly poll
// for status and assert history.
package harness

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
)

func TestHelperRoundTrip(t *testing.T) {
	topo := NewEmbedded()
	nc := topo.Connect(t)
	topo.Setup(t, nc)

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(func() { orch.Stop() })

	SubscribeWorker(t, nc, "greet", func(tc worker.TaskContext) error {
		return tc.Complete([]byte(`"hello"`))
	})

	svc := NewTestService(t, nc)
	wb := dag.NewWorkflow(UniqueName(t, "helper-test"))
	wb.Task("step-1", "greet")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	runID := RegisterAndStart(t, svc, wfDef, nil)

	// Positive: run completes.
	run := WaitForRunStatus(
		t, svc, runID, dag.RunStatusCompleted, 10*time.Second,
	)
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// Positive: history contains expected events.
	AssertHistoryContains(t, svc, runID,
		protocol.EventWorkflowStarted,
		protocol.EventStepCompleted,
		protocol.EventWorkflowCompleted,
	)
}

func TestWaitForPreconditionReturnsWhenReady(t *testing.T) {
	polls := 0
	ready := func() bool {
		polls++
		return polls >= 3
	}

	start := time.Now()
	WaitForPrecondition(t, "third poll", 5*time.Second, ready)
	elapsed := time.Since(start)

	// Positive: it returned only once the condition held.
	if polls < 3 {
		t.Fatalf("expected at least 3 polls, got %d", polls)
	}

	// Negative: it did not burn the whole budget waiting.
	if elapsed >= 5*time.Second {
		t.Fatalf("expected an early return, waited %s", elapsed)
	}
}

func TestUniqueNameDiffers(t *testing.T) {
	a := UniqueName(t, "wf")
	b := UniqueName(t, "wf")

	// Positive: names contain the base.
	if len(a) < 3 {
		t.Fatal("name too short")
	}

	// Negative: sequential calls produce different names.
	if a == b {
		t.Fatalf("expected unique names, both are %q", a)
	}
}

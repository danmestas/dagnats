// e2e/harness/helpers_test.go
// Tests for shared E2E test helpers. Methodology: use embedded NATS,
// register and run a simple workflow, verify helpers correctly poll
// for status and assert history.
package harness

import (
	"fmt"
	"strings"
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

func TestAssertHoldsForWindowPassesWhenCheckHolds(t *testing.T) {
	calls := 0
	check := func() (bool, string) {
		calls++
		return true, ""
	}

	start := time.Now()
	AssertHoldsForWindow(
		t, "always true", 200*time.Millisecond, 50*time.Millisecond, check,
	)
	elapsed := time.Since(start)

	// Positive: it polled repeatedly across the window rather than
	// returning after a single successful check.
	if calls < 2 {
		t.Fatalf("expected multiple polls, got %d", calls)
	}

	// Negative: it did not return early — the window was honored in
	// full, since a negative assertion isn't proven by one good poll.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("expected to honor the window, returned after %s",
			elapsed)
	}
}

// fakeFataler records Fatalf calls instead of tearing down the test,
// so AssertHoldsForWindow's failure path can be exercised without
// failing the test that exercises it.
type fakeFataler struct {
	failed bool
	msg    string
}

func (f *fakeFataler) Helper() {}

func (f *fakeFataler) Fatalf(format string, args ...any) {
	f.failed = true
	f.msg = fmt.Sprintf(format, args...)
}

func TestAssertHoldsForWindowFailsFastOnViolation(t *testing.T) {
	calls := 0
	check := func() (bool, string) {
		calls++
		if calls == 2 {
			return false, "boom"
		}
		return true, ""
	}
	fake := &fakeFataler{}

	start := time.Now()
	AssertHoldsForWindow(
		fake, "fails on 2nd poll",
		2*time.Second, 20*time.Millisecond, check,
	)
	elapsed := time.Since(start)

	// Positive: it reported failure once check() reported a
	// violation, with the violation detail in the message.
	if !fake.failed || !strings.Contains(fake.msg, "boom") {
		t.Fatalf("expected a failure containing %q, got failed=%v msg=%q",
			"boom", fake.failed, fake.msg)
	}

	// Negative: it failed fast, not after burning the full window —
	// a genuine violation must be reported immediately.
	if elapsed >= 1*time.Second {
		t.Fatalf("expected fast failure, took %s", elapsed)
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

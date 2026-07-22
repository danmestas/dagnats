// e2e/harness/helpers.go
// Shared test helpers for E2E tests. Eliminates boilerplate around
// starting orchestrators, registering workflows, polling for status,
// and asserting event history.
package harness

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

var nameCounter atomic.Int64

// UniqueName returns a test-unique name to prevent key collisions
// when tests share KV buckets across topologies.
func UniqueName(t *testing.T, base string) string {
	t.Helper()
	if base == "" {
		panic("UniqueName: base must not be empty")
	}
	n := nameCounter.Add(1)
	return fmt.Sprintf("%s-%d", base, n)
}

// NewTestService creates an api.Service with noop telemetry.
func NewTestService(t *testing.T, nc *nats.Conn) *api.Service {
	t.Helper()
	if nc == nil {
		panic("NewTestService: nc must not be nil")
	}
	return api.NewService(nc)
}

// RegisterAndStart registers a workflow and starts a run.
// Returns the run ID. Fails the test on error.
func RegisterAndStart(
	t *testing.T, svc *api.Service,
	wfDef dag.WorkflowDef, input []byte,
) string {
	t.Helper()
	if svc == nil {
		panic("RegisterAndStart: svc must not be nil")
	}
	if wfDef.Name == "" {
		panic("RegisterAndStart: workflow name must not be empty")
	}
	ctx := context.Background()
	if err := svc.RegisterWorkflow(ctx, wfDef); err != nil {
		t.Fatalf("RegisterWorkflow %q: %v", wfDef.Name, err)
	}
	runID, err := svc.StartRun(ctx, wfDef.Name, input)
	if err != nil {
		t.Fatalf("StartRun %q: %v", wfDef.Name, err)
	}
	return runID
}

// WaitForRunStatus polls until the run reaches the expected status.
// Uses 250ms poll interval. Fails the test on timeout.
func WaitForRunStatus(
	t *testing.T, svc *api.Service,
	runID string, status dag.RunStatus, timeout time.Duration,
) dag.WorkflowRun {
	t.Helper()
	if svc == nil {
		panic("WaitForRunStatus: svc must not be nil")
	}
	if runID == "" {
		panic("WaitForRunStatus: runID must not be empty")
	}
	ctx := context.Background()
	deadline := time.After(timeout)
	for {
		run, err := svc.GetRun(ctx, runID)
		if err == nil && run.Status == status {
			return run
		}
		select {
		case <-deadline:
			lastStatus := "unknown"
			if err == nil {
				lastStatus = run.Status.String()
			}
			t.Fatalf(
				"WaitForRunStatus: run %q did not reach %q "+
					"within %s (last: %s)",
				runID, status, timeout, lastStatus,
			)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// preconditionPollInterval bounds how often WaitForPrecondition
// re-checks. Short enough that the wait costs little once the setup
// lands, long enough not to spin a core on a loaded box.
const preconditionPollInterval = 25 * time.Millisecond

// WaitForPrecondition polls check until it reports ready, for tests
// that must perform an ACTION once some asynchronous setup is live —
// a KV watcher having registered a trigger, a worker having begun
// waiting. A fixed sleep there lands the action on a system that is
// not listening yet, and the miss then surfaces downstream as a wrong
// -looking symptom (a missing event, a 404) that reads as a product
// defect rather than a startup race (#558).
//
// Bounded on both axes: a wall-clock budget and a poll cap. On
// timeout it fails naming the precondition and saying explicitly that
// the setup, not the behavior under test, is what never arrived.
func WaitForPrecondition(
	t *testing.T, precondition string,
	timeout time.Duration, check func() bool,
) {
	t.Helper()
	if precondition == "" {
		panic("WaitForPrecondition: precondition must not be empty")
	}
	if check == nil {
		panic("WaitForPrecondition: check must not be nil")
	}
	if timeout <= preconditionPollInterval {
		panic("WaitForPrecondition: timeout must exceed poll interval")
	}
	pollsMax := int(timeout/preconditionPollInterval) + 2
	deadline := time.After(timeout)
poll:
	for polls := 0; polls < pollsMax; polls++ {
		if check() {
			return
		}
		select {
		case <-deadline:
			break poll
		case <-time.After(preconditionPollInterval):
		}
	}
	t.Fatalf(
		"WaitForPrecondition: %s never became ready within %s "+
			"(max %d polls) — the test setup did not complete, so no "+
			"action was taken; this is NOT a failure of the behavior "+
			"under test",
		precondition, timeout, pollsMax,
	)
}

// helperFataler is the subset of *testing.T that AssertHoldsForWindow
// needs. Declared explicitly (rather than taking *testing.T directly)
// so unit tests can substitute a fake that records failures instead
// of tearing down the real test — testing.TB can't be faked outside
// the testing package because it has an unexported method.
type helperFataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

// AssertHoldsForWindow polls check on every tick for the full window
// and fails the test the instant check reports a violation. It exists
// for negative assertions ("X never happened") where polling until
// check first succeeds would prove nothing — the first poll always
// succeeds trivially, so success only means something if it holds for
// the whole window, not just once (#562).
//
// check returns (true, "") while the expected state holds, or
// (false, detail) the moment a violation is observed; detail explains
// what went wrong. Bounded on both axes: a wall-clock budget (window)
// and a poll cap, so it never spins unboundedly.
func AssertHoldsForWindow(
	t helperFataler, description string,
	window, pollInterval time.Duration,
	check func() (bool, string),
) {
	t.Helper()
	if description == "" {
		panic("AssertHoldsForWindow: description must not be empty")
	}
	if check == nil {
		panic("AssertHoldsForWindow: check must not be nil")
	}
	if window <= pollInterval {
		panic("AssertHoldsForWindow: window must exceed poll interval")
	}
	pollsMax := int(window/pollInterval) + 2
	deadline := time.After(window)
	ticks := 0
poll:
	for polls := 0; polls < pollsMax; polls++ {
		ticks++
		ok, detail := check()
		if !ok {
			t.Fatalf(
				"AssertHoldsForWindow: %s violated after "+
					"%d poll(s): %s",
				description, polls+1, detail,
			)
			return // unreachable with a real *testing.T
		}
		select {
		case <-deadline:
			break poll
		case <-time.After(pollInterval):
		}
	}
	if ticks == 0 {
		panic("AssertHoldsForWindow: window elapsed without a poll")
	}
}

// SubscribeWorker creates a worker, registers the handler, starts it,
// and registers cleanup. Returns the worker for chaining.
func SubscribeWorker(
	t *testing.T, nc *nats.Conn,
	taskName string, handler worker.HandlerFunc,
) *worker.Worker {
	t.Helper()
	if nc == nil {
		panic("SubscribeWorker: nc must not be nil")
	}
	if taskName == "" {
		panic("SubscribeWorker: taskName must not be empty")
	}
	w := worker.NewWorker(nc)
	w.Handle(taskName, handler)
	w.Start()
	t.Cleanup(func() { w.Stop() })
	return w
}

// AssertHistoryContains verifies the run's history contains the
// expected event types in order (subsequence match, not exact).
func AssertHistoryContains(
	t *testing.T, svc *api.Service,
	runID string, expected ...protocol.EventType,
) {
	t.Helper()
	if svc == nil {
		panic("AssertHistoryContains: svc must not be nil")
	}
	if runID == "" {
		panic("AssertHistoryContains: runID must not be empty")
	}
	if len(expected) == 0 {
		panic("AssertHistoryContains: expected must not be empty")
	}
	ctx := context.Background()
	events, err := svc.ListRunEvents(ctx, runID, false)
	if err != nil {
		t.Fatalf("ListRunEvents %q: %v", runID, err)
	}
	ei := 0
	for _, evt := range events {
		if ei < len(expected) &&
			protocol.EventType(evt.Type) == expected[ei] {
			ei++
		}
	}
	if ei < len(expected) {
		t.Fatalf(
			"AssertHistoryContains: run %q missing event %q "+
				"(matched %d/%d expected events)",
			runID, expected[ei], ei, len(expected),
		)
	}
}

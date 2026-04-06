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
	"github.com/danmestas/dagnats/observe"
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
	tel := observe.NewNoopTelemetry()
	w := worker.NewWorker(nc, tel)
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

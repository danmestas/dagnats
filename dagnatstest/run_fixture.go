// dagnatstest/run_fixture.go
// RunFixture clusters run-population helpers around a shared
// Harness. Keeping these off Harness lets the harness stay a thin
// lifecycle owner; per-concern helpers cluster by concern, not by
// accretion (the inventory pattern documented in the AFK plan).
package dagnatstest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// RunFixture wraps a Harness with helpers that submit and advance
// workflow runs into terminal states. The fixture is concern-scoped
// (run shape) rather than infrastructure-scoped (NATS lifecycle).
type RunFixture struct {
	h *Harness

	// nameSeq disambiguates workflow names across calls inside a
	// single test, so SubmitAndAdvanceTo can be called many times.
	nameSeq atomic.Int64

	// started records whether the embedded worker has been started.
	// Worker.Start subscribes; calling twice would double-subscribe.
	started bool

	// registered tracks which (taskType, target) handlers we have
	// already installed on the worker. Worker.Handle is map-set so
	// re-installs are idempotent, but tracking lets us decide when
	// to defer start until after both task types are wired.
	registered map[string]bool
}

// NewRunFixture constructs a RunFixture bound to h. Panics on nil
// harness — programmer error.
func NewRunFixture(h *Harness) *RunFixture {
	if h == nil {
		panic("NewRunFixture: harness must not be nil")
	}
	if h.Svc == nil {
		panic("NewRunFixture: harness.Svc must not be nil")
	}
	if h.Worker == nil {
		panic("NewRunFixture: harness.Worker must not be nil")
	}
	return &RunFixture{h: h, registered: make(map[string]bool)}
}

// SubmitAndAdvanceTo registers and runs n single-step workflows
// whose handlers drive each run to the requested terminal state
// ("completed" or "failed"). Returns once every run has reached
// state or the per-run timeout fires.
func (f *RunFixture) SubmitAndAdvanceTo(
	t *testing.T, state string, n int,
) {
	t.Helper()
	if f == nil {
		panic("SubmitAndAdvanceTo: fixture must not be nil")
	}
	if n <= 0 {
		panic("SubmitAndAdvanceTo: n must be positive")
	}
	if n > 50 {
		panic("SubmitAndAdvanceTo: n exceeds upper bound")
	}
	target, err := dag.ParseRunStatus(state)
	if err != nil {
		t.Fatalf("SubmitAndAdvanceTo: bad state %q: %v",
			state, err)
	}
	if !target.IsTerminal() {
		t.Fatalf("SubmitAndAdvanceTo: %v is not terminal",
			target)
	}

	// Install handlers for every state we know how to drive,
	// then start the worker once. Subsequent calls to a different
	// state reuse already-subscribed task types.
	f.installAllHandlers(t)
	taskType := "runfix-task-" + target.String()
	f.ensureWorkerStarted(t)
	for i := 0; i < n; i++ {
		seq := f.nameSeq.Add(1)
		wfName := fmt.Sprintf(
			"runfix-%s-%d-%d",
			target.String(), time.Now().UnixNano(), seq,
		)
		f.runOne(t, wfName, taskType, target)
	}
}

// installAllHandlers installs handlers for every supported terminal
// state exactly once. We install all handlers up front because
// Worker.Start subscribes the currently-registered task set; adding
// a handler after Start has no effect.
func (f *RunFixture) installAllHandlers(t *testing.T) {
	t.Helper()
	if f.registered["installed"] {
		return
	}
	f.h.Handle(t, "runfix-task-completed", PassHandler())
	f.h.Handle(t, "runfix-task-failed",
		FailHandler("runfix forced failure"))
	f.registered["installed"] = true
}

// ensureWorkerStarted starts the worker on first use. Worker.Start
// subscribes; calling twice would double-subscribe, so we gate.
func (f *RunFixture) ensureWorkerStarted(t *testing.T) {
	t.Helper()
	if f.started {
		return
	}
	f.h.Start(t)
	f.started = true
}

// runOne registers a single-step workflow named wfName and waits
// for it to reach target.
func (f *RunFixture) runOne(
	t *testing.T, wfName, taskType string,
	target dag.RunStatus,
) {
	t.Helper()
	wb := dag.NewWorkflow(wfName)
	wb.Task("only", taskType)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("runOne: build %q: %v", wfName, err)
	}
	ctx := context.Background()
	if err := f.h.Svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("runOne: register %q: %v", wfName, err)
	}
	got := RunAndWait(
		t, f.h.Svc, wfName, nil, 10*time.Second,
	)
	if got.Status != target {
		t.Fatalf("runOne: %q reached %v, want %v",
			wfName, got.Status, target)
	}
}

// RunSingleStepToCompletion drives one fresh single-step workflow
// through the worker harness and waits for it to reach Completed.
// Returns the run ID and step ID of the now-terminal step. Callers
// rely on this to seed the "step is terminal" precondition before
// synthesizing a stale step.started in idempotency tests.
func (f *RunFixture) RunSingleStepToCompletion(
	t *testing.T,
) (string, string) {
	t.Helper()
	if f == nil {
		panic("RunSingleStepToCompletion: fixture must not be nil")
	}
	const stepID = "only"
	f.installAllHandlers(t)
	f.ensureWorkerStarted(t)

	seq := f.nameSeq.Add(1)
	wfName := fmt.Sprintf(
		"runfix-single-%d-%d", time.Now().UnixNano(), seq,
	)
	wb := dag.NewWorkflow(wfName)
	wb.Task(stepID, "runfix-task-completed")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("RunSingleStepToCompletion: build %q: %v",
			wfName, err)
	}
	ctx := context.Background()
	if err := f.h.Svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RunSingleStepToCompletion: register %q: %v",
			wfName, err)
	}
	runID, err := f.h.Svc.StartRun(ctx, wfName, nil)
	if err != nil {
		t.Fatalf("RunSingleStepToCompletion: StartRun %q: %v",
			wfName, err)
	}
	got := WaitForStatus(
		t, f.h.Svc, runID, 10*time.Second,
		dag.RunStatusCompleted,
	)
	if got.Steps[stepID].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"RunSingleStepToCompletion: step %q status = %v, "+
				"want Completed",
			stepID, got.Steps[stepID].Status,
		)
	}
	return runID, stepID
}

// Snapshot returns the current KV snapshot for runID. Fatals the
// test on load failure — callers use this to assert state didn't
// change across an event publish, so a load error is unrecoverable.
func (f *RunFixture) Snapshot(
	t *testing.T, runID string,
) dag.WorkflowRun {
	t.Helper()
	if f == nil {
		panic("Snapshot: fixture must not be nil")
	}
	if runID == "" {
		panic("Snapshot: runID must not be empty")
	}
	run, err := f.h.Svc.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("Snapshot: GetRun %q: %v", runID, err)
	}
	return run
}

// PublishStaleStepStarted synthesizes a step.started event for
// runID/stepID at the given attempt number and publishes it to the
// WORKFLOW_HISTORY stream so the engine's history consumer receives
// it. The event carries the same shape a real worker would publish.
//
// We deliberately do NOT set Nats-Msg-Id to the deterministic
// NATSMsgID(). The production failure mode this regression test
// guards against is delivery of an event the engine has already
// terminalised — that delivery can arrive via either consumer-side
// redelivery (same stream message, replayed past ack) or a fresh
// publish from a recovered worker that the stream's dedup window
// has already aged out. Both produce identical engine input. To
// exercise the engine path without coupling the test to either
// JetStream's consumer redelivery internals or the 5s dedup window
// on WORKFLOW_HISTORY, we publish with a unique tag in the MsgId so
// the stream accepts the message and the engine sees the duplicate
// payload. The engine's terminal-state guard in handleStepStarted
// is the contract under test, not the dedup window.
//
// attempt must be > 0; attempt == 0 is reserved for "not yet
// scheduled" elsewhere in the engine.
func (f *RunFixture) PublishStaleStepStarted(
	ctx context.Context, t *testing.T,
	runID, stepID string, attempt int,
) {
	t.Helper()
	if f == nil {
		panic("PublishStaleStepStarted: fixture must not be nil")
	}
	if runID == "" {
		panic("PublishStaleStepStarted: runID must not be empty")
	}
	if stepID == "" {
		panic("PublishStaleStepStarted: stepID must not be empty")
	}
	if attempt <= 0 {
		panic("PublishStaleStepStarted: attempt must be > 0")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepStarted, runID, stepID, nil,
	)
	evt.AttemptNumber = attempt
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("PublishStaleStepStarted: marshal: %v", err)
	}
	js, err := f.h.NC.JetStream()
	if err != nil {
		t.Fatalf("PublishStaleStepStarted: JetStream: %v", err)
	}
	tag := fmt.Sprintf(".stale-%d", time.Now().UnixNano())
	_, err = js.Publish(
		evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()+tag),
		nats.Context(ctx),
	)
	if err != nil {
		t.Fatalf("PublishStaleStepStarted: publish: %v", err)
	}
}

// engine/cancel_before_start_test.go
// Methodology: a run the API acknowledged but the orchestrator has not
// handled yet (#1006), against a real embedded NATS server. The
// orchestrator is built but its history consumer is never started:
// each test publishes the run's events to WORKFLOW_HISTORY, exactly as
// the API does, then hands them to dispatchEvent in the order under
// test, so "cancel handled before start" and "start handled with a
// cancel already behind it" are deterministic. "No task dispatched" is
// read from TASK_QUEUES' message count, with a positive control showing
// a plain start does put a task there.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type unstartedFixture struct {
	orch  *Orchestrator
	js    nats.JetStreamContext
	jsNew jetstream.JetStream
	def   dag.WorkflowDef
}

func newUnstartedFixture(t *testing.T) unstartedFixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	def := dag.WorkflowDef{
		Name: "unstarted-wf", Version: "1",
		Steps: []dag.StepDef{{ID: "a", Task: "unstarted-task", Type: dag.StepTypeNormal}},
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue workflow_defs: %v", err)
	}
	mustPut(t, defKV, def.Name, mustMarshal(t, def))
	mustPut(t, defKV, dag.DefVersionKey(def.Name, dag.DefHash(def)), mustMarshal(t, def))
	return unstartedFixture{orch: NewOrchestrator(nc), js: js, jsNew: jsNew, def: def}
}

// startEvent is the workflow.started the API publishes: the def inline,
// with input and labels beside it.
func (f unstartedFixture) startEvent(t *testing.T, runID string) protocol.Event {
	t.Helper()
	payload := mustMarshal(t, map[string]any{
		"workflow_def": f.def,
		"input":        json.RawMessage(`{"to":"a@example.com"}`),
		"labels":       map[string]string{"tenant": "t1"},
	})
	return protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, runID, payload)
}

func cancelEvent(runID string) protocol.Event {
	return protocol.NewWorkflowEvent(protocol.EventWorkflowCancelled, runID, nil)
}

func (f unstartedFixture) dispatch(t *testing.T, evt protocol.Event) {
	t.Helper()
	if err := f.orch.dispatchEvent(context.Background(), evt); err != nil {
		t.Fatalf("dispatch %s: %v", evt.Type, err)
	}
}

func (f unstartedFixture) taskCount(t *testing.T) uint64 {
	t.Helper()
	stream, err := f.jsNew.Stream(context.Background(), "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream TASK_QUEUES: %v", err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatalf("TASK_QUEUES info: %v", err)
	}
	return info.State.Msgs
}

func (f unstartedFixture) assertCancelled(t *testing.T, runID string) {
	t.Helper()
	run, err := f.orch.store.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("Load %s: %v", runID, err)
	}
	if run.Status != dag.RunStatusCancelled || run.CompletedAt == nil {
		t.Fatalf("run %s status = %s completed=%v, want cancelled",
			runID, run.Status, run.CompletedAt)
	}
	if run.WorkflowID != f.def.Name || run.Steps["a"].Status != dag.StepStatusCancelled {
		t.Fatalf("run %s = workflow %q step a %s, want %s with a cancelled",
			runID, run.WorkflowID, run.Steps["a"].Status, f.def.Name)
	}
	if got := f.taskCount(t); got != 0 {
		t.Fatalf("TASK_QUEUES holds %d messages, want 0: a cancelled run dispatched", got)
	}
}

// A cancel handled before its run's workflow.started (a redelivery, or
// another engine taking the cancel first) cancels the run from the
// start event, and the start that follows does not dispatch it.
func TestCancelHandledBeforeStartSticks(t *testing.T) {
	f := newUnstartedFixture(t)
	const runID = "cancel-first-run"
	publishEvent(t, f.js, f.startEvent(t, runID))
	publishEvent(t, f.js, cancelEvent(runID))

	f.dispatch(t, cancelEvent(runID))
	f.assertCancelled(t, runID)
	f.dispatch(t, f.startEvent(t, runID))
	f.assertCancelled(t, runID)
	assertNoDeadLetters(t, f.jsNew)
}

// A start handled while a cancel already waits behind it in history
// never dispatches: the run is finalized cancelled straight away.
func TestStartHonoursCancelAlreadyInHistory(t *testing.T) {
	f := newUnstartedFixture(t)
	const runID = "cancel-queued-run"
	publishEvent(t, f.js, f.startEvent(t, runID))
	publishEvent(t, f.js, cancelEvent(runID))

	f.dispatch(t, f.startEvent(t, runID))
	f.assertCancelled(t, runID)
	f.dispatch(t, cancelEvent(runID))
	f.assertCancelled(t, runID)

	// Positive control: without a cancel the same start dispatches.
	const liveID = "live-run"
	publishEvent(t, f.js, f.startEvent(t, liveID))
	f.dispatch(t, f.startEvent(t, liveID))
	run, err := f.orch.store.Load(context.Background(), liveID)
	if err != nil || run.Status != dag.RunStatusRunning {
		t.Fatalf("live run = %s err=%v, want running", run.Status, err)
	}
	if got := f.taskCount(t); got != 1 {
		t.Fatalf("TASK_QUEUES holds %d messages, want the live run's 1", got)
	}
}

// A cancel for a run nobody started, or for one whose snapshot was
// deleted (pruned), is dropped: no error to redeliver into
// DEAD_LETTERS, and no snapshot written.
func TestCancelForUnknownOrDeletedRunIsDropped(t *testing.T) {
	f := newUnstartedFixture(t)
	ctx := context.Background()
	f.dispatch(t, cancelEvent("never-started"))
	if _, err := f.orch.store.Load(ctx, "never-started"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("unknown run after cancel: err=%v, want ErrRunNotFound", err)
	}

	const prunedID = "pruned-run"
	publishEvent(t, f.js, f.startEvent(t, prunedID))
	f.dispatch(t, f.startEvent(t, prunedID))
	if err := f.orch.store.Delete(ctx, prunedID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	f.dispatch(t, cancelEvent(prunedID))
	if _, err := f.orch.store.Load(ctx, prunedID); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("pruned run after cancel: err=%v, want ErrRunNotFound", err)
	}
	assertNoDeadLetters(t, f.jsNew)
}

// LoadAccepted answers a run known only by its workflow.started as
// pending, and everything else exactly as Load does.
func TestLoadAccepted(t *testing.T) {
	f := newUnstartedFixture(t)
	ctx := context.Background()
	const runID = "accepted-run"
	publishEvent(t, f.js, f.startEvent(t, runID))

	run, err := f.orch.store.LoadAccepted(ctx, runID)
	if err != nil {
		t.Fatalf("LoadAccepted: %v", err)
	}
	if run.Status != dag.RunStatusPending || run.RunID != runID ||
		run.WorkflowID != f.def.Name || run.Revision != nil {
		t.Fatalf("accepted run = %+v, want pending %s of %s without a revision",
			run, runID, f.def.Name)
	}
	if run.Steps["a"].Status != dag.StepStatusPending || run.Labels["tenant"] != "t1" ||
		string(run.Input) != `{"to":"a@example.com"}` {
		t.Fatalf("accepted run steps/labels/input = %+v %v %s", run.Steps, run.Labels, run.Input)
	}

	f.dispatch(t, f.startEvent(t, runID))
	saved, err := f.orch.store.LoadAccepted(ctx, runID)
	if err != nil || saved.Status != dag.RunStatusRunning || saved.Revision == nil {
		t.Fatalf("saved run = %s rev=%v err=%v, want the running snapshot",
			saved.Status, saved.Revision, err)
	}
	if err := f.orch.store.Delete(ctx, runID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, id := range []string{runID, "never-started", "*", ">", "a..b"} {
		if _, err := f.orch.store.LoadAccepted(ctx, id); !errors.Is(err, ErrRunNotFound) {
			t.Errorf("LoadAccepted(%q): err=%v, want ErrRunNotFound", id, err)
		}
	}
}

// A start that loses the creating write to a cancel (another engine
// finalized the run between this start's existence check and its save)
// leaves the cancelled run alone and dispatches nothing.
func TestStartLosingCreateToCancelDoesNotDispatch(t *testing.T) {
	f := newUnstartedFixture(t)
	ctx := context.Background()
	const runID = "lost-race-run"
	cancelled := dag.NewWorkflowRun(f.def, runID)
	cancelUnfinishedSteps(cancelled)
	cancelled = markTerminal(cancelled, dag.RunStatusCancelled)
	if created, err := f.orch.store.CreateSnapshot(ctx, cancelled); err != nil || !created {
		t.Fatalf("CreateSnapshot: created=%v err=%v", created, err)
	}

	run := dag.NewWorkflowRun(f.def, runID)
	run.Status = dag.RunStatusRunning
	if err := f.orch.createOrHealRun(ctx, false, f.def, run, true); err != nil {
		t.Fatalf("createOrHealRun: %v", err)
	}
	f.assertCancelled(t, runID)
}

// A cancel that finds the run saved after all (its start was handled
// between the cancel's Load and its write) does not overwrite the
// snapshot: it reports a conflict, so the normal path cancels the run
// from a fresh Load.
func TestCancelBeforeStartYieldsToASavedRun(t *testing.T) {
	f := newUnstartedFixture(t)
	ctx := context.Background()
	const runID = "saved-meanwhile-run"
	start := f.startEvent(t, runID)
	publishEvent(t, f.js, start)
	f.dispatch(t, start)

	if err := f.orch.cancelBeforeStart(ctx, runID); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("cancelBeforeStart on a saved run: err=%v, want ErrSnapshotConflict", err)
	}
	pending, ok := pendingRunFromStart(start)
	if !ok {
		t.Fatal("pendingRunFromStart refused the API's start payload")
	}
	err := f.orch.persistCancelledBeforeStart(ctx, pending)
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("persistCancelledBeforeStart on a saved run: err=%v, want ErrSnapshotConflict", err)
	}
	run, err := f.orch.store.Load(ctx, runID)
	if err != nil || run.Status != dag.RunStatusRunning {
		t.Fatalf("saved run = %s err=%v, want still running", run.Status, err)
	}
}

// A start event that names no workflow describes no run.
func TestLoadAcceptedNeedsAWorkflow(t *testing.T) {
	f := newUnstartedFixture(t)
	const runID = "nameless-run"
	publishEvent(t, f.js, protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, []byte(`{"input":{}}`)))
	_, err := f.orch.store.LoadAccepted(context.Background(), runID)
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("nameless start: err=%v, want ErrRunNotFound", err)
	}
	const strayID = "stray-run"
	stray := `{"type":"workflow.started","run_id":"","payload":{"workflow_id":"unstarted-wf"}}`
	if _, err := f.js.Publish("history."+strayID, []byte(stray)); err != nil {
		t.Fatalf("publish stray start: %v", err)
	}
	_, err = f.orch.store.LoadAccepted(context.Background(), strayID)
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("start whose body names no run: err=%v, want ErrRunNotFound", err)
	}
	const namedID = "named-run"
	publishEvent(t, f.js, protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, namedID,
		[]byte(`{"trigger":"cron","workflow_id":"unstarted-wf"}`)))
	run, err := f.orch.store.LoadAccepted(context.Background(), namedID)
	if err != nil || run.WorkflowID != "unstarted-wf" || run.Status != dag.RunStatusPending {
		t.Fatalf("trigger-envelope start = %q %s err=%v, want pending unstarted-wf",
			run.WorkflowID, run.Status, err)
	}
}

// historySubject refuses a run id that would match more than one run's
// subject, so a probe like "*" cannot make readers scan other runs.
func TestHistorySubjectNamesOneRun(t *testing.T) {
	if subject, ok := historySubject("abc123"); !ok || subject != "history.abc123" {
		t.Fatalf("historySubject(abc123) = %q, %v", subject, ok)
	}
	for _, runID := range []string{"*", ">", "a.*", "a..b", ".a", "a b"} {
		if _, ok := historySubject(runID); ok {
			t.Errorf("historySubject(%q) accepted a run id that is not one subject", runID)
		}
	}
}

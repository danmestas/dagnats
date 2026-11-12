// engine/cancel_admission_test.go
// Methodology: cancels that meet admission state (#1006 review), over a
// real embedded NATS server with the concurrency_runs bucket, so the
// workflow's run-slot member set is the real KV record. The
// orchestrator's history consumer is never started; events are
// published to WORKFLOW_HISTORY and handed to dispatchEvent in the
// order under test, and "crashed after Admit" is Admit called by hand
// with no snapshot written. Each test asserts the slot set and
// TASK_QUEUES before and after, so a leaked slot or a dispatch shows.
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

func newLimitedFixture(t *testing.T) unstartedFixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	err := natsutil.SetupAll(nc, natsutil.WithKVBuckets(
		natsutil.KVConfig{Bucket: "concurrency_runs"},
	))
	if err != nil {
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
		Name: "limited-wf", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps:       []dag.StepDef{{ID: "a", Task: "limited-task", Type: dag.StepTypeNormal}},
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue workflow_defs: %v", err)
	}
	mustPut(t, defKV, def.Name, mustMarshal(t, def))
	mustPut(t, defKV, dag.DefVersionKey(def.Name, dag.DefHash(def)), mustMarshal(t, def))
	f := unstartedFixture{orch: NewOrchestrator(nc), js: js, jsNew: jsNew, def: def}
	if f.orch.admission.concurrency == nil {
		t.Fatal("the orchestrator found no concurrency_runs bucket")
	}
	return f
}

func (f unstartedFixture) slotHolders(t *testing.T) []string {
	t.Helper()
	cm := f.orch.admission.concurrency
	members, _, err := cm.readMembers(context.Background(), cm.runKV, "workflow."+f.def.Name)
	if err != nil {
		t.Fatalf("readMembers: %v", err)
	}
	return members
}

func (f unstartedFixture) runStatus(t *testing.T, runID string) dag.RunStatus {
	t.Helper()
	run, err := f.orch.store.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("Load %s: %v", runID, err)
	}
	return run.Status
}

// A start that claimed its run slot and crashed before its first save,
// then a cancel handled first: the slot is given back, so the next run
// is admitted. Covers the cancel path and startExistingRun's release.
func TestCancelBeforeStartReleasesACrashedStartsSlot(t *testing.T) {
	f := newLimitedFixture(t)
	ctx := context.Background()
	const runID = "crashed-start-run"
	start := f.startEvent(t, runID)
	publishEvent(t, f.js, start)
	run := dag.NewWorkflowRun(f.def, runID)
	if _, err := f.orch.admission.Admit(ctx, f.def, run, nil); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if got := f.slotHolders(t); len(got) != 1 || got[0] != runID {
		t.Fatalf("slot holders after Admit = %v, want [%s]", got, runID)
	}

	f.dispatch(t, cancelEvent(runID))
	if got := f.slotHolders(t); len(got) != 0 {
		t.Fatalf("slot holders after the cancel = %v, want none", got)
	}
	f.dispatch(t, start)
	if got := f.slotHolders(t); len(got) != 0 {
		t.Fatalf("slot holders after cancel and redelivered start = %v, want none", got)
	}
	const nextID = "next-run"
	publishEvent(t, f.js, f.startEvent(t, nextID))
	f.dispatch(t, f.startEvent(t, nextID))
	if got := f.runStatus(t, nextID); got != dag.RunStatusRunning {
		t.Fatalf("next run = %s, want running: the slot leaked", got)
	}
}

// A redelivered start that finds its run already terminal gives back a
// slot the run still holds, whichever order put it there.
func TestStartExistingRunReleasesATerminalRunsSlot(t *testing.T) {
	f := newLimitedFixture(t)
	ctx := context.Background()
	const runID = "terminal-holder-run"
	start := f.startEvent(t, runID)
	publishEvent(t, f.js, start)
	publishEvent(t, f.js, cancelEvent(runID))
	f.dispatch(t, start) // cancelled from history, never admitted
	run := dag.NewWorkflowRun(f.def, runID)
	if _, err := f.orch.admission.Admit(ctx, f.def, run, nil); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	f.dispatch(t, start)
	if got := f.slotHolders(t); len(got) != 0 {
		t.Fatalf("slot holders after a redelivered start = %v, want none", got)
	}
}

// A start that failed before naming a workflow claimed no slot: its
// redelivery finds a terminal run with no workflow id and is acked.
func TestRedeliveredNamelessFailedStartIsAcked(t *testing.T) {
	f := newUnstartedFixture(t)
	const runID = "nameless-failed-run"
	nameless := f.def
	nameless.Name = ""
	start := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, runID,
		mustMarshal(t, map[string]any{"workflow_def": nameless}))
	publishEvent(t, f.js, start)
	f.dispatch(t, start)
	run, err := f.orch.store.Load(context.Background(), runID)
	if err != nil || run.Status != dag.RunStatusFailed || run.WorkflowID != "" {
		t.Fatalf("nameless start = %s %q err=%v, want a failed run with no workflow",
			run.Status, run.WorkflowID, err)
	}
	f.dispatch(t, start)
	assertNoDeadLetters(t, f.jsNew)
}

// A cancel before start whose slot release fails records the release
// as owed on the snapshot it just created (a revision-checked Save, not
// a second create), for the reconciler to retry.
func TestCancelBeforeStartRecordsAFailedRelease(t *testing.T) {
	f := newLimitedFixture(t)
	ctx := context.Background()
	const runID = "release-fails-run"
	publishEvent(t, f.js, f.startEvent(t, runID))
	if err := f.jsNew.DeleteKeyValue(ctx, "concurrency_runs"); err != nil {
		t.Fatalf("DeleteKeyValue: %v", err)
	}
	f.dispatch(t, cancelEvent(runID))
	run, err := f.orch.store.Load(ctx, runID)
	if err != nil || run.Status != dag.RunStatusCancelled || !run.ReleasePending {
		t.Fatalf("run = %s release_pending=%v err=%v, want cancelled with the release owed",
			run.Status, run.ReleasePending, err)
	}
}

// A cancel of a run queued behind the concurrency limit finalizes it,
// and it never starts when the slot frees.
func TestCancelQueuedRunNeverStarts(t *testing.T) {
	f := newLimitedFixture(t)
	for _, id := range []string{"holder-run", "queued-run"} {
		publishEvent(t, f.js, f.startEvent(t, id))
		f.dispatch(t, f.startEvent(t, id))
	}
	if got := f.runStatus(t, "queued-run"); got != dag.RunStatusPending {
		t.Fatalf("second run = %s, want pending behind the limit", got)
	}
	publishEvent(t, f.js, cancelEvent("queued-run"))
	f.dispatch(t, cancelEvent("queued-run"))
	if got := f.runStatus(t, "queued-run"); got != dag.RunStatusCancelled {
		t.Fatalf("cancelled queued run = %s, want cancelled", got)
	}

	publishEvent(t, f.js, cancelEvent("holder-run"))
	f.dispatch(t, cancelEvent("holder-run"))
	if got := f.runStatus(t, "queued-run"); got != dag.RunStatusCancelled {
		t.Fatalf("queued run after the slot freed = %s, want still cancelled", got)
	}
	if got := f.taskCount(t); got != 1 {
		t.Fatalf("TASK_QUEUES holds %d messages, want only the holder's 1", got)
	}
	if got := f.slotHolders(t); len(got) != 0 {
		t.Fatalf("slot holders = %v, want none", got)
	}
}

// Two starts for one run id that both pass the existence check: the
// second loses the create, is handled again, and skips, so the run
// dispatches once and nothing is dead-lettered.
func TestDuplicateStartDispatchesOnce(t *testing.T) {
	f := newUnstartedFixture(t)
	ctx := context.Background()
	const runID = "duplicate-start-run"
	start := f.startEvent(t, runID)
	publishEvent(t, f.js, start)
	f.dispatch(t, start)

	second := dag.NewWorkflowRun(f.def, runID)
	second.Status = dag.RunStatusRunning
	err := f.orch.createOrHealRun(ctx, false, f.def, second, true)
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("losing create: err=%v, want ErrSnapshotConflict", err)
	}
	if err := f.orch.handleEventWithConflictRetry(ctx, start); err != nil {
		t.Fatalf("re-handled duplicate start: %v", err)
	}
	if got := f.taskCount(t); got != 1 {
		t.Fatalf("TASK_QUEUES holds %d messages, want 1", got)
	}
	if got := f.runStatus(t, runID); got != dag.RunStatusRunning {
		t.Fatalf("run = %s, want running", got)
	}
	assertNoDeadLetters(t, f.jsNew)
}

// A run id started twice (a republish outside the dedup window) is
// described by its latest start: its input, and the input a cancel
// before start records.
func TestAcceptedRunUsesTheLatestStart(t *testing.T) {
	f := newUnstartedFixture(t)
	ctx := context.Background()
	const runID = "restarted-run"
	for _, to := range []string{"old@example.com", "new@example.com"} {
		evt := f.startEvent(t, runID)
		evt.Payload = mustMarshal(t, map[string]any{
			"workflow_def": f.def,
			"input":        json.RawMessage(`{"to":"` + to + `"}`),
		})
		publishWithoutDedup(t, f.js, evt)
	}
	run, err := f.orch.store.LoadAccepted(ctx, runID)
	if err != nil || string(run.Input) != `{"to":"new@example.com"}` {
		t.Fatalf("accepted input = %s err=%v, want the latest start's", run.Input, err)
	}
	publishEvent(t, f.js, cancelEvent(runID))
	f.dispatch(t, cancelEvent(runID))
	saved, err := f.orch.store.Load(ctx, runID)
	if err != nil || saved.Status != dag.RunStatusCancelled ||
		string(saved.Input) != `{"to":"new@example.com"}` {
		t.Fatalf("cancelled run = %s input %s err=%v, want cancelled with the latest input",
			saved.Status, saved.Input, err)
	}
}

func publishWithoutDedup(t *testing.T, js nats.JetStreamContext, evt protocol.Event) {
	t.Helper()
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if _, err := js.Publish(evt.NATSSubject(), data); err != nil {
		t.Fatalf("publish event: %v", err)
	}
}

// A run id holding a NATS wildcard never reads another run's snapshot:
// "run.*" as a last_by_subj subject would match every run's key.
func TestLoadRefusesWildcardRunIDs(t *testing.T) {
	f := newUnstartedFixture(t)
	ctx := context.Background()
	const runID = "stored-run"
	publishEvent(t, f.js, f.startEvent(t, runID))
	f.dispatch(t, f.startEvent(t, runID))
	if _, err := f.orch.store.Load(ctx, runID); err != nil {
		t.Fatalf("Load(%s): %v", runID, err)
	}
	for _, id := range []string{"*", ">", "stored-*"} {
		if run, err := f.orch.store.Load(ctx, id); !errors.Is(err, ErrRunNotFound) {
			t.Errorf("Load(%q) = run %q err=%v, want ErrRunNotFound", id, run.RunID, err)
		}
		if run, err := f.orch.store.LoadAccepted(ctx, id); !errors.Is(err, ErrRunNotFound) {
			t.Errorf("LoadAccepted(%q) = run %q err=%v, want ErrRunNotFound", id, run.RunID, err)
		}
	}
}

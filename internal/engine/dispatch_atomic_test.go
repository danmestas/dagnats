// engine/dispatch_atomic_test.go
// Methodology: real embedded NATS server, orchestrator built but not
// started (no consumer races the direct handler call). A wrapped
// workflow_runs KV forces exactly one snapshot conflict -- a rival
// write lands immediately before the first save that records the sleep
// step as running, i.e. a LATER save of a fan-out step.completed event.
// The event is then handled through the same conflict-retry entry point
// the history consumer uses. Asserts every ready step was dispatched
// (sleep running with a timer scheduled, both tasks published) and that
// the conflict really fired.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// conflictOnceKV injects one rival write right before the first Update
// whose payload satisfies trigger, so that Update fails its revision
// check exactly once.
type conflictOnceKV struct {
	jetstream.KeyValue
	trigger  func(dag.WorkflowRun) bool
	mu       sync.Mutex
	injected int
	err      error
}

func (kv *conflictOnceKV) Update(
	ctx context.Context, key string, value []byte, revision uint64,
) (uint64, error) {
	kv.mu.Lock()
	var run dag.WorkflowRun
	if kv.injected == 0 && json.Unmarshal(value, &run) == nil && kv.trigger(run) {
		kv.injected++
		entry, err := kv.KeyValue.Get(ctx, key)
		if err == nil {
			_, err = kv.KeyValue.Put(ctx, key, entry.Value())
		}
		kv.err = err
	}
	kv.mu.Unlock()
	return kv.KeyValue.Update(ctx, key, value, revision)
}

// fanOutDef is a -> {nap (sleep), t1, t2}: a's completion makes three
// steps ready at once, one of which saves again on dispatch.
func fanOutDef() dag.WorkflowDef {
	wb := dag.NewWorkflow("fanout-atomic")
	first := wb.Task("a", "fan-a")
	wb.Sleep("nap", time.Hour).After(first)
	wb.Task("t1", "fan-t1").After(first)
	wb.Task("t2", "fan-t2").After(first)
	def, err := wb.Build()
	if err != nil {
		panic("fanOutDef: " + err.Error())
	}
	return def
}

func TestFanOutDispatch_LaterSaveConflictStillDispatchesEveryStep(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	def := fanOutDef()
	defKV, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	mustPutJS(t, ctx, defKV, def.Name, mustMarshal(t, def))
	mustPutJS(t, ctx, defKV, dag.DefVersionKey(def.Name, dag.DefHash(def)), mustMarshal(t, def))

	orch := NewOrchestrator(nc)
	runsKV, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("workflow_runs: %v", err)
	}
	wrapped := &conflictOnceKV{KeyValue: runsKV, trigger: func(run dag.WorkflowRun) bool {
		return run.Steps["nap"].Status == dag.StepStatusRunning
	}}
	orch.store = &SnapshotStore{kv: wrapped, nc: nc}

	run := dag.NewWorkflowRun(def, "run-fanout")
	run.Status = dag.RunStatusRunning
	run.DefHash = dag.DefHash(def)
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusRunning, Attempts: 1}
	if err := orch.store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}

	evt := protocol.NewStepEvent(protocol.EventStepCompleted, run.RunID, "a", []byte(`"ok"`))
	if err := orch.handleEventWithConflictRetry(ctx, evt); err != nil {
		t.Fatalf("handle step.completed(a): %v", err)
	}
	if wrapped.injected != 1 || wrapped.err != nil {
		t.Fatalf("conflict injected %d times (err %v), want exactly 1",
			wrapped.injected, wrapped.err)
	}
	assertFanOutDispatched(t, ctx, orch, js, run.RunID)
}

// TestWorkflowStarted_RedeliveryAfterLostDispatchSaveDispatches: the
// start's first save (the new run) landed but its dispatch save lost
// to a rival, so the run exists with every step still pending. Handling
// workflow.started again must dispatch the entry step, not skip the
// run as an already-started redelivery.
func TestWorkflowStarted_RedeliveryAfterLostDispatchSaveDispatches(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	def := fanOutDef()
	defKV, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	mustPutJS(t, ctx, defKV, def.Name, mustMarshal(t, def))
	mustPutJS(t, ctx, defKV, dag.DefVersionKey(def.Name, dag.DefHash(def)), mustMarshal(t, def))

	orch := NewOrchestrator(nc)
	run := dag.NewWorkflowRun(def, "run-undispatched")
	run.Status = dag.RunStatusRunning
	run.DefHash = dag.DefHash(def)
	if err := orch.store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, run.RunID, mustMarshal(t, def),
	)
	if err := orch.handleEventWithConflictRetry(ctx, evt); err != nil {
		t.Fatalf("handle workflow.started: %v", err)
	}
	got, err := orch.store.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Steps["a"].Status != dag.StepStatusQueued {
		t.Errorf("a = %v, want queued", got.Steps["a"].Status)
	}
	tasks := streamSubjectCounts(t, ctx, js, "TASK_QUEUES", "task.>")
	if tasks["task.fan-a."+run.RunID] != 1 {
		t.Errorf("entry task published %d times, want 1 (all: %v)",
			tasks["task.fan-a."+run.RunID], tasks)
	}
}

// TestRunDispatchPlan_OneFailedEffectStillRunsTheRest: every ready
// step is already saved queued or running when the effects run, and a
// redelivery only dispatches pending steps, so an effect that stops
// the loop strands every sibling after it. All effects and the task
// batch must run, and the failure must still be reported.
func TestRunDispatchPlan_OneFailedEffectStillRunsTheRest(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	def := fanOutDef()
	orch := NewOrchestrator(nc)
	run := dag.NewWorkflowRun(def, "run-effects")
	run.Status = dag.RunStatusRunning

	scheduleFailed := errors.New("schedule: no responders")
	laterRan := false
	t1, _ := findStepDef(def, "t1")
	plan := dispatchPlan{
		effects: []stepEffect{
			{stepID: "nap", send: func(context.Context) error { return scheduleFailed }},
			{stepID: "t2", send: func(context.Context) error { laterRan = true; return nil }},
		},
		normalSteps: []dag.StepDef{t1},
	}
	err = orch.runDispatchPlan(ctx, def, run, plan)
	if !errors.Is(err, scheduleFailed) {
		t.Fatalf("err = %v, want the failed effect reported", err)
	}
	if !laterRan {
		t.Error("an effect after the failed one never ran")
	}
	tasks := streamSubjectCounts(t, ctx, js, "TASK_QUEUES", "task.>")
	if tasks["task.fan-t1."+run.RunID] != 1 {
		t.Errorf("t1 published %d times after a sibling's effect failed, want 1",
			tasks["task.fan-t1."+run.RunID])
	}
}

// TestWorkflowStarted_RedeliveryWithReadyPendingStepsHeals: the entry
// step finished at dispatch (a detached sub-workflow) but the save that
// would dispatch its successors lost a conflict. The run is running
// with ready steps still pending, and workflow.started handled again
// must dispatch them rather than skip a run that is not all-pending.
func TestWorkflowStarted_RedeliveryWithReadyPendingStepsHeals(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	def := fanOutDef()
	defKV, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	mustPutJS(t, ctx, defKV, def.Name, mustMarshal(t, def))
	mustPutJS(t, ctx, defKV, dag.DefVersionKey(def.Name, dag.DefHash(def)), mustMarshal(t, def))

	orch := NewOrchestrator(nc)
	run := dag.NewWorkflowRun(def, "run-ready-pending")
	run.Status = dag.RunStatusRunning
	run.DefHash = dag.DefHash(def)
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted}
	if err := orch.store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, run.RunID, mustMarshal(t, def),
	)
	if err := orch.handleEventWithConflictRetry(ctx, evt); err != nil {
		t.Fatalf("handle workflow.started: %v", err)
	}
	assertFanOutDispatched(t, ctx, orch, js, run.RunID)
}

// assertFanOutDispatched checks the snapshot and the queues: the sleep
// step is running with a wake time and a timer, and t1 and t2 are
// queued with one task message each.
func assertFanOutDispatched(
	t *testing.T, ctx context.Context, orch *Orchestrator,
	js jetstream.JetStream, runID string,
) {
	t.Helper()
	got, err := orch.store.Load(ctx, runID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if nap := got.Steps["nap"]; nap.Status != dag.StepStatusRunning || nap.WakeAt == nil {
		t.Errorf("nap = %v (wake_at %v), want running with a wake time",
			nap.Status, nap.WakeAt)
	}
	for _, stepID := range []string{"t1", "t2"} {
		if got.Steps[stepID].Status != dag.StepStatusQueued {
			t.Errorf("%s = %v, want queued", stepID, got.Steps[stepID].Status)
		}
	}
	tasks := streamSubjectCounts(t, ctx, js, "TASK_QUEUES", "task.>")
	for _, subject := range []string{"task.fan-t1." + runID, "task.fan-t2." + runID} {
		if tasks[subject] != 1 {
			t.Errorf("%s published %d times, want 1 (all: %v)", subject, tasks[subject], tasks)
		}
	}
	timers := streamSubjectCounts(t, ctx, js, "SLEEP_TIMERS", ">")
	if len(timers) == 0 {
		t.Error("no sleep timer was scheduled for nap")
	}
}

// streamSubjectCounts returns the per-subject message counts of stream
// for subjects matching filter.
func streamSubjectCounts(
	t *testing.T, ctx context.Context, js jetstream.JetStream, stream, filter string,
) map[string]uint64 {
	t.Helper()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(filter))
	if err != nil {
		t.Fatalf("stream %s info: %v", stream, err)
	}
	return info.State.Subjects
}

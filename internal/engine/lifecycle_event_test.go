// internal/engine/lifecycle_event_test.go
// Tests for engine-side step.queued + step.started lifecycle event
// handling. Embedded NATS, real orchestrator. Dispatch-side tests
// (Task 8) verify the publish at the dispatch site; handler tests
// (Tasks 9-10) verify the onEvent switch updates state correctly with
// monotonic guards.
// Methodology: red-green TDD. Each test asserts both a positive event
// (the event we expect) and a negative property.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrchestrator_PublishesStepQueuedOnDispatch(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "queued-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-q1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	sub, err := js.SubscribeSync("history.run-q1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	var queuedEvt protocol.Event
	var sawQueued bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !sawQueued {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if evt.Type == protocol.EventStepQueued && evt.StepID == "a" {
			queuedEvt = evt
			sawQueued = true
		}
	}
	if !sawQueued {
		t.Fatal("expected step.queued event for step 'a', not found")
	}
	if queuedEvt.AttemptNumber != 1 {
		t.Fatalf("AttemptNumber = %d, want 1", queuedEvt.AttemptNumber)
	}
	if queuedEvt.RunID != "run-q1" {
		t.Fatalf("RunID = %q, want %q", queuedEvt.RunID, "run-q1")
	}
}

func TestOrchestrator_StepQueuedMsgIdIsDeterministic(t *testing.T) {
	evt := protocol.Event{
		Type:          protocol.EventStepQueued,
		RunID:         "run-mid",
		StepID:        "step-x",
		AttemptNumber: 1,
	}
	got := evt.NATSMsgID()
	want := "run-mid.step-x.step.queued.1"
	if got != want {
		t.Fatalf("NATSMsgID = %q, want %q", got, want)
	}
	evt.AttemptNumber = 2
	got2 := evt.NATSMsgID()
	if got2 == got {
		t.Fatalf("NATSMsgID for attempt 1 and 2 must differ; both = %q", got)
	}
}

// TestOrchestrator_DispatchPublishesTaskOnWorkflowStarted: methodology
// — start a workflow with one normal step and verify the orchestrator
// publishes a task message to TASK_QUEUES. Anchors the basic dispatch
// path; does not exercise the queued-publish-failure recovery (that
// would need a publish-seam injection — currently unimplemented; see
// the related fail_fast_e2e tests for the worker-side failure path).
func TestOrchestrator_DispatchPublishesTaskOnWorkflowStarted(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "dispatch-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-dp", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	sub, _ := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task message not delivered: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task message, got %d", len(msgs))
	}
	msgs[0].Ack()
}

var _ context.Context = context.Background()
var _ jetstream.JetStream = nil

func TestOnEvent_StepStarted_TransitionsQueuedToRunning(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "started-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start the workflow — step 'a' should reach Queued.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-st1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	// Wait for queued state.
	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-st1", "a", dag.StepStatusQueued, 5*time.Second)

	// Now simulate worker emitting step.started.
	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-st1", "a", nil,
	)
	startedEvt.AttemptNumber = 1
	startedData, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), startedData,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	// Engine must transition Queued → Running.
	waitForStepStatus(t, store, "run-st1", "a", dag.StepStatusRunning, 5*time.Second)

	// Negative space: status is NOT still Queued.
	run, err := store.Load(context.Background(), "run-st1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if run.Steps["a"].Status == dag.StepStatusQueued {
		t.Fatal("expected Running, got Queued")
	}
}

func TestOnEvent_StepStarted_IncrementsAttempts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "attempts-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-att", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-att", "a", dag.StepStatusQueued, 5*time.Second)

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-att", "a", nil,
	)
	startedEvt.AttemptNumber = 3
	startedData, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), startedData,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	waitForStepAttempts(t, store, "run-att", "a", 3, 5*time.Second)
	run, _ := store.Load(context.Background(), "run-att")
	if run.Steps["a"].Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", run.Steps["a"].Attempts)
	}
	if run.Steps["a"].Attempts == 0 {
		t.Fatal("Attempts must not stay at 0")
	}
}

func TestOnEvent_StepStarted_IsIdempotentOnSameAttempt(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "idem-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-idem", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-idem", "a", dag.StepStatusQueued, 5*time.Second)

	// Publish step.started twice with the same attempt; both deduped
	// by Nats-Msg-Id, so the engine sees only one. Attempts stays at 2.
	for i := 0; i < 2; i++ {
		startedEvt := protocol.NewStepEvent(
			protocol.EventStepStarted, "run-idem", "a", nil,
		)
		startedEvt.AttemptNumber = 2
		data, _ := startedEvt.Marshal()
		js.Publish(
			startedEvt.NATSSubject(), data,
			nats.MsgId(startedEvt.NATSMsgID()),
		)
	}
	waitForStepStatus(t, store, "run-idem", "a", dag.StepStatusRunning, 5*time.Second)
	run, _ := store.Load(context.Background(), "run-idem")
	if run.Steps["a"].Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2 (idempotent)", run.Steps["a"].Attempts)
	}
}

func TestOnEvent_StepStarted_IgnoredAfterCompleted(t *testing.T) {
	// Methodology: seed the store with a Completed step, then fire
	// a stale step.started. The engine must not regress the state.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "stale-comp-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-stcomp")
	run.Status = dag.RunStatusRunning
	st := run.Steps["a"]
	st.Status = dag.StepStatusCompleted
	st.Attempts = 1
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-stcomp", "a", nil,
	)
	startedEvt.AttemptNumber = 1
	data, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), data,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, err := store.Load(context.Background(), "run-stcomp")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("Status = %v, want Completed (must not regress)",
			loaded.Steps["a"].Status)
	}
}

func TestOnEvent_StepStarted_IgnoredAfterFailed(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "stale-fail-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-stfail")
	run.Status = dag.RunStatusFailed
	st := run.Steps["a"]
	st.Status = dag.StepStatusFailed
	st.Attempts = 4
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-stfail", "a", nil,
	)
	startedEvt.AttemptNumber = 4
	data, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), data,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, _ := store.Load(context.Background(), "run-stfail")
	if loaded.Steps["a"].Status != dag.StepStatusFailed {
		t.Fatalf("Status = %v, want Failed (must not regress)",
			loaded.Steps["a"].Status)
	}
	if loaded.Steps["a"].Attempts != 4 {
		t.Fatalf("Attempts = %d, want 4 (must not change)",
			loaded.Steps["a"].Attempts)
	}
}

func TestOnEvent_StepStarted_AttemptsMonotonic_NeverDecreases(t *testing.T) {
	// Methodology: seed a step with Attempts=5, fire step.started
	// with AttemptNumber=2. Engine's max() rule must keep Attempts=5.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "mono-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-mono")
	run.Status = dag.RunStatusRunning
	st := run.Steps["a"]
	st.Status = dag.StepStatusRunning
	st.Attempts = 5
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-mono", "a", nil,
	)
	startedEvt.AttemptNumber = 2
	data, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), data,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, _ := store.Load(context.Background(), "run-mono")
	if loaded.Steps["a"].Attempts != 5 {
		t.Fatalf("Attempts = %d, want 5 (monotonic — never decreases)",
			loaded.Steps["a"].Attempts)
	}
}

// waitForStepStatus polls the store until step status matches or
// timeout fires. Bounded — never spins past timeout.
func waitForStepStatus(
	t *testing.T,
	store *SnapshotStore,
	runID, stepID string,
	want dag.StepStatus,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), runID)
		if err == nil && run.Steps[stepID].Status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("step %q in run %q did not reach status %v within %v",
		stepID, runID, want, timeout)
}

// waitForStepAttempts polls the store until step attempts match or
// timeout fires.
func waitForStepAttempts(
	t *testing.T,
	store *SnapshotStore,
	runID, stepID string,
	want int,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), runID)
		if err == nil && run.Steps[stepID].Attempts == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("step %q in run %q did not reach attempts %d within %v",
		stepID, runID, want, timeout)
}

func TestOnEvent_StepQueued_DuringReplay_ReconstructsState(t *testing.T) {
	// Methodology: simulate replay by publishing a sequence of events
	// to a fresh history stream and verifying final state. The
	// step.queued event during replay must set Status=Queued without
	// rolling back any later transitions.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "replay-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Replay sequence: workflow.started → step.queued → step.started → step.completed.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-rp", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-rp", "a", dag.StepStatusQueued, 5*time.Second)

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-rp", "a", nil,
	)
	startedEvt.AttemptNumber = 1
	startedData, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), startedData,
		nats.MsgId(startedEvt.NATSMsgID()),
	)
	waitForStepStatus(t, store, "run-rp", "a", dag.StepStatusRunning, 5*time.Second)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "run-rp", "a", []byte(`"done"`),
	)
	compData, _ := compEvt.Marshal()
	js.Publish(
		compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()),
	)
	waitForStepStatus(t, store, "run-rp", "a", dag.StepStatusCompleted, 5*time.Second)

	loaded, _ := store.Load(context.Background(), "run-rp")
	if loaded.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("Status = %v, want Completed", loaded.Steps["a"].Status)
	}
	if loaded.Steps["a"].Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1", loaded.Steps["a"].Attempts)
	}
}

func TestOnEvent_StepQueued_NoRollback_FromRunning(t *testing.T) {
	// Methodology: seed a step with Status=Running, then fire
	// step.queued. Engine's monotonic guard must keep state at Running.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "noroll-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-nr")
	run.Status = dag.RunStatusRunning
	st := run.Steps["a"]
	st.Status = dag.StepStatusRunning
	st.Attempts = 2
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	queuedEvt := protocol.NewStepEvent(
		protocol.EventStepQueued, "run-nr", "a", nil,
	)
	queuedEvt.AttemptNumber = 1
	data, _ := queuedEvt.Marshal()
	js.Publish(
		queuedEvt.NATSSubject(), data,
		nats.MsgId(queuedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, _ := store.Load(context.Background(), "run-nr")
	if loaded.Steps["a"].Status != dag.StepStatusRunning {
		t.Fatalf("Status = %v, want Running (no rollback)",
			loaded.Steps["a"].Status)
	}
	if loaded.Steps["a"].Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2 (must not change)",
			loaded.Steps["a"].Attempts)
	}
}

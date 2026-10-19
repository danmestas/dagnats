// engine/dispatch_crash_test.go
// Methodology: real embedded NATS server per test, orchestrators built
// but not started, events handled through the same conflict-retry entry
// point the history consumer uses. A crash is simulated with the
// orchestrator's crashHook: the dispatch stops at a checkpoint (after
// the dispatch save, or after the sends but before the sent marker is
// saved), that orchestrator is discarded, and a fresh one handles the
// same event again, as a redelivery after an engine restart would.
// Assertions read the durable state directly: TASK_QUEUES and
// SLEEP_TIMERS per-subject counts, the event_waiters bucket, and the
// run snapshot.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// errSimulatedCrash stands in for the engine process dying.
var errSimulatedCrash = errors.New("simulated engine crash")

// crashDef is a -> {t1 (task), nap (sleep), w (wait_for_event)}: a's
// completion dispatches a task, arms a timer and registers a waiter.
func crashDef() dag.WorkflowDef {
	return crashDefTimed(time.Hour, time.Hour)
}

// crashDefTimed is crashDef with the sleep and the wait timeout given.
func crashDefTimed(sleepFor, waitTimeout time.Duration) dag.WorkflowDef {
	wb := dag.NewWorkflow("dispatch-crash")
	first := wb.Task("a", "crash-a")
	wb.Task("t1", "crash-t1").After(first)
	wb.Sleep("nap", sleepFor).After(first)
	wb.WaitForEvent("w", dag.WaitForEventOpts{
		Event: "payment.completed",
		Match: dag.Match{
			Left: "order_id", Op: dag.MatchOpEq,
			Right: "step.a.output.order_id",
		},
		Timeout: waitTimeout,
	}).After(first)
	def, err := wb.Build()
	if err != nil {
		panic("crashDef: " + err.Error())
	}
	return def
}

// crashFixture starts a server, registers def, and saves a running run
// whose step a is running, so step.completed(a) dispatches the rest.
type crashFixture struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	ctx context.Context
	def dag.WorkflowDef
	run dag.WorkflowRun
	evt protocol.Event
}

func newCrashFixture(t *testing.T, def dag.WorkflowDef, runID string) crashFixture {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	mustPutJS(t, ctx, defKV, def.Name, mustMarshal(t, def))
	mustPutJS(t, ctx, defKV, dag.DefVersionKey(def.Name, dag.DefHash(def)),
		mustMarshal(t, def))
	run := dag.NewWorkflowRun(def, runID)
	run.Status = dag.RunStatusRunning
	run.DefHash = dag.DefHash(def)
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusRunning, Attempts: 1}
	if err := NewOrchestrator(nc).store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	output := []byte(`{"order_id":"o-1"}`)
	evt := protocol.NewStepEvent(protocol.EventStepCompleted, runID, "a", output)
	return crashFixture{nc: nc, js: js, ctx: ctx, def: def, run: run, evt: evt}
}

// crashAt returns an orchestrator whose dispatch dies at checkpoint.
func crashAt(nc *nats.Conn, checkpoint dispatchCheckpoint) *Orchestrator {
	orch := NewOrchestrator(nc)
	orch.crashHook = func(reached dispatchCheckpoint) error {
		if reached == checkpoint {
			return errSimulatedCrash
		}
		return nil
	}
	return orch
}

// handleCrashing handles fx.evt on an orchestrator that dies at
// checkpoint and requires that it did die there.
func handleCrashing(t *testing.T, fx crashFixture, checkpoint dispatchCheckpoint) {
	t.Helper()
	err := crashAt(fx.nc, checkpoint).handleEventWithConflictRetry(fx.ctx, fx.evt)
	if !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("first handle: err = %v, want the simulated crash", err)
	}
}

func TestDispatchCrash_AfterSaveBeforeSend_RedeliveryDispatches(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-crash-saved")
	handleCrashing(t, fx, checkpointSaved)
	tasks := streamSubjectCounts(t, fx.ctx, fx.js, "TASK_QUEUES", "task.>")
	if len(tasks) != 0 {
		t.Fatalf("crash before send published tasks %v, want none", tasks)
	}

	// The restarted engine gets the unacked step.completed again.
	if err := NewOrchestrator(fx.nc).handleEventWithConflictRetry(
		fx.ctx, fx.evt,
	); err != nil {
		t.Fatalf("redelivered handle: %v", err)
	}
	assertCrashDispatched(t, fx)
}

// assertCrashDispatched checks that t1's task, nap's timer and w's
// waiter and timeout timer each exist exactly once, and that no step
// is left marked unsent.
func assertCrashDispatched(t *testing.T, fx crashFixture) {
	t.Helper()
	runID := fx.run.RunID
	tasks := streamSubjectCounts(t, fx.ctx, fx.js, "TASK_QUEUES", "task.>")
	if got := tasks["task.crash-t1."+runID]; got != 1 {
		t.Errorf("t1 task published %d times, want 1 (all: %v)", got, tasks)
	}
	timers := streamSubjectCounts(t, fx.ctx, fx.js, "SLEEP_TIMERS", ">")
	for _, stepID := range []string{"nap", "w"} {
		if got := timers["sleep."+runID+"."+stepID]; got != 1 {
			t.Errorf("%s timer scheduled %d times, want 1 (all: %v)",
				stepID, got, timers)
		}
	}
	waiters, err := fx.js.KeyValue(fx.ctx, "event_waiters")
	if err != nil {
		t.Fatalf("event_waiters: %v", err)
	}
	if _, err := waiters.Get(fx.ctx, "payment.completed."+runID+".w"); err != nil {
		t.Errorf("waiter for w not registered: %v", err)
	}
	got, err := NewOrchestrator(fx.nc).store.Load(fx.ctx, runID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, stepID := range []string{"t1", "nap", "w"} {
		if got.Steps[stepID].DispatchUnsent {
			t.Errorf("%s still marked unsent after its dispatch", stepID)
		}
	}
}

// shrinkTaskDedupWindow sets TASK_QUEUES' msg-id dedup window to its
// floor and waits it out, standing in for an engine restart slower
// than the production two-minute window: a send repeated after this
// is not dropped as a duplicate by the stream.
func shrinkTaskDedupWindow(t *testing.T, fx crashFixture) {
	t.Helper()
	stream, err := fx.js.Stream(fx.ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("TASK_QUEUES: %v", err)
	}
	cfg := stream.CachedInfo().Config
	cfg.Duplicates = 100 * time.Millisecond
	if _, err := fx.js.UpdateStream(fx.ctx, cfg); err != nil {
		t.Fatalf("shrink TASK_QUEUES dedup window: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
}

func TestDispatchCrash_AfterSendBeforeMark_RedeliveryDoesNotDuplicate(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-crash-sent")
	shrinkTaskDedupWindow(t, fx)
	handleCrashing(t, fx, checkpointSent)
	snapshot, err := NewOrchestrator(fx.nc).store.Load(fx.ctx, fx.run.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !snapshot.Steps["t1"].DispatchUnsent {
		t.Fatal("t1 not marked unsent after a crash before the mark save")
	}
	time.Sleep(300 * time.Millisecond) // past the dedup window again

	if err := NewOrchestrator(fx.nc).handleEventWithConflictRetry(
		fx.ctx, fx.evt,
	); err != nil {
		t.Fatalf("redelivered handle: %v", err)
	}
	// The task is still queued under its dispatch nonce, so it is not
	// queued a second time; the timers are within their own window.
	assertCrashDispatched(t, fx)
}

func TestDispatch_NoCrash_SendsOnceAndRedeliveryAddsNothing(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-no-crash")
	shrinkTaskDedupWindow(t, fx)
	orch := NewOrchestrator(fx.nc)
	if err := orch.handleEventWithConflictRetry(fx.ctx, fx.evt); err != nil {
		t.Fatalf("handle: %v", err)
	}
	assertCrashDispatched(t, fx)
	time.Sleep(300 * time.Millisecond) // past the dedup window again
	if err := orch.handleEventWithConflictRetry(fx.ctx, fx.evt); err != nil {
		t.Fatalf("redelivered handle: %v", err)
	}
	assertCrashDispatched(t, fx)
}

func TestDispatchCrash_StartedStepIsNotResent(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-crash-started")
	shrinkTaskDedupWindow(t, fx)
	handleCrashing(t, fx, checkpointSent)
	// A worker took t1 and acked it, so it is gone from TASK_QUEUES;
	// its step.started reaches the restarted engine first.
	drainTasks(t, fx, "task.crash-t1."+fx.run.RunID)
	orch := NewOrchestrator(fx.nc)
	started := protocol.NewStepEvent(protocol.EventStepStarted, fx.run.RunID, "t1", nil)
	started.AttemptNumber = 1
	if err := orch.handleEventWithConflictRetry(fx.ctx, started); err != nil {
		t.Fatalf("handle step.started(t1): %v", err)
	}
	tasks := streamSubjectCounts(t, fx.ctx, fx.js, "TASK_QUEUES", "task.>")
	if len(tasks) != 0 {
		t.Errorf("a started step's task was sent again: %v", tasks)
	}
	got, err := orch.store.Load(fx.ctx, fx.run.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if t1 := got.Steps["t1"]; t1.Status != dag.StepStatusRunning || t1.Attempts != 1 {
		t.Errorf("t1 = %v attempt %d, want running attempt 1", t1.Status, t1.Attempts)
	}
	for _, stepID := range []string{"t1", "nap", "w"} {
		if got.Steps[stepID].DispatchUnsent {
			t.Errorf("%s still marked unsent", stepID)
		}
	}
}

// drainTasks consumes and acks every message on subject, as a worker
// that ran the task would.
func drainTasks(t *testing.T, fx crashFixture, subject string) {
	t.Helper()
	consumer, err := fx.js.CreateConsumer(fx.ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
		FilterSubject: subject, AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("consumer on %s: %v", subject, err)
	}
	batch, err := consumer.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch %s: %v", subject, err)
	}
	drained := 0
	for msg := range batch.Messages() {
		if err := msg.DoubleAck(fx.ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
		drained++
	}
	if drained != 1 {
		t.Fatalf("drained %d tasks from %s, want 1", drained, subject)
	}
	if err := fx.js.DeleteConsumer(fx.ctx, "TASK_QUEUES", consumer.CachedInfo().Name); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
}

func TestDispatchCrash_ReconcilerResendsWhenNoEventArrives(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-crash-reconcile")
	handleCrashing(t, fx, checkpointSaved)
	// The event dead-lettered or was acked: only the reconciler visits.
	NewOrchestrator(fx.nc).reconcileOneRun(fx.ctx, fx.run.RunID)
	assertCrashDispatched(t, fx)
}

// publishHistory publishes evt to WORKFLOW_HISTORY, as a worker or the
// stream's redelivery would hand it to a running engine.
func publishHistory(t *testing.T, fx crashFixture, evt protocol.Event) {
	t.Helper()
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal %s: %v", evt.Type, err)
	}
	if _, err := fx.js.Publish(fx.ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID())); err != nil {
		t.Fatalf("publish %s: %v", evt.Type, err)
	}
}

func TestDispatchCrash_RestartedEngineCompletesTheRun(t *testing.T) {
	def := crashDefTimed(100*time.Millisecond, 300*time.Millisecond)
	fx := newCrashFixture(t, def, "run-crash-complete")
	handleCrashing(t, fx, checkpointSaved)

	restarted := NewOrchestrator(fx.nc)
	if err := restarted.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer restarted.Stop()
	publishHistory(t, fx, fx.evt) // the unacked step.completed(a)

	consumer, err := fx.js.CreateConsumer(fx.ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
		FilterSubject: "task.crash-t1." + fx.run.RunID,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("task consumer: %v", err)
	}
	task, err := consumer.Next(jetstream.FetchMaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("t1 task never delivered after the restart: %v", err)
	}
	if err := task.DoubleAck(fx.ctx); err != nil {
		t.Fatalf("ack t1: %v", err)
	}
	done := protocol.NewStepEvent(protocol.EventStepCompleted, fx.run.RunID, "t1", []byte(`"ok"`))
	publishHistory(t, fx, done)
	waitForRunStatus(t, restarted.store, fx.run.RunID, dag.RunStatusCompleted, 15*time.Second)

	got, err := restarted.store.Load(fx.ctx, fx.run.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if string(got.Steps["w"].Output) != `{"timeout":true}` {
		t.Errorf("w output = %s, want its timeout to have fired", got.Steps["w"].Output)
	}
	if extra, err := consumer.Next(jetstream.FetchMaxWait(500 * time.Millisecond)); err == nil {
		t.Errorf("a second t1 task was queued: %s", extra.Data())
	}
}

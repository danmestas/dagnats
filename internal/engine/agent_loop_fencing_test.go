// internal/engine/agent_loop_fencing_test.go
// Fenced step events for agent loops (#801): a duplicate step.continue
// advances the loop once, a stale attempt's event is dropped, a retry
// resumes from the state the last accepted continue committed, an
// over-cap state fails the loop cleanly, loop_delay survives an engine
// restart, an engine crash between the continue save and the task
// publish recovers, and an event without identity is accepted only
// while legacy events are enabled.
//
// Methodology: a real orchestrator on an embedded NATS server. The
// test plays the worker: it takes each task from TASK_QUEUES, reads
// the dispatch identity off the payload and publishes step.started
// and the lifecycle events a worker would, with that identity. Two
// continues for one iteration are what a worker that published
// Continue and died before acking (redelivery runs iteration 0 again)
// or a stalled worker racing its AckWait replacement produce. Task
// queues are drained with bounded fetches; every wait is bounded.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// loopFixture is an engine with one registered agent-loop workflow.
type loopFixture struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	jsNew   jetstream.JetStream
	defData []byte
	taskSub *nats.Subscription
	store   *SnapshotStore
}

// loopDef is a one-step agent-loop workflow running taskType.
func loopDef(name, taskType string, cfg dag.AgentLoopConfig, retry *dag.RetryPolicy) dag.WorkflowDef {
	if cfg.MaxIterations == 0 {
		cfg.MaxIterations = 10
	}
	return dag.WorkflowDef{
		Name: name, Version: "1",
		Steps: []dag.StepDef{{
			ID: "agent", Task: taskType, Type: dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&cfg), Retry: retry,
		}},
	}
}

// newLoopFixture starts NATS, registers wfDef and subscribes to its
// task subject. The orchestrator is the caller's to start.
func newLoopFixture(t *testing.T, wfDef dag.WorkflowDef) loopFixture {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)
	mustPut(t, defKV, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)), defData)
	taskSub, err := js.PullSubscribe(
		"task."+wfDef.Steps[0].Task+".*", "", nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	return loopFixture{
		nc: nc, js: js, jsNew: jsNew, defData: defData, taskSub: taskSub,
		store: NewSnapshotStore(jsNew),
	}
}

// startRun publishes workflow.started for runID.
func (fx loopFixture) startRun(t *testing.T, runID string) {
	t.Helper()
	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, runID, fx.defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, fx.js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))
}

// takeTask fetches the next task within wait, acks it and returns its
// payload.
func (fx loopFixture) takeTask(t *testing.T, wait time.Duration) protocol.TaskPayload {
	t.Helper()
	msgs, err := fx.taskSub.Fetch(1, nats.MaxWait(wait))
	if err != nil {
		t.Fatalf("Fetch task failed: %v", err)
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msgs[0].Data, &payload); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack task: %v", err)
	}
	return payload
}

// noTask fails the test if a task arrives within wait.
func (fx loopFixture) noTask(t *testing.T, wait time.Duration) {
	t.Helper()
	msgs, err := fx.taskSub.Fetch(1, nats.MaxWait(wait))
	if err == nil && len(msgs) > 0 {
		t.Fatalf("unexpected task: %s", msgs[0].Data)
	}
}

// publishEvent publishes evt under its own msg-id.
func (fx loopFixture) publishEvent(t *testing.T, evt protocol.Event) {
	t.Helper()
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, fx.js, evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
}

// started publishes the step.started a worker sends on claiming task,
// with the attempt number it would resolve (payload attempt, else 1)
// and the task's dispatch nonce.
func (fx loopFixture) started(t *testing.T, task protocol.TaskPayload) int {
	t.Helper()
	attempt := task.Attempt
	if attempt == 0 {
		attempt = 1
	}
	evt := protocol.NewStepEvent(protocol.EventStepStarted, task.RunID, task.StepID, nil)
	evt.AttemptNumber = attempt
	evt.DispatchNonce = task.DispatchNonce
	fx.publishEvent(t, evt)
	return attempt
}

// identityEvent builds a lifecycle event answering task, with the
// attempt the worker resolved.
func identityEvent(
	kind protocol.EventType, task protocol.TaskPayload, attempt int, payload []byte,
) protocol.Event {
	evt := protocol.NewStepEvent(kind, task.RunID, task.StepID, payload)
	evt.SetIdentity(task.Iteration, attempt, task.DispatchNonce)
	return evt
}

// continuePayload is a step.continue payload committing state.
func continuePayload(t *testing.T, state []byte) []byte {
	t.Helper()
	return mustMarshal(t, protocol.StepContinuePayload{State: state})
}

// failedPayload is a retriable step.failed payload.
func failedPayload(t *testing.T) []byte {
	t.Helper()
	return mustMarshal(t, protocol.StepFailedPayload{
		Error: "boom", FailureType: protocol.FailureTypeRetriable,
	})
}

// waitStep polls the snapshot until pred holds or window passes.
func (fx loopFixture) waitStep(
	t *testing.T, runID string, window time.Duration, pred func(dag.StepState) bool,
) dag.WorkflowRun {
	t.Helper()
	deadline := time.Now().Add(window)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, err := fx.store.Load(context.Background(), runID)
		if err == nil {
			run = r
			if pred(r.Steps["agent"]) {
				return r
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return run
}

// drainTaskIterations fetches tasks until a 2s fetch comes back empty
// (bounded at 5 tasks) and returns each task's Iteration.
func drainTaskIterations(t *testing.T, sub *nats.Subscription) []int {
	t.Helper()
	const taskCountMax = 5
	var iterations []int
	for i := 0; i < taskCountMax; i++ {
		msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil || len(msgs) == 0 {
			break
		}
		var payload protocol.TaskPayload
		if err := json.Unmarshal(msgs[0].Data, &payload); err != nil {
			t.Fatalf("decode task: %v", err)
		}
		iterations = append(iterations, payload.Iteration)
		if err := msgs[0].Ack(); err != nil {
			t.Fatalf("ack task: %v", err)
		}
	}
	return iterations
}

func TestStepContinue_DuplicateForSameIterationAdvancesOnce(t *testing.T) {
	fx := newLoopFixture(t, loopDef("dup-cont-wf", "dup-agent-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "dup-cont-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)

	// Two continues for iteration 0 with distinct msg-ids: the second
	// is what a redelivered iteration 0 publishes after the dedup
	// window, so JetStream must not be what drops it.
	for nonce := 1; nonce <= 2; nonce++ {
		cont := identityEvent(protocol.EventStepContinue, task, attempt, continuePayload(t, nil))
		msgID := fmt.Sprintf("%s.agent.continue.0.%d", runID, nonce)
		data, err := cont.Marshal()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		mustPublish(t, fx.js, cont.NATSSubject(), data, nats.MsgId(msgID))
	}

	iterations := drainTaskIterations(t, fx.taskSub)
	run, err := fx.store.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("Load run: %v", err)
	}
	if got := run.Steps["agent"].Iterations; got != 1 {
		t.Errorf("Iterations = %d after two continues for iteration 0, want 1", got)
	}
	if len(iterations) != 1 || iterations[0] != 1 {
		t.Errorf("enqueued task iterations = %v, want exactly [1]", iterations)
	}
}

func TestStepFailed_StaleAttemptEventIsDropped(t *testing.T) {
	retry := &dag.RetryPolicy{MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: 100 * time.Millisecond}
	fx := newLoopFixture(t, loopDef("stale-wf", "stale-task", dag.AgentLoopConfig{}, retry))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "stale-run"
	fx.startRun(t, runID)
	task0 := fx.takeTask(t, 5*time.Second)
	attempt0 := fx.started(t, task0)
	fx.publishEvent(t, identityEvent(protocol.EventStepFailed, task0, attempt0, failedPayload(t)))

	// The retry is a new dispatch: its task carries a fresh nonce.
	retryTask := fx.takeTask(t, 5*time.Second)
	if retryTask.Attempt != 2 || retryTask.DispatchNonce == task0.DispatchNonce {
		t.Fatalf("retry task = attempt %d nonce %q, want attempt 2 and a new nonce (was %q)",
			retryTask.Attempt, retryTask.DispatchNonce, task0.DispatchNonce)
	}
	fx.started(t, retryTask)

	// Attempt 1's worker wakes up and reports: both its completion and
	// a second failure name the dispatch the step has left.
	stale := identityEvent(protocol.EventStepCompleted, task0, attempt0, []byte(`"stale"`))
	fx.publishEvent(t, stale)
	staleFail := identityEvent(protocol.EventStepFailed, task0, attempt0, failedPayload(t))
	staleFail.Payload = mustMarshal(t, protocol.StepFailedPayload{Error: "late", FailureType: protocol.FailureTypeRetriable})
	fx.publishEvent(t, staleFail)
	fx.noTask(t, 1500*time.Millisecond)

	run := fx.waitStep(t, runID, time.Second, func(s dag.StepState) bool { return s.Status != dag.StepStatusRunning })
	step := run.Steps["agent"]
	if step.Status != dag.StepStatusRunning || step.Attempts != 2 || len(step.Output) != 0 {
		t.Fatalf("step = %v attempts=%d output=%q, want running attempt 2 with no output",
			step.Status, step.Attempts, step.Output)
	}
	// The live attempt's own completion still lands.
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, retryTask, 2, []byte(`"fresh"`)))
	run = fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.Status == dag.StepStatusCompleted })
	if got := string(run.Steps["agent"].Output); got != `"fresh"` {
		t.Fatalf("output = %s, want \"fresh\"", got)
	}
}

func TestStepContinue_RetryResumesFromCommittedState(t *testing.T) {
	retry := &dag.RetryPolicy{MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: 100 * time.Millisecond}
	fx := newLoopFixture(t, loopDef("resume-wf", "resume-task", dag.AgentLoopConfig{}, retry))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "resume-run"
	fx.startRun(t, runID)
	task0 := fx.takeTask(t, 5*time.Second)
	if task0.State != nil {
		t.Fatalf("iteration 0 carries state %q, want none", task0.State)
	}
	attempt := fx.started(t, task0)
	committed := []byte(`{"turn":1}`)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task0, attempt, continuePayload(t, committed)))

	task1 := fx.takeTask(t, 5*time.Second)
	if task1.Iteration != 1 || string(task1.State) != string(committed) {
		t.Fatalf("iteration task = iteration %d state %s, want 1 with the committed state",
			task1.Iteration, task1.State)
	}
	// The task is published before the sent mark is saved, so wait for
	// the mark to clear rather than reading the snapshot once.
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return !s.DispatchUnsent
	})
	if got := run.Steps["agent"]; string(got.LoopState) != string(committed) || got.DispatchUnsent {
		t.Fatalf("snapshot loop_state=%s unsent=%v, want the committed state and sent",
			got.LoopState, got.DispatchUnsent)
	}

	// Iteration 1 fails: the retry runs iteration 1 again from the
	// state the continue committed, not from scratch.
	attempt1 := fx.started(t, task1)
	fx.publishEvent(t, identityEvent(protocol.EventStepFailed, task1, attempt1, failedPayload(t)))
	retryTask := fx.takeTask(t, 5*time.Second)
	if retryTask.Iteration != 1 || retryTask.Attempt != 2 || string(retryTask.State) != string(committed) {
		t.Fatalf("retry task = iteration %d attempt %d state %s, want iteration 1 attempt 2 with the committed state",
			retryTask.Iteration, retryTask.Attempt, retryTask.State)
	}
	if retryTask.StateMaxBytes != protocol.LoopStateMaxBytesDefault {
		t.Fatalf("state_max_bytes = %d, want the default", retryTask.StateMaxBytes)
	}
}

func TestStepContinue_OverCapStateFailsLoop(t *testing.T) {
	fx := newLoopFixture(t, loopDef("cap-wf", "cap-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc, WithLoopStateMaxBytes(64))
	orch.Start()
	defer orch.Stop()
	const runID = "cap-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	if task.StateMaxBytes != 64 {
		t.Fatalf("state_max_bytes = %d, want the configured 64", task.StateMaxBytes)
	}
	attempt := fx.started(t, task)
	big := []byte(strings.Repeat("x", 100))
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt, continuePayload(t, big)))

	waitForRunStatus(t, fx.store, runID, dag.RunStatusFailed, 5*time.Second)
	run, err := fx.store.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("Load run: %v", err)
	}
	step := run.Steps["agent"]
	if step.Status != dag.StepStatusFailed || step.Iterations != 0 {
		t.Fatalf("step = %v iterations=%d, want failed at iteration 0", step.Status, step.Iterations)
	}
	if !strings.Contains(step.Error, "100 bytes") || !strings.Contains(step.Error, "64 byte cap") {
		t.Fatalf("error = %q, want it to name the size and the cap", step.Error)
	}
	fx.noTask(t, time.Second)
}

func TestStepContinue_LoopDelaySurvivesEngineRestart(t *testing.T) {
	cfg := dag.AgentLoopConfig{LoopDelay: 1500 * time.Millisecond}
	fx := newLoopFixture(t, loopDef("delay-wf", "delay-task", cfg, nil))
	first := NewOrchestrator(fx.nc)
	first.Start()
	const runID = "delay-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	committed := []byte(`{"turn":1}`)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt, continuePayload(t, committed)))

	// The delay is a durable timer; the engine dies while it runs.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deadline := time.Now().Add(5 * time.Second)
	for streamSubjectCounts(t, ctx, fx.jsNew, "SLEEP_TIMERS", "sleep.>")["sleep."+runID+".agent"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("loop_delay timer never reached SLEEP_TIMERS")
		}
		time.Sleep(20 * time.Millisecond)
	}
	first.Stop()
	fx.noTask(t, 200*time.Millisecond)

	second := NewOrchestrator(fx.nc, WithDebounceHandler(func(TimerMessage, uint64) {}))
	second.Start()
	defer second.Stop()
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 || string(next.State) != string(committed) || next.DispatchNonce == task.DispatchNonce {
		t.Fatalf("delayed task = iteration %d state %s nonce %q, want iteration 1 with the committed state and a new nonce",
			next.Iteration, next.State, next.DispatchNonce)
	}
	fx.noTask(t, time.Second)
}

func TestStepContinue_CrashBetweenSaveAndPublishRecovers(t *testing.T) {
	fx := newLoopFixture(t, loopDef("crash-wf", "crash-loop-task", dag.AgentLoopConfig{}, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const runID = "crash-loop-run"
	wfDef := loopDef("crash-wf", "crash-loop-task", dag.AgentLoopConfig{}, nil)
	run := dag.NewWorkflowRun(wfDef, runID)
	run.Status = dag.RunStatusRunning
	run.DefHash = dag.DefHash(wfDef)
	run.Steps["agent"] = dag.StepState{
		Status: dag.StepStatusRunning, Attempts: 1, DispatchNonce: "nonce-0",
	}
	if err := NewOrchestrator(fx.nc).store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	committed := []byte(`{"turn":1}`)
	task := protocol.TaskPayload{RunID: runID, StepID: "agent", DispatchNonce: "nonce-0"}
	evt := identityEvent(protocol.EventStepContinue, task, 1, continuePayload(t, committed))

	err := crashAt(fx.nc, checkpointSaved).handleEventWithConflictRetry(ctx, evt)
	if !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("first handle: err = %v, want the simulated crash", err)
	}
	saved, err := fx.store.Load(ctx, runID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s := saved.Steps["agent"]; s.Iterations != 1 || !s.DispatchUnsent || string(s.LoopState) != string(committed) {
		t.Fatalf("after crash: iterations=%d unsent=%v state=%s, want 1, true, committed",
			s.Iterations, s.DispatchUnsent, s.LoopState)
	}
	fx.noTask(t, 300*time.Millisecond)

	// The restarted engine gets the unacked continue again: the fence
	// drops it (iteration 0 is done) and the unsent mark sends the task.
	if err := NewOrchestrator(fx.nc).handleEventWithConflictRetry(ctx, evt); err != nil {
		t.Fatalf("redelivered handle: %v", err)
	}
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 || string(next.State) != string(committed) {
		t.Fatalf("resent task = iteration %d state %s, want iteration 1 with the committed state",
			next.Iteration, next.State)
	}
	fx.noTask(t, 500*time.Millisecond)
	after, err := fx.store.Load(ctx, runID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s := after.Steps["agent"]; s.Iterations != 1 || s.DispatchUnsent {
		t.Fatalf("after resend: iterations=%d unsent=%v, want 1 and sent", s.Iterations, s.DispatchUnsent)
	}
}

func TestStepCompleted_LegacyEventAcceptedOnlyWithFlag(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy accepted", legacy: true},
		{name: "legacy dropped", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newLoopFixture(t, loopDef("legacy-wf", "legacy-task", dag.AgentLoopConfig{}, nil))
			orch := NewOrchestrator(fx.nc, WithLegacyStepEvents(tc.legacy))
			orch.Start()
			defer orch.Stop()
			const runID = "legacy-run"
			fx.startRun(t, runID)
			task := fx.takeTask(t, 5*time.Second)
			attempt := fx.started(t, task)
			legacy := protocol.NewStepEvent(protocol.EventStepCompleted, runID, "agent", []byte(`"old"`))
			fx.publishEvent(t, legacy)
			run := fx.waitStep(t, runID, 1500*time.Millisecond, func(s dag.StepState) bool {
				return s.Status == dag.StepStatusCompleted
			})
			completed := run.Steps["agent"].Status == dag.StepStatusCompleted
			if completed != tc.legacy {
				t.Fatalf("legacy step.completed accepted = %v, want %v", completed, tc.legacy)
			}
			if tc.legacy {
				return
			}
			// An event that names its dispatch is accepted either way.
			fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, task, attempt, []byte(`"new"`)))
			run = fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
				return s.Status == dag.StepStatusCompleted
			})
			if got := string(run.Steps["agent"].Output); got != `"new"` {
				t.Fatalf("output = %s, want \"new\"", got)
			}
		})
	}
}

func TestAcceptStepEvent(t *testing.T) {
	running := dag.StepState{
		Status: dag.StepStatusRunning, Attempts: 2, Iterations: 3, DispatchNonce: "n",
	}
	withIdentity := func(iteration, attempt int, nonce string) protocol.Event {
		evt := protocol.NewStepEvent(protocol.EventStepCompleted, "r", "s", nil)
		evt.SetIdentity(iteration, attempt, nonce)
		return evt
	}
	legacy := protocol.NewStepEvent(protocol.EventStepContinue, "r", "s", nil)
	queued := running
	queued.Status = dag.StepStatusQueued
	done := running
	done.Status = dag.StepStatusCompleted
	cases := []struct {
		name       string
		state      dag.StepState
		evt        protocol.Event
		legacy     bool
		wantAccept bool
		wantReason string
	}{
		{"match", running, withIdentity(3, 2, "n"), true, true, ""},
		{"match without nonce", running, withIdentity(3, 2, ""), false, true, ""},
		{"stale iteration", running, withIdentity(2, 2, "n"), true, false, rejectIteration},
		{"future iteration", running, withIdentity(4, 2, "n"), true, false, rejectIteration},
		{"stale attempt", running, withIdentity(3, 1, "n"), true, false, rejectAttempt},
		{"wrong nonce", running, withIdentity(3, 2, "other"), true, false, rejectNonce},
		// A Queued step is dispatched but its step.started is not applied
		// yet: the nonce alone names the dispatch and Attempts may lag.
		{"queued step", queued, withIdentity(3, 2, "n"), true, true, ""},
		{"queued lagging attempt", queued, withIdentity(3, 3, "n"), true, true, ""},
		{"queued stale iteration", queued, withIdentity(2, 2, "n"), true, false, rejectIteration},
		{"queued without nonce", queued, withIdentity(3, 2, ""), true, false, rejectNonce},
		{"queued wrong nonce", queued, withIdentity(3, 2, "other"), true, false, rejectNonce},
		{"terminal step", done, withIdentity(3, 2, "n"), true, false, rejectNotRunning},
		{"legacy accepted", done, legacy, true, true, ""},
		{"legacy dropped", running, legacy, false, false, rejectNoIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			accept, reason := acceptStepEvent(tc.state, tc.evt, tc.legacy)
			if accept != tc.wantAccept || reason != tc.wantReason {
				t.Fatalf("acceptStepEvent = (%v, %q), want (%v, %q)",
					accept, reason, tc.wantAccept, tc.wantReason)
			}
		})
	}
}

func TestStepTimeout_FiresForLoopAfterContinues(t *testing.T) {
	wfDef := loopDef("timeout-wf", "timeout-task", dag.AgentLoopConfig{}, nil)
	wfDef.Steps[0].Timeout = 800 * time.Millisecond
	fx := newLoopFixture(t, wfDef)
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "timeout-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	// Two fast continues: the attempt's watchdog outlives both nonces.
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt, continuePayload(t, nil)))
	task1 := fx.takeTask(t, 5*time.Second)
	fx.started(t, task1)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task1, attempt, continuePayload(t, nil)))
	task2 := fx.takeTask(t, 5*time.Second)
	fx.started(t, task2)

	waitForRunStatus(t, fx.store, runID, dag.RunStatusFailed, 5*time.Second)
	run, err := fx.store.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("Load run: %v", err)
	}
	step := run.Steps["agent"]
	if step.Status != dag.StepStatusFailed || step.Iterations != 2 ||
		!strings.Contains(step.Error, "step timeout") {
		t.Fatalf("step = %v iterations=%d error=%q, want failed by the step timeout at iteration 2",
			step.Status, step.Iterations, step.Error)
	}
}

func TestRetryTimerWithoutNonceRedispatchesUnderSnapshotNonce(t *testing.T) {
	// The policy's own backoff is far longer than the test, so the only
	// timer that fires is the one scheduled by hand below.
	retry := &dag.RetryPolicy{MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: time.Minute}
	wfDef := loopDef("oldtimer-wf", "oldtimer-task", dag.AgentLoopConfig{}, retry)
	fx := newLoopFixture(t, wfDef)
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "oldtimer-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt0 := fx.started(t, task)
	fx.publishEvent(t, identityEvent(protocol.EventStepFailed, task, attempt0, failedPayload(t)))
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.RetryDispatchAt != nil
	})
	liveNonce := run.Steps["agent"].DispatchNonce
	if liveNonce == "" || liveNonce == task.DispatchNonce {
		t.Fatalf("retry did not restamp the nonce: %q", liveNonce)
	}
	// A retry timer persisted before #801 carries no nonce.
	stepDef := wfDef.Steps[0]
	err := orch.sleepTimer.Schedule(context.Background(), TimerMessage{
		Action: TimerActionRetryAfter, RunID: runID, StepID: "agent", DurationMs: 50,
		TaskType: stepDef.Task, Subject: orch.publisher.StepSubject(stepDef, runID), Attempt: 1,
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	retryTask := fx.takeTask(t, 5*time.Second)
	if retryTask.DispatchNonce != liveNonce || retryTask.Attempt != 2 {
		t.Fatalf("redispatch = nonce %q attempt %d, want the snapshot nonce %q and attempt 2",
			retryTask.DispatchNonce, retryTask.Attempt, liveNonce)
	}
	attempt := fx.started(t, retryTask)
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, retryTask, attempt, []byte(`"done"`)))
	run = fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted
	})
	if run.Steps["agent"].Status != dag.StepStatusCompleted {
		t.Fatalf("completion under the redispatched nonce was not accepted: %v", run.Steps["agent"].Status)
	}
}

func TestStepContinue_SetEmptyStateClearsSavedState(t *testing.T) {
	fx := newLoopFixture(t, loopDef("clear-wf", "clear-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "clear-run"
	fx.startRun(t, runID)
	task0 := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task0)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task0, attempt, continuePayload(t, []byte(`{"turn":1}`))))
	task1 := fx.takeTask(t, 5*time.Second)
	if string(task1.State) != `{"turn":1}` {
		t.Fatalf("iteration 1 state = %s", task1.State)
	}
	// A continue with no state field leaves the saved state alone...
	fx.started(t, task1)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task1, attempt, []byte(`{}`)))
	task2 := fx.takeTask(t, 5*time.Second)
	if string(task2.State) != `{"turn":1}` {
		t.Fatalf("iteration 2 state = %s, want the kept state", task2.State)
	}
	// ...and one that marks an empty state as set clears it.
	fx.started(t, task2)
	cleared := mustMarshal(t, protocol.StepContinuePayload{StateSet: true})
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task2, attempt, cleared))
	task3 := fx.takeTask(t, 5*time.Second)
	if len(task3.State) != 0 {
		t.Fatalf("iteration 3 state = %s, want cleared", task3.State)
	}
}

func TestStaleRetryTimerDispatchesNothing(t *testing.T) {
	retry := &dag.RetryPolicy{MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: 100 * time.Millisecond}
	wfDef := loopDef("stale-timer-wf", "stale-timer-task", dag.AgentLoopConfig{}, retry)
	fx := newLoopFixture(t, wfDef)
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "stale-timer-run"
	fx.startRun(t, runID)
	task0 := fx.takeTask(t, 5*time.Second)
	attempt0 := fx.started(t, task0)
	fx.publishEvent(t, identityEvent(protocol.EventStepFailed, task0, attempt0, failedPayload(t)))
	retryTask := fx.takeTask(t, 5*time.Second)
	attempt1 := fx.started(t, retryTask)
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Attempts == 2 && s.RetryDispatchAt == nil
	})

	// A leftover timer for attempt 1: the step moved past it, so the
	// fire must publish no task, whatever nonce it would have used.
	stepDef := wfDef.Steps[0]
	for _, action := range []TimerAction{TimerActionRetryBackoff, TimerActionRetryAfter, TimerActionRateRetry} {
		err := orch.sleepTimer.Schedule(context.Background(), TimerMessage{
			Action: action, RunID: runID, StepID: "agent", DurationMs: 50,
			TaskType: stepDef.Task, Subject: orch.publisher.StepSubject(stepDef, runID),
			Attempt: 1, DispatchNonce: task0.DispatchNonce,
		})
		if err != nil {
			t.Fatalf("Schedule %s: %v", action, err)
		}
	}
	fx.noTask(t, 1500*time.Millisecond)

	// The live attempt still completes.
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, retryTask, attempt1, []byte(`"done"`)))
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted
	})
	if run.Steps["agent"].Status != dag.StepStatusCompleted {
		t.Fatalf("live attempt not completed: %v", run.Steps["agent"].Status)
	}
}

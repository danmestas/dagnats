// internal/engine/agent_inputs_test.go
// Durable waits and input wakeups (ADR-027): a step.continue that asks
// for a delay is scheduled on SLEEP_TIMERS and survives an engine
// restart; one that waits for input holds no task until an input is
// queued, then gets its iteration; an input queued while the continue
// that commits the wait is still in flight wakes the step through the
// engine's re-check; a late wait.elapsed after an input wake does not
// send a second task; and the completion sweep settles queued inputs
// as unanswered, including one submitted after the step finished.
//
// Methodology: a real orchestrator on an embedded NATS server with the
// loop fixture of agent_loop_fencing_test.go; the test plays the worker
// (takes tasks, publishes step.started and step.continue with the
// dispatch identity) and the API (writes submissions through the inputs
// store and publishes step.input.queued). Every wait is bounded.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

const inputsTestTimeout = 10 * time.Second

func waitPayload(t *testing.T, wait protocol.ContinueWait) []byte {
	t.Helper()
	return mustMarshal(t, protocol.StepContinuePayload{Wait: &wait})
}

// submitInput plays the API: creates the submission and announces it.
func (fx loopFixture) submitInput(t *testing.T, runID, requestID string) *inputs.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	store, err := inputs.Open(ctx, fx.jsNew)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	if _, _, err := store.Submit(ctx, inputs.Submission{
		RunID: runID, StepID: "agent", RequestID: requestID, Mode: inputs.ModeSteer,
		Content: json.RawMessage(`"x"`),
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	fx.publishEvent(t, protocol.NewInputQueuedEvent(runID, "agent", requestID))
	return store
}

// shortenTaskDedup shrinks TASK_QUEUES' dedup window so a duplicate
// dispatch is not hidden by JetStream.
func (fx loopFixture) shortenTaskDedup(t *testing.T, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	stream, err := fx.jsNew.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cfg := stream.CachedInfo().Config
	cfg.Duplicates = window
	if _, err := fx.jsNew.UpdateStream(ctx, cfg); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}
}

func (fx loopFixture) inputStatus(t *testing.T, store *inputs.Store, runID, requestID string) inputs.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	entry, err := store.Get(ctx, runID, "agent", requestID)
	if err != nil {
		t.Fatalf("Get %s: %v", requestID, err)
	}
	return entry.Status
}

func TestContinueWait_DelaySurvivesEngineRestart(t *testing.T) {
	fx := newLoopFixture(t, loopDef("wait-delay-wf", "wait-delay-task", dag.AgentLoopConfig{}, nil))
	first := NewOrchestrator(fx.nc)
	first.Start()
	const runID = "wait-delay-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
		waitPayload(t, protocol.ContinueWait{DelayMs: 1500})))

	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Iterations == 1 && s.WaitUntil != nil && !s.DispatchUnsent
	})
	if wake := run.Steps["agent"].WaitUntil; time.Until(*wake) > 1500*time.Millisecond {
		t.Fatalf("wake_at %s is further out than the delay", wake)
	}
	first.Stop()
	fx.noTask(t, 200*time.Millisecond)

	second := NewOrchestrator(fx.nc, WithDebounceHandler(func(TimerMessage, uint64) {}))
	second.Start()
	defer second.Stop()
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 || next.DispatchNonce == task.DispatchNonce {
		t.Fatalf("woken task = iteration %d nonce %q, want iteration 1 under a new nonce",
			next.Iteration, next.DispatchNonce)
	}
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.WaitUntil == nil && !s.DispatchUnsent
	})
	fx.noTask(t, time.Second)
}

func TestContinueWait_UntilInputWakesOnSubmission(t *testing.T) {
	fx := newLoopFixture(t, loopDef("wait-input-wf", "wait-input-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "wait-input-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
		waitPayload(t, protocol.ContinueWait{Input: true})))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Iterations == 1 && s.WaitInput && !s.DispatchUnsent
	})
	fx.noTask(t, 500*time.Millisecond)

	store := fx.submitInput(t, runID, "req-1")
	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 {
		t.Fatalf("woken task iteration = %d, want 1", next.Iteration)
	}
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return !s.WaitInput && !s.DispatchUnsent
	})
	if !run.Steps["agent"].HasInputs {
		t.Error("step does not remember it has inputs to settle")
	}
	if got := fx.inputStatus(t, store, runID, "req-1"); got != inputs.StatusQueued {
		t.Errorf("req-1 status = %s, want still queued for the worker to place", got)
	}
	// A late wait.elapsed for the dispatch that was woken is stale.
	// Outside TASK_QUEUES' dedup window, so that a second publish of
	// the iteration would be a real second task.
	fx.shortenTaskDedup(t, 200*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	stale := protocol.NewStepEvent(protocol.EventStepWaitElapsed, runID, "agent", nil)
	stale.SetIdentity(1, run.Steps["agent"].Attempts, run.Steps["agent"].DispatchNonce)
	fx.publishEvent(t, stale)
	fx.noTask(t, time.Second)
}

func TestContinueWait_InputQueuedBeforeTheWaitIsFoundOnEntry(t *testing.T) {
	fx := newLoopFixture(t, loopDef("wait-race-wf", "wait-race-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "wait-race-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)

	// The input lands, and its event is handled, before the continue
	// that commits UntilInput: no event will follow the wait, so the
	// engine's check on entering it (rule 2) is what sends the task.
	fx.submitInput(t, runID, "early")
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.HasInputs })
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
		waitPayload(t, protocol.ContinueWait{Input: true})))

	next := fx.takeTask(t, 5*time.Second)
	if next.Iteration != 1 {
		t.Fatalf("task iteration = %d, want 1 sent at once", next.Iteration)
	}
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return !s.DispatchUnsent })
	if run.Steps["agent"].WaitInput {
		t.Error("step saved as waiting for input it already has")
	}
	fx.noTask(t, 500*time.Millisecond)
}

func TestContinueWait_CompletionSweepSettlesQueuedInputs(t *testing.T) {
	fx := newLoopFixture(t, loopDef("sweep-wf", "sweep-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "sweep-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	store := fx.submitInput(t, runID, "before")
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.HasInputs })
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, task, attempt, []byte(`{}`)))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted && !s.HasInputs
	})
	if got := fx.inputStatus(t, store, runID, "before"); got != inputs.StatusUnanswered {
		t.Errorf("input queued before completion = %s, want unanswered", got)
	}
	// One submitted after the step finished is settled by its own event.
	fx.submitInput(t, runID, "after")
	deadline := time.Now().Add(5 * time.Second)
	for fx.inputStatus(t, store, runID, "after") != inputs.StatusUnanswered {
		if time.Now().After(deadline) {
			t.Fatal("input submitted after completion was never settled")
		}
		time.Sleep(25 * time.Millisecond)
	}
	msgs, err := fx.taskSub.Fetch(1, nats.MaxWait(300*time.Millisecond))
	if err == nil && len(msgs) > 0 {
		t.Fatalf("a finished step was dispatched again: %s", msgs[0].Data)
	}
}

func TestContinueWait_RunFailureSettlesAWaitingStepsInputs(t *testing.T) {
	wfDef := dag.WorkflowDef{Name: "sweep-run-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "agent", Task: "sweep-run-agent", Type: dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 10})},
		{ID: "other", Task: "sweep-run-other"},
	}}
	fx := newLoopFixture(t, wfDef)
	otherSub, err := fx.js.PullSubscribe("task.sweep-run-other.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "sweep-run-run"
	fx.startRun(t, runID)
	agent := fx.takeTask(t, 5*time.Second)
	agentAttempt := fx.started(t, agent)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, agent, agentAttempt,
		waitPayload(t, protocol.ContinueWait{Input: true})))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.WaitInput })
	// An input wakes the agent; the worker takes the task and leaves
	// the step Running with the input still queued.
	store := fx.submitInput(t, runID, "pending")
	fx.takeTask(t, 5*time.Second)
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.HasInputs && !s.WaitInput })

	// A sibling fails the run while the agent step is still Running.
	msgs, err := otherSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch other: %v", err)
	}
	var other protocol.TaskPayload
	if err := json.Unmarshal(msgs[0].Data, &other); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
	otherAttempt := fx.started(t, other)
	fx.publishEvent(t, identityEvent(protocol.EventStepFailed, other, otherAttempt, mustMarshal(t,
		protocol.StepFailedPayload{Error: "boom", FailureType: protocol.FailureTypeNonRetriable})))
	deadline := time.Now().Add(5 * time.Second)
	for fx.inputStatus(t, store, runID, "pending") != inputs.StatusUnanswered {
		if time.Now().After(deadline) {
			run, _ := fx.store.Load(context.Background(), runID)
			t.Fatalf("input of a step on a failed run never settled; run=%s agent=%+v",
				run.Status, run.Steps["agent"])
		}
		time.Sleep(25 * time.Millisecond)
	}
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return !s.HasInputs })
	if !run.Status.IsTerminal() {
		t.Errorf("run status = %s, want terminal", run.Status)
	}
}

func TestInputsStore_TransientOpenFailureRecovers(t *testing.T) {
	fx := newLoopFixture(t, loopDef("store-wf", "store-task", dag.AgentLoopConfig{}, nil))
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	if err := fx.jsNew.DeleteKeyValue(ctx, "submissions"); err != nil {
		t.Fatalf("DeleteKeyValue: %v", err)
	}
	orch := NewOrchestrator(fx.nc)
	if orch.inputsStore(ctx) != nil {
		t.Fatal("store opened without a bucket")
	}
	wfDef := loopDef("store-wf", "store-task", dag.AgentLoopConfig{}, nil)
	run := dag.NewWorkflowRun(wfDef, "store-run")
	run.Status = dag.RunStatusCompleted
	run.Steps["agent"] = dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1, HasInputs: true}
	if err := orch.store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	if err := orch.saveSnapshot(ctx, run, "agent"); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}
	saved, err := fx.store.Load(ctx, "store-run")
	if err != nil || !saved.Steps["agent"].HasInputs {
		t.Fatalf("HasInputs cleared while the store was unavailable (err=%v)", err)
	}
	// The bucket comes back; the next save settles and clears the mark.
	if err := natsutil.SetupAll(fx.nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	if orch.inputsStore(ctx) == nil {
		t.Fatal("store not reopened after the bucket returned")
	}
	if err := orch.saveSnapshot(ctx, saved, "agent"); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}
	if again, err := fx.store.Load(ctx, "store-run"); err != nil || again.Steps["agent"].HasInputs {
		t.Fatalf("HasInputs still set after the store recovered (err=%v)", err)
	}
}

func TestContinueWait_WakeDoesNotApplyLoopDelay(t *testing.T) {
	cfg := dag.AgentLoopConfig{LoopDelay: 10 * time.Second}
	fx := newLoopFixture(t, loopDef("wake-delay-wf", "wake-delay-task", cfg, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "wake-delay-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
		waitPayload(t, protocol.ContinueWait{Input: true})))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.WaitInput })
	fx.submitInput(t, runID, "now")
	if next := fx.takeTask(t, 2*time.Second); next.Iteration != 1 {
		t.Fatalf("woken task iteration = %d, want 1", next.Iteration)
	}
}

// TestContinueWait_TwoEnginesOneTaskPerRound is the shared-consumer
// case: two engines on one WORKFLOW_HISTORY consumer, and in every
// round the continue that asks to wait for input and the input's own
// event are published back to back, so either engine may handle either
// first. Whichever interleaving happens, the step wakes exactly once.
func TestContinueWait_TwoEnginesOneTaskPerRound(t *testing.T) {
	fx := newLoopFixture(t, loopDef("two-eng-wf", "two-eng-task", dag.AgentLoopConfig{MaxIterations: 50}, nil))
	first := NewOrchestrator(fx.nc)
	first.Start()
	defer first.Stop()
	second := NewOrchestrator(fx.nc)
	second.Start()
	defer second.Stop()
	const runID = "two-eng-run"
	fx.startRun(t, runID)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := inputs.Open(ctx, fx.jsNew)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	task := fx.takeTask(t, 5*time.Second)
	for round := 0; round < 20; round++ {
		attempt := fx.started(t, task)
		requestID := fmt.Sprintf("round-%d", round)
		// The submission races the continue: in some rounds the step
		// enters its wait before the input exists and its event wakes
		// it; in others the input is there when the wait is entered and
		// the re-check sends the task. Exactly one task either way.
		var submitted sync.WaitGroup
		submitted.Add(1)
		go func() {
			defer submitted.Done()
			if _, _, err := store.Submit(ctx, inputs.Submission{
				RunID: runID, StepID: "agent", RequestID: requestID, Mode: inputs.ModeSteer,
				Content: json.RawMessage(`1`),
			}); err != nil {
				t.Errorf("Submit: %v", err)
				return
			}
			fx.publishEvent(t, protocol.NewInputQueuedEvent(runID, "agent", requestID))
		}()
		fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task, attempt,
			waitPayload(t, protocol.ContinueWait{Input: true})))
		submitted.Wait()
		task = fx.takeTask(t, 5*time.Second)
		if task.Iteration != round+1 {
			t.Fatalf("round %d: task iteration = %d, want %d", round, task.Iteration, round+1)
		}
		fx.noTask(t, 300*time.Millisecond)
		// The worker would place it; here it is withdrawn so the next
		// round's wait is real.
		if err := store.Withdraw(ctx, runID, "agent", requestID); err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
	}
}

func TestContinueWait_SweepSettlesAClaimWithoutARecord(t *testing.T) {
	fx := newLoopFixture(t, loopDef("sweep-claim-wf", "sweep-claim-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "sweep-claim-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	attempt := fx.started(t, task)
	store := fx.submitInput(t, runID, "claimed")
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool { return s.HasInputs })
	// A worker claimed it and died before appending the record.
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	entry, err := store.Get(ctx, runID, "agent", "claimed")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.Claim(ctx, entry, 0); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, task, attempt, []byte(`{}`)))
	fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted && !s.HasInputs
	})
	if got := fx.inputStatus(t, store, runID, "claimed"); got != inputs.StatusUnanswered {
		t.Errorf("unrecorded claim after completion = %s, want unanswered", got)
	}
}

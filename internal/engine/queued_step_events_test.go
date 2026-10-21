// internal/engine/queued_step_events_test.go
// Methodology: one orchestrator on an embedded NATS server, with the
// test standing in for the worker. In a cluster the engines share one
// WORKFLOW_HISTORY consumer, so the engine handling a worker's
// step.continue / step.completed / step.failed may load the step while
// it is still Queued: another engine is still applying the
// step.started. Each test publishes the lifecycle event before any
// step.started, so the step is Queued when it lands, and asserts the
// event is acted on exactly once when its dispatch nonce matches and
// dropped when it does not. A late step.started for a dispatch the
// step has already left must not move the step either.
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

// settleWait is how long a test waits for an event that must have no
// effect: the orchestrator applies an event in milliseconds.
const settleWait = 300 * time.Millisecond

func TestStepCompleted_BeforeStartedApplied_CompletesRun(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "early-wf", Version: "1",
		Steps: []dag.StepDef{{ID: "agent", Task: "early-task", Type: dag.StepTypeNormal}},
	}
	fx := newLoopFixture(t, def)
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "early-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)
	if task.DispatchNonce == "" {
		t.Fatalf("task carries no dispatch nonce: %+v", task)
	}

	// No fx.started: the step is still Queued when the completion lands.
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, task, 1, []byte(`"ok"`)))

	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted
	})
	step := run.Steps["agent"]
	if step.Status != dag.StepStatusCompleted || string(step.Output) != `"ok"` {
		t.Fatalf("step = %v output=%s, want completed with \"ok\"", step.Status, step.Output)
	}
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("run = %v, want completed", run.Status)
	}
}

func TestStepCompleted_WrongNonceOnQueuedStep_Dropped(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "wrong-wf", Version: "1",
		Steps: []dag.StepDef{{ID: "agent", Task: "wrong-task", Type: dag.StepTypeNormal}},
	}
	fx := newLoopFixture(t, def)
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "wrong-run"
	fx.startRun(t, runID)
	task := fx.takeTask(t, 5*time.Second)

	// Attempt 2 keeps the msg-id apart from the real completion's; on a
	// Queued step only the nonce is compared, so the nonce is what
	// rejects it.
	foreign := task
	foreign.DispatchNonce = "not-this-dispatch"
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, foreign, 2, []byte(`"no"`)))
	run := fx.waitStep(t, runID, settleWait, func(s dag.StepState) bool {
		return s.Status != dag.StepStatusQueued
	})
	if step := run.Steps["agent"]; step.Status != dag.StepStatusQueued || len(step.Output) != 0 {
		t.Fatalf("step = %v output=%s, want still queued with no output", step.Status, step.Output)
	}
	if run.Status == dag.RunStatusCompleted {
		t.Fatal("run completed from a completion naming another dispatch")
	}

	// The real worker's completion still lands.
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, task, 1, []byte(`"ok"`)))
	run = fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted
	})
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("run = %v, want completed", run.Status)
	}
}

func TestStepContinue_BeforeStartedApplied_AdvancesOnce(t *testing.T) {
	fx := newLoopFixture(t, loopDef("early-loop-wf", "early-loop-task", dag.AgentLoopConfig{}, nil))
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "early-loop-run"
	fx.startRun(t, runID)
	task0 := fx.takeTask(t, 5*time.Second)
	if task0.Iteration != 0 {
		t.Fatalf("first task iteration = %d, want 0", task0.Iteration)
	}

	// No fx.started: the loop's first iteration is still Queued.
	committed := []byte(`{"turn":1}`)
	fx.publishEvent(t, identityEvent(protocol.EventStepContinue, task0, 1, continuePayload(t, committed)))

	task1 := fx.takeTask(t, 5*time.Second)
	if task1.Iteration != 1 || string(task1.State) != string(committed) ||
		task1.DispatchNonce == task0.DispatchNonce {
		t.Fatalf("next task = iteration %d state %s nonce %q, want iteration 1 with the committed state and a fresh nonce",
			task1.Iteration, task1.State, task1.DispatchNonce)
	}
	fx.noTask(t, settleWait)
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Iterations == 1
	})
	if step := run.Steps["agent"]; step.Iterations != 1 || step.Attempts != 1 {
		t.Fatalf("step iterations=%d attempts=%d, want 1 and 1", step.Iterations, step.Attempts)
	}
}

func TestStepFailed_BeforeStartedApplied_RetriesOnce(t *testing.T) {
	// The policy's own backoff is far longer than the test, so the only
	// timer that fires is the one scheduled by hand below.
	retry := &dag.RetryPolicy{MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: time.Minute}
	wfDef := loopDef("early-fail-wf", "early-fail-task", dag.AgentLoopConfig{}, retry)
	fx := newLoopFixture(t, wfDef)
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "early-fail-run"
	fx.startRun(t, runID)
	task0 := fx.takeTask(t, 5*time.Second)

	// No fx.started: the step is still Queued when the failure lands.
	fx.publishEvent(t, identityEvent(protocol.EventStepFailed, task0, 1, failedPayload(t)))
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.RetryDispatchAt != nil
	})
	step := run.Steps["agent"]
	liveNonce := step.DispatchNonce
	if liveNonce == "" || liveNonce == task0.DispatchNonce {
		t.Fatalf("retry did not restamp the nonce: %q", liveNonce)
	}
	// The worker's attempt is adopted so the retry is attempt 2.
	if step.Attempts != 1 || step.Status != dag.StepStatusQueued {
		t.Fatalf("step attempts=%d status=%v, want 1 and queued", step.Attempts, step.Status)
	}

	// The failed dispatch's step.started arrives last: it names a
	// dispatch the step has left and must not mark the retry Running.
	fx.started(t, task0)
	run = fx.waitStep(t, runID, settleWait, func(s dag.StepState) bool {
		return s.Status != dag.StepStatusQueued
	})
	step = run.Steps["agent"]
	if step.Status != dag.StepStatusQueued || step.RetryDispatchAt == nil ||
		step.AttemptStartedAt != nil {
		t.Fatalf("late step.started moved the step: status=%v retry_at=%v started_at=%v",
			step.Status, step.RetryDispatchAt, step.AttemptStartedAt)
	}

	// A retry-after timer: the engine's own backoff timer owns the
	// retry_backoff msg-id for attempt 1, so a second one would dedup.
	stepDef := wfDef.Steps[0]
	err := orch.sleepTimer.Schedule(t.Context(), TimerMessage{
		Action: TimerActionRetryAfter, RunID: runID, StepID: "agent", DurationMs: 50,
		TaskType: stepDef.Task, Subject: orch.publisher.StepSubject(stepDef, runID),
		Attempt: 1, DispatchNonce: liveNonce,
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	retryTask := fx.takeTask(t, 5*time.Second)
	if retryTask.DispatchNonce != liveNonce || retryTask.Attempt != 2 {
		t.Fatalf("retry = nonce %q attempt %d, want nonce %q attempt 2",
			retryTask.DispatchNonce, retryTask.Attempt, liveNonce)
	}
	fx.noTask(t, settleWait)
	attempt := fx.started(t, retryTask)
	fx.publishEvent(t, identityEvent(protocol.EventStepCompleted, retryTask, attempt, []byte(`"done"`)))
	run = fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusCompleted
	})
	if step := run.Steps["agent"]; step.Status != dag.StepStatusCompleted || step.Attempts != 2 {
		t.Fatalf("step = %v attempts=%d, want completed on attempt 2", step.Status, step.Attempts)
	}
}

// TestStepFailed_LegacyBeforeStartedApplied_SchedulesRetry: a worker
// older than the identity fields sends step.failed with no identity
// and, on a first dispatch, no attempt number. Accepted while the step
// is still Queued (step.queued/started not applied yet, likely in a
// cluster), the step has Attempts 0; the retry path must still treat it
// as one attempt run instead of panicking in the backoff and wedging.
func TestStepFailed_LegacyBeforeStartedApplied_SchedulesRetry(t *testing.T) {
	retry := &dag.RetryPolicy{MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: time.Minute}
	wfDef := loopDef("legacy-fail-wf", "legacy-fail-task", dag.AgentLoopConfig{}, retry)
	fx := newLoopFixture(t, wfDef)
	orch := NewOrchestrator(fx.nc)
	orch.Start()
	defer orch.Stop()
	const runID = "legacy-fail-run"
	fx.startRun(t, runID)
	task0 := fx.takeTask(t, 5*time.Second)
	// Rewind the step to how another engine can still see it in a
	// cluster: dispatched (Queued) with step.queued not yet applied.
	ctx := t.Context()
	// Let this engine's own step.queued land first so nothing raises
	// the rewound count afterwards.
	run := fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.Attempts >= 1
	})
	queued := run.Steps["agent"]
	if queued.Status != dag.StepStatusQueued {
		t.Fatalf("step = %v, want queued", queued.Status)
	}
	queued.Attempts = 0
	run.Steps["agent"] = queued
	if err := orch.store.Save(ctx, run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	legacy := protocol.NewStepEvent(protocol.EventStepFailed, runID, "agent", failedPayload(t))
	if legacy.HasIdentity() || legacy.AttemptNumber != 0 {
		t.Fatalf("event must be legacy with no attempt: %+v", legacy)
	}
	fx.publishEvent(t, legacy)
	run = fx.waitStep(t, runID, 5*time.Second, func(s dag.StepState) bool {
		return s.RetryDispatchAt != nil
	})
	step := run.Steps["agent"]
	if step.Attempts != 1 {
		t.Fatalf("attempts = %d after a legacy failure, want 1", step.Attempts)
	}
	if step.DispatchNonce == task0.DispatchNonce || step.Status != dag.StepStatusQueued {
		t.Fatalf("retry not scheduled: status=%v nonce=%q", step.Status, step.DispatchNonce)
	}
}

// engine/deferral_attempt_test.go
// Methodology: a deferral of a dispatch that never reached a handler
// (rate limit, task concurrency limit, soft-sticky fallback) is not an
// attempt. Lifecycle tests run a real orchestrator on an embedded NATS
// server and play the worker by hand: a first run takes the step's one
// rate or concurrency slot so the second run's dispatch is deferred to
// SLEEP_TIMERS, then the test takes each task the second run gets off
// TASK_QUEUES, reads the attempt and dispatch nonce it carries, and
// answers under that identity. Timer-level tests seed a run snapshot,
// schedule one deferral timer and read the task it re-publishes.
// Assertions read the task payloads and the run snapshot.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
)

const (
	deferralTask   = "dfr-task"
	deferralBurnID = "dfr-burn"
	deferralRunID  = "dfr-run"
	deferralStepID = "dfr-step"
)

// deferralFixture is a started orchestrator with one single-step
// workflow registered, and a pull subscription on the step's tasks
// for deferralRunID.
type deferralFixture struct {
	js    nats.JetStreamContext
	orch  *Orchestrator
	def   []byte
	tasks *nats.Subscription
}

func newDeferralFixture(t *testing.T, step dag.StepDef) deferralFixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	wfDef := dag.WorkflowDef{Name: "dfr-wf", Version: "1", Steps: []dag.StepDef{step}}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)
	mustPut(t, defKV, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)), defData)
	orch := NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	tasks, err := js.PullSubscribe("task."+deferralTask+"."+deferralRunID, "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	return deferralFixture{js: js, orch: orch, def: defData, tasks: tasks}
}

// startDeferred starts the burn run, waits for its task so it holds
// the step's only slot, then starts deferralRunID, whose dispatch the
// engine must defer. It returns the burn run's task.
func (fx deferralFixture) startDeferred(t *testing.T) protocol.TaskPayload {
	t.Helper()
	burn, err := fx.js.PullSubscribe("task."+deferralTask+"."+deferralBurnID, "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe burn: %v", err)
	}
	fx.publish(t, protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, deferralBurnID, fx.def))
	msgs, err := burn.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("burn run's task never dispatched: %v", err)
	}
	fx.publish(t, protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, deferralRunID, fx.def))
	if task, ok := fx.take(t, 300*time.Millisecond); ok {
		t.Fatalf("dispatch was not deferred: got task %+v at once", task)
	}
	return decodeTask(t, msgs[0].Data)
}

func (fx deferralFixture) publish(t *testing.T, evt protocol.Event) {
	t.Helper()
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal %s: %v", evt.Type, err)
	}
	mustPublish(t, fx.js, evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
}

// take takes and acks the next task for deferralRunID, if one arrives
// within wait.
func (fx deferralFixture) take(t *testing.T, wait time.Duration) (protocol.TaskPayload, bool) {
	t.Helper()
	msgs, err := fx.tasks.Fetch(1, nats.MaxWait(wait))
	if err != nil || len(msgs) == 0 {
		return protocol.TaskPayload{}, false
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack task: %v", err)
	}
	return decodeTask(t, msgs[0].Data), true
}

// waitStep polls deferralRunID's step until pred holds.
func (fx deferralFixture) waitStep(t *testing.T, pred func(dag.StepState) bool) dag.StepState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last dag.StepState
	for time.Now().Before(deadline) {
		run, err := fx.orch.store.Load(context.Background(), deferralRunID)
		if err == nil {
			last = run.Steps[deferralStepID]
			if pred(last) {
				return last
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("step never reached the expected state; last %+v", last)
	return last
}

// assertDeferredRunsAsAttemptOne takes the deferred task, starts it,
// and checks the first execution is attempt 1 under the snapshot's
// dispatch nonce, leaving Attempts at 1.
func assertDeferredRunsAsAttemptOne(t *testing.T, fx deferralFixture) {
	t.Helper()
	task, ok := fx.take(t, 10*time.Second)
	if !ok {
		t.Fatal("deferred task never re-dispatched")
	}
	if task.Attempt != 1 {
		t.Fatalf("deferred first dispatch carried attempt %d, want 1", task.Attempt)
	}
	fx.publish(t, startedFor(task))
	state := fx.waitStep(t, func(s dag.StepState) bool {
		return s.Status == dag.StepStatusRunning
	})
	if state.Attempts != 1 {
		t.Fatalf("Attempts = %d after the deferred dispatch started, want 1", state.Attempts)
	}
	if task.DispatchNonce == "" || task.DispatchNonce != state.DispatchNonce {
		t.Fatalf("task nonce %q, snapshot nonce %q: want the dispatch's own",
			task.DispatchNonce, state.DispatchNonce)
	}
}

func rateLimitedStep() dag.StepDef {
	return dag.StepDef{
		ID: deferralStepID, Task: deferralTask, Type: dag.StepTypeNormal,
		RateLimit: &dag.RateLimit{Limit: 1, Period: time.Second},
	}
}

func TestDeferral_RateLimitedFirstDispatchRunsAsAttemptOne(t *testing.T) {
	fx := newDeferralFixture(t, rateLimitedStep())
	fx.startDeferred(t)
	assertDeferredRunsAsAttemptOne(t, fx)
}

func TestDeferral_ConcurrencyLimitedFirstDispatchRunsAsAttemptOne(t *testing.T) {
	fx := newDeferralFixture(t, dag.StepDef{
		ID: deferralStepID, Task: deferralTask, Type: dag.StepTypeNormal,
		MaxTaskConcurrency: 1,
	})
	burn := fx.startDeferred(t)
	// The deferred dispatch waits for the slot: the burn run frees it.
	fx.publish(t, startedFor(burn))
	fx.publish(t, identityEvent(protocol.EventStepCompleted, burn, burn.Attempt, nil))
	assertDeferredRunsAsAttemptOne(t, fx)
}

// TestDeferral_RetryBudgetCountsOnlyExecutions fails every execution
// of a rate-limited step whose first dispatch is deferred. Retries are
// re-published by their backoff timer, which does not consult the rate
// limiter, so the first dispatch is the one deferral a step can take.
// The deferral must not spend budget: the executions are attempts
// 1..MaxAttempts+1 (the budget check retries while the failed
// attempt's number is <= MaxAttempts), each a fresh dispatch nonce.
// Before the fix the deferred dispatch ran as attempt 2 and the step
// got one execution fewer. A step.completed under the first, superseded
// dispatch is sent mid-way and must be fenced off.
func TestDeferral_RetryBudgetCountsOnlyExecutions(t *testing.T) {
	step := rateLimitedStep()
	step.Retry = &dag.RetryPolicy{
		MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: 50 * time.Millisecond,
	}
	fx := newDeferralFixture(t, step)
	fx.startDeferred(t)
	var runs []protocol.TaskPayload
	wait := 10 * time.Second // the deferral, then the 50ms backoffs
	for len(runs) < 10 {
		task, ok := fx.take(t, wait)
		wait = 2 * time.Second
		if !ok {
			break
		}
		runs = append(runs, task)
		fx.publish(t, startedFor(task))
		if len(runs) == 2 {
			fx.publish(t, identityEvent(protocol.EventStepCompleted, runs[0],
				runs[0].Attempt, []byte(`"stale"`)))
		}
		fx.publish(t, identityEvent(protocol.EventStepFailed, task, task.Attempt,
			failedPayload(t)))
	}
	waitForRunStatus(t, fx.orch.store, deferralRunID, dag.RunStatusFailed, 5*time.Second)
	want := step.Retry.MaxAttempts + 1
	if len(runs) != want {
		t.Fatalf("step executed %d times, want %d: %+v", len(runs), want, runs)
	}
	seen := map[string]bool{}
	for i, task := range runs {
		if task.Attempt != i+1 {
			t.Fatalf("execution %d carried attempt %d, want %d", i+1, task.Attempt, i+1)
		}
		if task.DispatchNonce == "" || seen[task.DispatchNonce] {
			t.Fatalf("execution %d reused or lacked a nonce: %q", i+1, task.DispatchNonce)
		}
		seen[task.DispatchNonce] = true
	}
}

// timerFixture is a started SleepTimer on a fresh server with a
// subscription on deferralTask's subjects.
type timerFixture struct {
	js    jetstream.JetStream
	store *SnapshotStore
	timer *SleepTimer
	tasks *nats.Subscription
}

func newTimerFixture(t *testing.T) timerFixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	legacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	tasks, err := legacy.SubscribeSync("task."+deferralTask+".>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	timer := NewSleepTimer(nc, js, natsutil.NewTracingPublisher(nc, js))
	if err := timer.Start(); err != nil {
		t.Fatalf("SleepTimer.Start: %v", err)
	}
	t.Cleanup(timer.Stop)
	return timerFixture{js: js, store: NewSnapshotStore(js), timer: timer, tasks: tasks}
}

// seed saves a run whose step is in state.
func (fx timerFixture) seed(t *testing.T, state dag.StepState) {
	t.Helper()
	run := dag.WorkflowRun{
		RunID: deferralRunID, Steps: map[string]dag.StepState{deferralStepID: state},
	}
	if err := fx.store.Save(context.Background(), run); err != nil {
		t.Fatalf("seed Save: %v", err)
	}
}

// deferralTimer is the timer a rate-limited dispatch schedules.
func deferralTimer(attempt int, nonce string) TimerMessage {
	return TimerMessage{
		Action: TimerActionRateRetry, RunID: deferralRunID, StepID: deferralStepID,
		DurationMs: 50, TaskType: deferralTask, Input: json.RawMessage(`{}`),
		Attempt: attempt, DispatchNonce: nonce,
	}
}

// next returns the next re-published task within wait.
func (fx timerFixture) next(t *testing.T, wait time.Duration) (protocol.TaskPayload, bool) {
	t.Helper()
	msg, err := fx.tasks.NextMsg(wait)
	if err != nil {
		return protocol.TaskPayload{}, false
	}
	return decodeTask(t, msg.Data), true
}

// TestDeferral_DeferredRetryKeepsItsAttempt defers a genuine retry
// dispatched as attempt 2 (its step.queued already raised Attempts to
// 2): it must still run as attempt 2 under its own nonce, not 3.
func TestDeferral_DeferredRetryKeepsItsAttempt(t *testing.T) {
	fx := newTimerFixture(t)
	fx.seed(t, dag.StepState{
		Status: dag.StepStatusQueued, Attempts: 2, DispatchNonce: "nonce-retry-2",
	})
	if err := fx.timer.Schedule(context.Background(),
		deferralTimer(2, "nonce-retry-2")); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	task, ok := fx.next(t, 5*time.Second)
	if !ok {
		t.Fatal("deferred retry never re-published")
	}
	if task.Attempt != 2 {
		t.Fatalf("deferred retry carried attempt %d, want 2", task.Attempt)
	}
	if task.DispatchNonce != "nonce-retry-2" {
		t.Fatalf("deferred retry carried nonce %q, want nonce-retry-2", task.DispatchNonce)
	}
}

// TestDeferral_SupersededDeferralIsDropped fires a deferral timer for
// attempt 1 after the step already moved on to a queued attempt 2: the
// timer belongs to a dispatch the step left and must publish nothing.
func TestDeferral_SupersededDeferralIsDropped(t *testing.T) {
	fx := newTimerFixture(t)
	fx.seed(t, dag.StepState{
		Status: dag.StepStatusQueued, Attempts: 2, DispatchNonce: "nonce-retry-2",
	})
	if err := fx.timer.Schedule(context.Background(), deferralTimer(1, "")); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if task, ok := fx.next(t, time.Second); ok {
		t.Fatalf("superseded deferral re-published %+v", task)
	}
	if err := fx.timer.Schedule(context.Background(),
		deferralTimer(2, "nonce-retry-2")); err != nil {
		t.Fatalf("Schedule live: %v", err)
	}
	if _, ok := fx.next(t, 5*time.Second); !ok {
		t.Fatal("the live deferral never re-published")
	}
}

// TestDeferral_SoftStickyFallbackKeepsAttemptAndNonce dispatches a
// step's first attempt through a soft-sticky binding. Its step.queued
// has raised Attempts to 1, as it does before the 5s fallback fires;
// the fallback must re-publish attempt 1 under the original nonce.
func TestDeferral_SoftStickyFallbackKeepsAttemptAndNonce(t *testing.T) {
	fx := newTimerFixture(t)
	run := dag.WorkflowRun{
		RunID: deferralRunID,
		Steps: map[string]dag.StepState{deferralStepID: {
			Status: dag.StepStatusQueued, DispatchNonce: "nonce-sticky",
		}},
	}
	kv, err := fx.js.KeyValue(context.Background(), "sticky_bindings")
	if err != nil {
		t.Fatalf("sticky_bindings: %v", err)
	}
	enqueued, err := otel.Meter("test").Int64Counter("dfr_enqueued")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	router := NewStickyRouter(kv, fx.js, fx.timer.tp, fx.timer, otel.Tracer("test"), enqueued)
	step := dag.StepDef{ID: deferralStepID, Task: deferralTask, Type: dag.StepTypeNormal}
	err = router.PublishTask(context.Background(), deferralRunID, step, []byte(`{}`), run,
		"worker-gone", dag.StickySoft, "nonce-sticky", "dfr-wf",
		"task."+deferralTask+"."+deferralRunID, dispatchFirstAttempt)
	if err != nil {
		t.Fatalf("PublishTask: %v", err)
	}
	queued := run.Steps[deferralStepID]
	queued.Attempts = 1
	fx.seed(t, queued)
	task, ok := fx.next(t, 10*time.Second)
	if !ok {
		t.Fatal("soft-sticky fallback never re-published")
	}
	if task.Attempt != 1 || task.DispatchNonce != "nonce-sticky" {
		t.Fatalf("fallback carried attempt %d nonce %q, want 1 nonce-sticky",
			task.Attempt, task.DispatchNonce)
	}
}

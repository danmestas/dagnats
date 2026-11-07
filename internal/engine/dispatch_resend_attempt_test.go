// engine/dispatch_resend_attempt_test.go
// Methodology: the dispatch-crash fixture (real embedded NATS server,
// orchestrators built but not started, events handled through the
// conflict-retry entry point the history consumer uses). The engine
// dies right after saving t1 as dispatched, so t1 is left marked
// unsent; the step's own step.queued is then applied, as another engine
// in the cluster would apply it while the original send is in flight,
// and a later event triggers the "never sent" resend (#953). The test
// then plays the worker by hand: it takes t1's task off TASK_QUEUES,
// reads the attempt and dispatch nonce the task carries, and answers
// with step.started / step.completed / step.failed under that identity.
// Assertions read the task payloads and the run snapshot.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// queuedAfterCrash crashes fx's dispatch after its save, then applies
// t1's step.queued (attempt 1), leaving t1 Queued, counted as attempt
// 1 and still marked unsent with no task sent.
func queuedAfterCrash(t *testing.T, fx crashFixture) {
	t.Helper()
	handleCrashing(t, fx, checkpointSaved)
	queued := protocol.NewStepEvent(protocol.EventStepQueued, fx.run.RunID, "t1", nil)
	queued.AttemptNumber = 1
	if err := NewOrchestrator(fx.nc).handleEventWithConflictRetry(fx.ctx, queued); err != nil {
		t.Fatalf("handle step.queued(t1): %v", err)
	}
	run := loadCrashRun(t, fx)
	if t1 := run.Steps["t1"]; t1.Attempts != 1 || !t1.DispatchUnsent {
		t.Fatalf("t1 attempts=%d unsent=%v, want attempt 1 still unsent",
			t1.Attempts, t1.DispatchUnsent)
	}
}

// loadCrashRun loads fx's run snapshot.
func loadCrashRun(t *testing.T, fx crashFixture) dag.WorkflowRun {
	t.Helper()
	run, err := NewOrchestrator(fx.nc).store.Load(fx.ctx, fx.run.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return run
}

// takeT1Task takes the one t1 task waiting on TASK_QUEUES and acks it,
// as the worker that runs it would.
func takeT1Task(t *testing.T, fx crashFixture) protocol.TaskPayload {
	t.Helper()
	consumer, err := fx.js.CreateConsumer(fx.ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
		FilterSubject: "task.crash-t1." + fx.run.RunID,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("consumer on t1's subject: %v", err)
	}
	defer func() {
		name := consumer.CachedInfo().Name
		if err := fx.js.DeleteConsumer(fx.ctx, "TASK_QUEUES", name); err != nil {
			t.Errorf("delete consumer: %v", err)
		}
	}()
	batch, err := consumer.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch t1 tasks: %v", err)
	}
	var tasks []protocol.TaskPayload
	for msg := range batch.Messages() {
		tasks = append(tasks, decodeTask(t, msg.Data()))
		if err := msg.DoubleAck(fx.ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if len(tasks) != 1 {
		t.Fatalf("took %d t1 tasks, want exactly 1: %+v", len(tasks), tasks)
	}
	return tasks[0]
}

// decodeTask decodes a TASK_QUEUES message body.
func decodeTask(t *testing.T, data []byte) protocol.TaskPayload {
	t.Helper()
	var payload protocol.TaskPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	return payload
}

// handleCrashEvent handles evt on a fresh orchestrator.
func handleCrashEvent(t *testing.T, fx crashFixture, evt protocol.Event) {
	t.Helper()
	if err := NewOrchestrator(fx.nc).handleEventWithConflictRetry(fx.ctx, evt); err != nil {
		t.Fatalf("handle %s(%s): %v", evt.Type, evt.StepID, err)
	}
}

// startedFor is the step.started a worker sends on claiming task.
func startedFor(task protocol.TaskPayload) protocol.Event {
	evt := protocol.NewStepEvent(protocol.EventStepStarted, task.RunID, task.StepID, nil)
	evt.AttemptNumber = task.Attempt
	evt.DispatchNonce = task.DispatchNonce
	return evt
}

func TestDispatchResend_KeepsTheDispatchAttempt(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-resend-attempt")
	queuedAfterCrash(t, fx)
	saved := loadCrashRun(t, fx).Steps["t1"]

	// The redelivered step.completed(a) finds t1 still unsent: resend.
	handleCrashEvent(t, fx, fx.evt)
	task := takeT1Task(t, fx)
	if task.Attempt != 1 {
		t.Fatalf("resent task attempt = %d, want 1: a resend is not a new attempt", task.Attempt)
	}
	if task.DispatchNonce == "" || task.DispatchNonce != saved.DispatchNonce {
		t.Fatalf("resent task nonce = %q, want the saved dispatch's %q",
			task.DispatchNonce, saved.DispatchNonce)
	}

	handleCrashEvent(t, fx, startedFor(task))
	// A completion naming another dispatch of the step is still fenced.
	stale := task
	stale.DispatchNonce = "an-older-dispatch"
	handleCrashEvent(t, fx, identityEvent(protocol.EventStepCompleted, stale, 1, []byte(`"no"`)))
	if t1 := loadCrashRun(t, fx).Steps["t1"]; t1.Status != dag.StepStatusRunning {
		t.Fatalf("t1 = %v after a stale completion, want still running", t1.Status)
	}

	// The worker's completion under the resent identity is accepted.
	done := identityEvent(protocol.EventStepCompleted, task, task.Attempt, []byte(`"ok"`))
	handleCrashEvent(t, fx, done)
	t1 := loadCrashRun(t, fx).Steps["t1"]
	if t1.Status != dag.StepStatusCompleted || string(t1.Output) != `"ok"` {
		t.Fatalf("t1 = %v output %s, want completed with \"ok\"", t1.Status, t1.Output)
	}
	if t1.Attempts != 1 {
		t.Fatalf("t1 attempts = %d, want 1 for a step that ran once", t1.Attempts)
	}
}

func TestDispatchResend_SupersededAttemptIsFenced(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-resend-superseded")
	queuedAfterCrash(t, fx)
	handleCrashEvent(t, fx, fx.evt)
	task := takeT1Task(t, fx)
	handleCrashEvent(t, fx, startedFor(task))

	// Attempt 2 under the live nonce is what the pre-fix resend stamped;
	// the step is running attempt 1, so it names no live dispatch.
	superseded := identityEvent(protocol.EventStepCompleted, task, 2, []byte(`"no"`))
	handleCrashEvent(t, fx, superseded)
	t1 := loadCrashRun(t, fx).Steps["t1"]
	if t1.Status != dag.StepStatusRunning || len(t1.Output) != 0 {
		t.Fatalf("t1 = %v output %s, want still running with no output", t1.Status, t1.Output)
	}
	if t1.Attempts != 1 {
		t.Fatalf("t1 attempts = %d, want 1", t1.Attempts)
	}
}

func TestDispatchResend_StepQueuedDoesNotResendInFlightDispatch(t *testing.T) {
	fx := newCrashFixture(t, crashDef(), "run-resend-in-flight")
	queuedAfterCrash(t, fx)
	// The step.queued is published between the dispatch save and its
	// send, so it is not taken as a sign the send never happened.
	tasks := streamSubjectCounts(t, fx.ctx, fx.js, "TASK_QUEUES", "task.>")
	if len(tasks) != 0 {
		t.Fatalf("step.queued resent tasks %v, want none", tasks)
	}

	// The crash gap still heals: the reconciler resends t1 at attempt 1.
	NewOrchestrator(fx.nc).reconcileOneRun(fx.ctx, fx.run.RunID)
	task := takeT1Task(t, fx)
	if task.Attempt != 1 {
		t.Fatalf("reconciler resent attempt %d, want 1", task.Attempt)
	}
	if loadCrashRun(t, fx).Steps["t1"].DispatchUnsent {
		t.Fatal("t1 still marked unsent after the reconciler resent it")
	}
}

func TestDispatchResend_GenuineRetryIsANewAttempt(t *testing.T) {
	def := crashDef()
	for i := range def.Steps {
		if def.Steps[i].ID == "t1" {
			def.Steps[i].Retry = &dag.RetryPolicy{
				MaxAttempts: 3, Strategy: dag.RetryFixed, InitialDelay: 100 * time.Millisecond,
			}
		}
	}
	fx := newCrashFixture(t, def, "run-resend-retry")
	queuedAfterCrash(t, fx)
	handleCrashEvent(t, fx, fx.evt)
	first := takeT1Task(t, fx)
	handleCrashEvent(t, fx, startedFor(first))

	timers := NewSleepTimer(fx.nc, fx.js, natsutil.NewTracingPublisher(fx.nc, fx.js))
	if err := timers.Start(); err != nil {
		t.Fatalf("SleepTimer.Start: %v", err)
	}
	defer timers.Stop()
	handleCrashEvent(t, fx, identityEvent(protocol.EventStepFailed, first, first.Attempt,
		failedPayload(t)))

	retry := takeT1Task(t, fx)
	if retry.Attempt != 2 {
		t.Fatalf("retry attempt = %d, want 2", retry.Attempt)
	}
	if retry.DispatchNonce == "" || retry.DispatchNonce == first.DispatchNonce {
		t.Fatalf("retry nonce = %q, want a fresh one (first was %q)",
			retry.DispatchNonce, first.DispatchNonce)
	}
}

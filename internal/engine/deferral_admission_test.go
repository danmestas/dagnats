// engine/deferral_admission_test.go
// Methodology: a deferred dispatch (rate limit or task concurrency
// limit) must re-enter the same admission a fresh dispatch passes when
// its timer fires, and a deferred map instance must be re-sent under
// its own identity rather than dropped. Lifecycle tests run a real
// orchestrator on an embedded NATS server and play the worker by hand:
// the worker takes every task of the limited task type, holds each for
// a while before completing it, and records when it took it. A second
// task taken while the concurrency limit's worth are held is a breach,
// and so is a second task the rate limit's period did not allow.
// Timer-level tests seed a run snapshot, schedule one deferral timer
// and read what it re-publishes and what it logs.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

const (
	admitSrcTask  = "adm-src"
	admitItemTask = "adm-item"
	admitMapStep  = "fan"
)

// admitFixture is a started orchestrator with one workflow registered
// and a pull subscription on every task of admitItemTask.
type admitFixture struct {
	js    nats.JetStreamContext
	orch  *Orchestrator
	def   []byte
	items *nats.Subscription
}

func newAdmitFixture(t *testing.T, steps []dag.StepDef) admitFixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	wfDef := dag.WorkflowDef{Name: "adm-wf", Version: "1", Steps: steps}
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
	items, err := js.PullSubscribe("task."+admitItemTask+".>", "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	return admitFixture{js: js, orch: orch, def: defData, items: items}
}

func (fx admitFixture) publish(t *testing.T, evt protocol.Event) {
	t.Helper()
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal %s: %v", evt.Type, err)
	}
	mustPublish(t, fx.js, evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
}

func (fx admitFixture) startRun(t *testing.T, runID string) {
	t.Helper()
	fx.publish(t, protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, runID, fx.def))
}

// takenTask is a task the hand-played worker took, and when.
type takenTask struct {
	task protocol.TaskPayload
	at   time.Time
}

// playWorker takes tasks of admitItemTask until want have completed,
// holding each for hold before completing it. With limit > 0 it fails
// the test the moment a task arrives while limit tasks are held. It
// then waits quietly to prove no task arrives a second time.
func (fx admitFixture) playWorker(
	t *testing.T, want, limit int, hold time.Duration,
) []takenTask {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var taken, held []takenTask
	completed := 0
	for completed < want && time.Now().Before(deadline) {
		if task, ok := fx.takeItem(t, 50*time.Millisecond); ok {
			if limit > 0 && len(held) >= limit {
				t.Fatalf("task %s arrived while %d of limit %d were running",
					task.StepID, len(held), limit)
			}
			fx.publish(t, startedFor(task))
			entry := takenTask{task: task, at: time.Now()}
			taken = append(taken, entry)
			held = append(held, entry)
		}
		held, completed = fx.completeHeld(t, held, hold, completed)
	}
	if completed < want {
		t.Fatalf("only %d of %d tasks completed: taken %+v", completed, want, taken)
	}
	if task, ok := fx.takeItem(t, 2500*time.Millisecond); ok {
		t.Fatalf("task %s (attempt %d) arrived a second time", task.StepID, task.Attempt)
	}
	return taken
}

// completeHeld completes every held task held for at least hold.
func (fx admitFixture) completeHeld(
	t *testing.T, held []takenTask, hold time.Duration, completed int,
) ([]takenTask, int) {
	t.Helper()
	kept := held[:0]
	for _, entry := range held {
		if time.Since(entry.at) < hold {
			kept = append(kept, entry)
			continue
		}
		fx.publish(t, identityEvent(protocol.EventStepCompleted, entry.task,
			entry.task.Attempt, []byte(`"ok"`)))
		completed++
	}
	return kept, completed
}

func (fx admitFixture) takeItem(t *testing.T, wait time.Duration) (protocol.TaskPayload, bool) {
	t.Helper()
	msgs, err := fx.items.Fetch(1, nats.MaxWait(wait))
	if err != nil || len(msgs) == 0 {
		return protocol.TaskPayload{}, false
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack task: %v", err)
	}
	return decodeTask(t, msgs[0].Data), true
}

// mapSteps is src -> fan, a map step over src's output whose instances
// run admitItemTask under limit.
func mapSteps(limit func(*dag.StepDef)) []dag.StepDef {
	fan := dag.StepDef{
		ID: admitMapStep, Task: admitItemTask, Type: dag.StepTypeMap,
		DependsOn: []string{"src"},
		Config:    dag.MarshalConfig(&dag.MapConfig{MaxItems: 10}),
	}
	limit(&fan)
	return []dag.StepDef{
		{ID: "src", Task: admitSrcTask, Type: dag.StepTypeNormal},
		fan,
	}
}

// startMapRun starts runID and completes src with a three-item array,
// so the engine fans out three instances of admitMapStep.
func (fx admitFixture) startMapRun(t *testing.T, runID string) {
	t.Helper()
	src, err := fx.js.PullSubscribe("task."+admitSrcTask+"."+runID, "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe src: %v", err)
	}
	fx.startRun(t, runID)
	msgs, err := src.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("src task never dispatched: %v", err)
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack src: %v", err)
	}
	task := decodeTask(t, msgs[0].Data)
	fx.publish(t, startedFor(task))
	fx.publish(t, identityEvent(protocol.EventStepCompleted, task, task.Attempt,
		[]byte(`["a","b","c"]`)))
}

// assertEachInstanceOnce checks the three instances each ran once, as
// attempt 1 under a nonce of their own, and that the run completed.
func (fx admitFixture) assertEachInstanceOnce(
	t *testing.T, runID string, taken []takenTask,
) {
	t.Helper()
	seen := map[string]bool{}
	for _, entry := range taken {
		if seen[entry.task.StepID] {
			t.Fatalf("instance %s ran twice", entry.task.StepID)
		}
		seen[entry.task.StepID] = true
		if entry.task.Attempt != 1 || entry.task.DispatchNonce == "" {
			t.Fatalf("instance %s ran as attempt %d nonce %q, want attempt 1 with a nonce",
				entry.task.StepID, entry.task.Attempt, entry.task.DispatchNonce)
		}
	}
	for i := 0; i < 3; i++ {
		if !seen[mapInstanceID(admitMapStep, i)] {
			t.Fatalf("instance %d never ran: %+v", i, taken)
		}
	}
	waitForRunStatus(t, fx.orch.store, runID, dag.RunStatusCompleted, 10*time.Second)
}

// TestDeferral_RateLimitedMapRunsEveryInstanceOnce fans out three
// instances under a one-per-300ms rate limit: two are deferred, and
// each deferral must re-send its instance rather than drop it as a
// timer the snapshot does not know.
func TestDeferral_RateLimitedMapRunsEveryInstanceOnce(t *testing.T) {
	fx := newAdmitFixture(t, mapSteps(func(s *dag.StepDef) {
		s.RateLimit = &dag.RateLimit{Limit: 1, Period: 300 * time.Millisecond}
	}))
	fx.startMapRun(t, "adm-map-rate")
	taken := fx.playWorker(t, 3, 0, 0)
	fx.assertEachInstanceOnce(t, "adm-map-rate", taken)
}

// TestDeferral_ConcurrencyLimitedMapRunsInstancesOneAtATime fans out
// three instances under task concurrency 1: the deferred instances
// must run, one at a time, which also needs each instance's slot
// released when it completes.
func TestDeferral_ConcurrencyLimitedMapRunsInstancesOneAtATime(t *testing.T) {
	fx := newAdmitFixture(t, mapSteps(func(s *dag.StepDef) {
		s.MaxTaskConcurrency = 1
	}))
	fx.startMapRun(t, "adm-map-conc")
	taken := fx.playWorker(t, 3, 1, 1200*time.Millisecond)
	fx.assertEachInstanceOnce(t, "adm-map-conc", taken)
}

// TestDeferral_FailedMapFreesInstanceSlots fails the one admitted
// instance of a map under task concurrency 1, which fails the run, and
// starts a second run: its first instance gets the slot only if the
// failed run released its instances' slots.
func TestDeferral_FailedMapFreesInstanceSlots(t *testing.T) {
	fx := newAdmitFixture(t, mapSteps(func(s *dag.StepDef) {
		s.MaxTaskConcurrency = 1
	}))
	fx.startMapRun(t, "adm-map-fail")
	task, ok := fx.takeItem(t, 5*time.Second)
	if !ok {
		t.Fatal("no instance of the first run was dispatched")
	}
	fx.publish(t, identityEvent(protocol.EventStepFailed, task, task.Attempt,
		mustMarshal(t, protocol.StepFailedPayload{
			Error: "boom", FailureType: protocol.FailureTypeNonRetriable,
		})))
	waitForRunStatus(t, fx.orch.store, "adm-map-fail", dag.RunStatusFailed, 10*time.Second)
	fx.startMapRun(t, "adm-map-after")
	next, ok := fx.takeItem(t, 5*time.Second)
	if !ok {
		t.Fatal("the failed run kept its instance's slot: no task for the next run")
	}
	if next.RunID != "adm-map-after" {
		t.Fatalf("got a task of run %s after its map failed, want adm-map-after", next.RunID)
	}
}

func singleItemStep(limit func(*dag.StepDef)) []dag.StepDef {
	step := dag.StepDef{ID: "one", Task: admitItemTask, Type: dag.StepTypeNormal}
	limit(&step)
	return []dag.StepDef{step}
}

var admitRunIDs = []string{"adm-run-1", "adm-run-2", "adm-run-3"}

// TestDeferral_ConcurrencyDeferralWaitsForASlot starts three runs of a
// step limited to one task at a time. The worker holds each task past
// the deferral timer's 1s delay: a timer that re-sends without taking
// a slot hands the worker a second task while the first still runs.
func TestDeferral_ConcurrencyDeferralWaitsForASlot(t *testing.T) {
	fx := newAdmitFixture(t, singleItemStep(func(s *dag.StepDef) {
		s.MaxTaskConcurrency = 1
	}))
	for _, runID := range admitRunIDs {
		fx.startRun(t, runID)
	}
	taken := fx.playWorker(t, len(admitRunIDs), 1, 1500*time.Millisecond)
	for _, entry := range taken {
		if entry.task.Attempt != 1 {
			t.Fatalf("run %s ran as attempt %d, want 1", entry.task.RunID, entry.task.Attempt)
		}
	}
	for _, runID := range admitRunIDs {
		waitForRunStatus(t, fx.orch.store, runID, dag.RunStatusCompleted, 10*time.Second)
	}
}

// TestDeferral_RateDeferralWaitsForAToken starts three runs of a step
// limited to one dispatch a second. Two dispatches are deferred by the
// same full period; a timer that re-sends without asking the limiter
// again sends both at once.
func TestDeferral_RateDeferralWaitsForAToken(t *testing.T) {
	fx := newAdmitFixture(t, singleItemStep(func(s *dag.StepDef) {
		s.RateLimit = &dag.RateLimit{Limit: 1, Period: time.Second}
	}))
	for _, runID := range admitRunIDs {
		fx.startRun(t, runID)
	}
	taken := fx.playWorker(t, len(admitRunIDs), 0, 0)
	for i := 1; i < len(taken); i++ {
		gap := taken[i].at.Sub(taken[i-1].at)
		if gap < 600*time.Millisecond {
			t.Fatalf("dispatches %d and %d were %v apart under a 1/s limit",
				i, i+1, gap)
		}
	}
	for _, runID := range admitRunIDs {
		waitForRunStatus(t, fx.orch.store, runID, dag.RunStatusCompleted, 10*time.Second)
	}
}

// syncBuffer is a bytes.Buffer safe for the timer goroutine's logging.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureWarnings routes slog's default logger into a buffer for the
// rest of the test.
func captureWarnings(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// mapInstanceTimer is the deferral timer of instance 1 of deferralStepID.
func mapInstanceTimer(action TimerAction) TimerMessage {
	tm := deferralTimer(1, "nonce-instance-1")
	tm.Action = action
	tm.StepID = mapInstanceID(deferralStepID, 1)
	return tm
}

// seedMap saves a run whose map step deferralStepID has status and
// three instances in instanceStatus.
func (fx timerFixture) seedMap(t *testing.T, status, instanceStatus dag.StepStatus) {
	t.Helper()
	instances := make([]dag.MapInstanceState, 3)
	for i := range instances {
		instances[i].Status = instanceStatus
	}
	fx.seed(t, dag.StepState{Status: status, MapInstances: instances})
}

// TestDeferral_MapInstanceTimerKeepsInstanceIdentity fires a deferral
// timer for a queued map instance, which has no StepState of its own:
// it must re-send the instance as attempt 1 under the nonce it was
// dispatched with.
func TestDeferral_MapInstanceTimerKeepsInstanceIdentity(t *testing.T) {
	for _, action := range []TimerAction{TimerActionRateRetry, TimerActionTaskConcurRetry} {
		t.Run(string(action), func(t *testing.T) {
			fx := newTimerFixture(t)
			fx.seedMap(t, dag.StepStatusRunning, dag.StepStatusQueued)
			if err := fx.timer.Schedule(context.Background(),
				mapInstanceTimer(action)); err != nil {
				t.Fatalf("Schedule: %v", err)
			}
			task, ok := fx.next(t, 5*time.Second)
			if !ok {
				t.Fatal("deferred map instance never re-published")
			}
			if task.StepID != mapInstanceID(deferralStepID, 1) || task.Attempt != 1 {
				t.Fatalf("re-sent %s attempt %d, want instance 1 attempt 1",
					task.StepID, task.Attempt)
			}
			if task.DispatchNonce != "nonce-instance-1" {
				t.Fatalf("re-sent nonce %q, want nonce-instance-1", task.DispatchNonce)
			}
		})
	}
}

// TestDeferral_StaleTimerIsDroppedAndLogged fires deferral timers that
// no longer belong to a waiting dispatch: a step that already finished,
// and an instance of a map step that already finished. Each must
// publish nothing and log a warning naming the run, step and reason.
func TestDeferral_StaleTimerIsDroppedAndLogged(t *testing.T) {
	cases := []struct {
		name  string
		seed  func(timerFixture, *testing.T)
		timer TimerMessage
	}{
		{"step", func(fx timerFixture, t *testing.T) {
			fx.seed(t, dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1})
		}, deferralTimer(1, "nonce-done")},
		{"map instance", func(fx timerFixture, t *testing.T) {
			fx.seedMap(t, dag.StepStatusCompleted, dag.StepStatusCompleted)
		}, mapInstanceTimer(TimerActionRateRetry)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWarnings(t)
			fx := newTimerFixture(t)
			tc.seed(fx, t)
			if err := fx.timer.Schedule(context.Background(), tc.timer); err != nil {
				t.Fatalf("Schedule: %v", err)
			}
			if task, ok := fx.next(t, 1500*time.Millisecond); ok {
				t.Fatalf("stale timer re-published %+v", task)
			}
			assertDropLogged(t, logs, tc.timer)
		})
	}
}

func assertDropLogged(t *testing.T, logs *syncBuffer, tm TimerMessage) {
	t.Helper()
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, "dropping stale dispatch timer") {
			continue
		}
		for _, want := range []string{
			"level=WARN", "run_id=" + tm.RunID, "step_id=" + tm.StepID, "reason=",
		} {
			if !strings.Contains(line, want) {
				t.Fatalf("drop log %q lacks %q", line, want)
			}
		}
		return
	}
	t.Fatalf("no drop warning logged; logs:\n%s", logs.String())
}

// TestDeferral_RateTimerCarriesItsStage checks the timer a rate
// deferral schedules says so, which is what makes its fire ask the
// limiter again; the soft-sticky fallback, which also rides rate_retry,
// already passed admission and must not.
func TestDeferral_RateTimerCarriesItsStage(t *testing.T) {
	var tm TimerMessage
	if err := json.Unmarshal([]byte(`{"action":"rate_retry","run_id":"r","step_id":"s",`+
		`"rate_deferred":true}`), &tm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !tm.RateDeferred {
		t.Fatal("rate_deferred did not decode")
	}
	data, err := json.Marshal(TimerMessage{Action: TimerActionRateRetry, RunID: "r", StepID: "s"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "rate_deferred") {
		t.Fatalf("a timer that was not rate deferred encoded %s", data)
	}
}

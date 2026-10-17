// worker/step_timeout_redelivery_test.go
// Regression tests for issue #743: a redelivered task restarted the step
// watchdog, so a step whose worker kept losing the task never timed out.
// Each redelivery was numbered as a new attempt from NATS NumDelivered,
// and each new number armed a full timer while the earlier one was
// dropped as stale.
//
// Methodology: a real orchestrator and real JetStream. The first test
// stands in for a dying worker with a raw pull consumer that never acks,
// so JetStream redelivers the task; each delivery reports step.started
// numbered the way the worker numbers it (resolveAttemptNumber). The
// second publishes the lifecycle events directly and reads the armed
// timers back from SLEEP_TIMERS to check the remaining-time arithmetic,
// including that a retry starts a full timeout and that a leftover
// redelivery before the retry dispatches does not start its clock.
package worker

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startTimeoutEngine starts a NATS server and an orchestrator, and
// registers wfDef. It returns both JetStream clients and the def bytes.
func startTimeoutEngine(
	t *testing.T, wfDef dag.WorkflowDef,
) (nats.JetStreamContext, jetstream.JetStream, []byte) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
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
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	for _, key := range []string{
		wfDef.Name, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)),
	} {
		if _, err := defKV.Put(key, defData); err != nil {
			t.Fatalf("put def %q: %v", key, err)
		}
	}
	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	return js, jsNew, defData
}

// publishStepEvent publishes a lifecycle event for step "s".
func publishStepEvent(
	t *testing.T, js nats.JetStreamContext, eventType protocol.EventType,
	runID string, attempt int, payload any,
) {
	t.Helper()
	var data []byte
	if payload != nil {
		var err error
		if data, err = json.Marshal(payload); err != nil {
			t.Fatalf("marshal %s payload: %v", eventType, err)
		}
	}
	evt := protocol.NewStepEvent(eventType, runID, "s", data)
	evt.AttemptNumber = attempt
	raw, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal %s: %v", eventType, err)
	}
	if _, err := js.Publish(
		evt.NATSSubject(), raw, nats.MsgId(evt.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish %s: %v", eventType, err)
	}
}

// redeliverNeverAck reports step.started for every delivery and never
// acks, until ctx ends, counting deliveries in deliveries.
func redeliverNeverAck(
	ctx context.Context, t *testing.T, js nats.JetStreamContext,
	cons jetstream.Consumer, deliveries *atomic.Int32,
) {
	const fetchCountMax = 1000
	for i := 0; i < fetchCountMax && ctx.Err() == nil; i++ {
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil {
			continue
		}
		for msg := range batch.Messages() {
			var payload protocol.TaskPayload
			if err := json.Unmarshal(msg.Data(), &payload); err != nil {
				t.Errorf("decode task payload: %v", err)
				return
			}
			attempt, err := resolveAttemptNumber(payload.Attempt, msg)
			if err != nil {
				t.Errorf("resolve attempt: %v", err)
				return
			}
			deliveries.Add(1)
			evt := protocol.NewStepEvent(
				protocol.EventStepStarted, payload.RunID, payload.StepID, nil,
			)
			evt.AttemptNumber = attempt
			raw, err := evt.Marshal()
			if err != nil {
				t.Errorf("marshal step.started: %v", err)
				return
			}
			if _, err := js.Publish(
				evt.NATSSubject(), raw, nats.MsgId(evt.NATSMsgID()),
			); err != nil {
				t.Errorf("publish step.started: %v", err)
				return
			}
		}
	}
}

func TestStepTimeout_RedeliveredFirstAttemptStillTimesOut(t *testing.T) {
	const ackWait = 300 * time.Millisecond
	wfDef := dag.WorkflowDef{
		Name: "to-redeliver", Version: "1",
		Steps: []dag.StepDef{{
			ID: "s", Task: "to-redeliver-task",
			Type: dag.StepTypeNormal, Timeout: 5 * ackWait,
		}},
	}
	js, jsNew, defData := startTimeoutEngine(t, wfDef)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cons, err := jsNew.CreateOrUpdateConsumer(ctx, "TASK_QUEUES",
		jetstream.ConsumerConfig{
			Durable:       "redeliver-never-ack",
			FilterSubject: "task.to-redeliver-task.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       ackWait,
			MaxDeliver:    -1,
		})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	var deliveries atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		redeliverNeverAck(ctx, t, js, cons, &deliveries)
	}()
	defer func() { cancel(); <-done }()

	run := publishStartAndWaitTerminal(
		t, js, jsNew, "run-redeliver-1", defData,
		dag.RunStatusFailed, 10*time.Second,
	)
	step := run.Steps["s"]
	if !strings.Contains(strings.ToLower(step.Error), "timeout") {
		t.Fatalf("step.Error = %q, want a timeout", step.Error)
	}
	// The run failed on the timeout counted from the first start, not
	// from the last redelivery.
	if step.AttemptStartedAt == nil || step.CompletedAt == nil {
		t.Fatalf("step timestamps unset: %+v", step)
	}
	ran := step.CompletedAt.Sub(*step.AttemptStartedAt)
	if ran > wfDef.Steps[0].Timeout+2*time.Second {
		t.Fatalf("step ran %v before timing out, timeout %v",
			ran, wfDef.Steps[0].Timeout)
	}
	// Negative space: the task really was redelivered while it ran.
	if got := deliveries.Load(); got < 3 {
		t.Fatalf("deliveries = %d, want at least 3", got)
	}
}

// timerLog reads the timers armed in SLEEP_TIMERS, which keeps them
// (limits retention) after they fire, so the count only grows.
type timerLog struct {
	t      *testing.T
	stream jetstream.Stream
	count  uint64
}

func newTimerLog(t *testing.T, jsNew jetstream.JetStream) *timerLog {
	t.Helper()
	stream, err := jsNew.Stream(context.Background(), "SLEEP_TIMERS")
	if err != nil {
		t.Fatalf("SLEEP_TIMERS: %v", err)
	}
	return &timerLog{t: t, stream: stream}
}

// next waits for the next timer to be armed and returns it.
func (l *timerLog) next() enginepkg.TimerMessage {
	l.t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		info, err := l.stream.Info(ctx)
		if err != nil {
			l.t.Fatalf("SLEEP_TIMERS info: %v", err)
		}
		if info.State.Msgs > l.count {
			l.count++
			raw, err := l.stream.GetMsg(ctx, info.State.FirstSeq+l.count-1)
			if err != nil {
				l.t.Fatalf("get timer %d: %v", l.count, err)
			}
			var tm enginepkg.TimerMessage
			if err := json.Unmarshal(raw.Data, &tm); err != nil {
				l.t.Fatalf("decode timer: %v", err)
			}
			return tm
		}
		time.Sleep(50 * time.Millisecond)
	}
	l.t.Fatalf("no timer %d was armed", l.count+1)
	return enginepkg.TimerMessage{}
}

// watchdog waits for the next timer, asserts it is the step watchdog
// for attempt, and returns its delay.
func (l *timerLog) watchdog(attempt int) time.Duration {
	l.t.Helper()
	tm := l.next()
	if tm.Action != enginepkg.TimerActionStepTimeout || tm.Attempt != attempt {
		l.t.Fatalf("timer = %s attempt %d, want step_timeout attempt %d",
			tm.Action, tm.Attempt, attempt)
	}
	return time.Duration(tm.DurationMs) * time.Millisecond
}

// TestStepTimeout_RedeliveryArmsRemainderRetryArmsFull checks the timer
// arithmetic behind #743, for both kinds of retry: a redelivery re-arms
// the watchdog for what is left of the timeout, a leftover redelivery
// before the retry dispatches leaves the retry's clock unstarted, and
// the retry itself gets the whole timeout.
func TestStepTimeout_RedeliveryArmsRemainderRetryArmsFull(t *testing.T) {
	const timeout, retryDelay = time.Minute, time.Second
	failures := map[string]protocol.StepFailedPayload{
		"backoff": {Error: "boom", FailureType: protocol.FailureTypeRetriable},
		"retry-after": {
			Error: "busy", FailureType: protocol.FailureTypeRetryAfter,
			RetryAfterMs: retryDelay.Milliseconds(),
		},
	}
	for name, failure := range failures {
		t.Run(name, func(t *testing.T) {
			wfDef := dag.WorkflowDef{
				Name: "to-remainder", Version: "1",
				Steps: []dag.StepDef{{
					ID: "s", Task: "to-remainder-task",
					Type: dag.StepTypeNormal, Timeout: timeout,
					Retry: &dag.RetryPolicy{
						MaxAttempts: 5, Strategy: dag.RetryFixed,
						InitialDelay: retryDelay, MaxDelay: retryDelay,
					},
				}},
			}
			js, jsNew, defData := startTimeoutEngine(t, wfDef)
			runID := "run-remainder-" + name
			publishStartAndWaitTerminal(t, js, jsNew, runID, defData,
				dag.RunStatusRunning, 5*time.Second)
			assertRetryClock(t, js, jsNew, runID, failure, timeout, retryDelay)
		})
	}
}

// assertRetryClock drives one step through start, redelivery, failure,
// a leftover redelivery and the retry, checking each armed watchdog.
func assertRetryClock(
	t *testing.T, js nats.JetStreamContext, jsNew jetstream.JetStream,
	runID string, failure protocol.StepFailedPayload,
	timeout, retryDelay time.Duration,
) {
	t.Helper()
	timers := newTimerLog(t, jsNew)
	publishStepEvent(t, js, protocol.EventStepStarted, runID, 1, nil)
	// Positive: the first start arms the whole timeout.
	if got := timers.watchdog(1); got != timeout {
		t.Fatalf("first watchdog = %v, want %v", got, timeout)
	}
	time.Sleep(time.Second)
	publishStepEvent(t, js, protocol.EventStepStarted, runID, 2, nil)
	// Negative: a redelivery does not restart the clock.
	if got := timers.watchdog(2); got > timeout-time.Second ||
		got < timeout-10*time.Second {
		t.Fatalf("redelivery watchdog = %v, want about %v",
			got, timeout-time.Second)
	}

	publishStepEvent(t, js, protocol.EventStepFailed, runID, 2, failure)
	timers.next() // the retry's own timer
	publishStepEvent(t, js, protocol.EventStepStarted, runID, 3, nil)
	timers.watchdog(3) // the leftover gets a timer of its own
	run, err := enginepkg.NewSnapshotStore(jsNew).Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	// Negative: the leftover did not start the retry's clock.
	if started := run.Steps["s"].AttemptStartedAt; started != nil {
		t.Fatalf("leftover redelivery started the retry's clock at %v", started)
	}

	time.Sleep(retryDelay + 200*time.Millisecond) // the retry dispatches
	publishStepEvent(t, js, protocol.EventStepStarted, runID, 4, nil)
	// Positive: the retry is a new attempt with the whole timeout.
	if got := timers.watchdog(4); got != timeout {
		t.Fatalf("retry watchdog = %v, want %v", got, timeout)
	}
}

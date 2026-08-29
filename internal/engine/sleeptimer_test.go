// engine/sleeptimer_test.go
// Tests for the durable sleep timer. Uses real embedded NATS server.
// Methodology: schedule a short sleep timer, subscribe to the history
// stream, and verify that the sleep completion event fires within a
// bounded timeout. Also verifies dedup and correct event structure.
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

func TestSleepTimerFiresCompletion(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	st := NewSleepTimer(nc, js, natsutil.NewTracingPublisher(nc, js))
	if err := st.Start(); err != nil {
		t.Fatalf("SleepTimer.Start failed: %v", err)
	}
	defer st.Stop()

	// Subscribe to history.run-sleep-1 to catch the completion event.
	sub, err := jsLegacy.SubscribeSync(
		"history.run-sleep-1",
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}

	// Schedule a 100ms sleep.
	err = st.Schedule(context.Background(), TimerMessage{
		Action:     TimerActionSleepComplete,
		RunID:      "run-sleep-1",
		StepID:     "sleep-step",
		DurationMs: 100,
	})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Wait for the completion event (bounded 5s timeout).
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf(
			"did not receive sleep completion event: %v", err,
		)
	}

	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event failed: %v", err)
	}

	// Positive: event type is step.sleep.completed.
	if evt.Type != protocol.EventStepSleepCompleted {
		t.Fatalf(
			"expected event type %s, got %s",
			protocol.EventStepSleepCompleted, evt.Type,
		)
	}

	// Positive: step ID matches.
	if evt.StepID != "sleep-step" {
		t.Fatalf(
			"expected step ID 'sleep-step', got %q",
			evt.StepID,
		)
	}

	// Negative: run ID matches (not some other run).
	if evt.RunID != "run-sleep-1" {
		t.Fatalf(
			"expected run ID 'run-sleep-1', got %q",
			evt.RunID,
		)
	}
}

func TestSleepTimerDedupDuplicateSchedule(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	st := NewSleepTimer(nc, js, natsutil.NewTracingPublisher(nc, js))
	if err := st.Start(); err != nil {
		t.Fatalf("SleepTimer.Start failed: %v", err)
	}
	defer st.Stop()

	sub, err := jsLegacy.SubscribeSync(
		"history.run-dedup-1",
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}

	tmsg := TimerMessage{
		Action:     TimerActionSleepComplete,
		RunID:      "run-dedup-1",
		StepID:     "sleep-dup",
		DurationMs: 100,
	}

	// Schedule twice — second should be deduped.
	if err := st.Schedule(context.Background(), tmsg); err != nil {
		t.Fatalf("first Schedule failed: %v", err)
	}
	if err := st.Schedule(context.Background(), tmsg); err != nil {
		t.Fatalf("second Schedule failed: %v", err)
	}

	// Wait for exactly one completion event.
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("did not receive completion event: %v", err)
	}

	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Positive: got exactly one event of the right type.
	if evt.Type != protocol.EventStepSleepCompleted {
		t.Fatalf("wrong event type: %s", evt.Type)
	}

	// Negative: no second event should arrive.
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Fatal("expected no second event, but got one")
	}
}

func TestSleepTimerFiresRateRetry(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	st := NewSleepTimer(nc, js, natsutil.NewTracingPublisher(nc, js))
	if err := st.Start(); err != nil {
		t.Fatalf("SleepTimer.Start failed: %v", err)
	}
	defer st.Stop()

	// Subscribe to task.test-task.> to catch the re-published task.
	sub, err := jsLegacy.SubscribeSync(
		"task.test-task.>",
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}

	// Schedule a rate_retry timer with 100ms delay.
	err = st.Schedule(context.Background(), TimerMessage{
		Action:     TimerActionRateRetry,
		RunID:      "run-rate-1",
		StepID:     "step-rate-1",
		DurationMs: 100,
		TaskType:   "test-task",
		Input:      json.RawMessage(`{"key":"value"}`),
	})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Wait for the task message on task.test-task.run-rate-1.
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("did not receive task message: %v", err)
	}

	// Verify the message arrived on the correct subject.
	if msg.Subject != "task.test-task.run-rate-1" {
		t.Fatalf(
			"expected subject task.test-task.run-rate-1, got %s",
			msg.Subject,
		)
	}

	// Verify the payload is correct.
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload.RunID != "run-rate-1" {
		t.Fatalf("expected RunID run-rate-1, got %s", payload.RunID)
	}
	if payload.StepID != "step-rate-1" {
		t.Fatalf(
			"expected StepID step-rate-1, got %s", payload.StepID,
		)
	}
	if string(payload.Input) != `{"key":"value"}` {
		t.Fatalf(
			"expected Input {\"key\":\"value\"}, got %s",
			string(payload.Input),
		)
	}
}

func TestSleepTimerStartsLazilyOnSchedule(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	st := NewSleepTimer(nc, js, natsutil.NewTracingPublisher(nc, js))
	// Do NOT call Start() — Schedule should trigger it.

	// Subscribe to history.lazy-run to catch the completion event.
	sub, err := jsLegacy.SubscribeSync(
		"history.lazy-run",
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}

	err = st.Schedule(context.Background(), TimerMessage{
		Action:     TimerActionSleepComplete,
		RunID:      "lazy-run",
		StepID:     "lazy-step",
		DurationMs: 100,
	})
	if err != nil {
		t.Fatalf("Schedule before Start: %v", err)
	}

	// Wait for the completion event (bounded 5s timeout).
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf(
			"did not receive sleep completion event: %v", err,
		)
	}

	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	// Positive: event type is step.sleep.completed.
	if evt.Type != protocol.EventStepSleepCompleted {
		t.Fatalf(
			"expected event type %s, got %s",
			protocol.EventStepSleepCompleted, evt.Type,
		)
	}

	// Negative: timer fired even without explicit Start().
	if evt.StepID != "lazy-step" {
		t.Fatalf(
			"expected step ID 'lazy-step', got %q",
			evt.StepID,
		)
	}
}

// TestSleepTimerRateRetryDerivesAttemptFromSnapshot is the #624 review
// round-2 regression test: any re-dispatch through the rate/concurrency
// retry path (fireRateRetry) must derive TaskPayload.Attempt from the
// step's CURRENT run snapshot (via SnapshotStore), never hardcode or
// drop it. Before this fix fireRateRetry omitted Attempt entirely
// (always resolving to the zero value), so every rate-retried task ran
// as if it were attempt 1 (worker's resolveAttemptNumber NumDelivered
// fallback) regardless of how many real attempts the step already had
// — colliding with a genuine attempt 1's BUILD_LOGS subject and never
// matching the default `?attempt=` a consumer would look at.
func TestSleepTimerRateRetryDerivesAttemptFromSnapshot(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Seed a run snapshot whose step has already completed 3 attempts —
	// the rate-retry fire below must read THIS value, not trust
	// whatever (if anything) the TimerMessage itself carries.
	store := NewSnapshotStore(js)
	seedRun := dag.WorkflowRun{
		RunID: "run-rate-attempt",
		Steps: map[string]dag.StepState{
			"step-rate-attempt": {
				Status: dag.StepStatusRunning, Attempts: 3,
			},
		},
	}
	if err := store.Save(context.Background(), seedRun); err != nil {
		t.Fatalf("seed Save failed: %v", err)
	}

	st := NewSleepTimer(nc, js, natsutil.NewTracingPublisher(nc, js))
	if err := st.Start(); err != nil {
		t.Fatalf("SleepTimer.Start failed: %v", err)
	}
	defer st.Stop()

	sub, err := jsLegacy.SubscribeSync(
		"task.attempt-task.>", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}

	err = st.Schedule(context.Background(), TimerMessage{
		Action:     TimerActionRateRetry,
		RunID:      "run-rate-attempt",
		StepID:     "step-rate-attempt",
		DurationMs: 100,
		TaskType:   "attempt-task",
		Input:      json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("did not receive task message: %v", err)
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	// Positive: Attempts(3) + 1 — the snapshot tallies COMPLETED
	// attempts, so the next dispatch is one past that.
	if payload.Attempt != 4 {
		t.Fatalf("payload.Attempt = %d, want 4 (snapshot Attempts=3 + 1)",
			payload.Attempt)
	}
	// Negative: must not be the old hardcoded/dropped value.
	if payload.Attempt == 1 {
		t.Fatal("payload.Attempt = 1 — looks like the pre-fix hardcoded default")
	}
}

// engine/sleeptimer_test.go
// Tests for the durable sleep timer. Uses real embedded NATS server.
// Methodology: schedule a short sleep timer, subscribe to the history
// stream, and verify that the sleep completion event fires within a
// bounded timeout. Also verifies dedup and correct event structure.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestSleepTimerFiresCompletion(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	st := NewSleepTimer(nc, js)
	if err := st.Start(); err != nil {
		t.Fatalf("SleepTimer.Start failed: %v", err)
	}
	defer st.Stop()

	// Subscribe to history.run-sleep-1 to catch the completion event.
	sub, err := js.SubscribeSync(
		"history.run-sleep-1",
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}

	// Schedule a 100ms sleep.
	err = st.Schedule(TimerMessage{
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
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	st := NewSleepTimer(nc, js)
	if err := st.Start(); err != nil {
		t.Fatalf("SleepTimer.Start failed: %v", err)
	}
	defer st.Stop()

	sub, err := js.SubscribeSync(
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
	if err := st.Schedule(tmsg); err != nil {
		t.Fatalf("first Schedule failed: %v", err)
	}
	if err := st.Schedule(tmsg); err != nil {
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

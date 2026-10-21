// worker/step_started_nonce_test.go
// Methodology: a real worker on an embedded NATS server claims a task
// whose payload carries a dispatch nonce, then the history stream is
// read back. The step.started the worker publishes must carry that
// nonce (#814): the engine uses it to ignore a step.started for a
// dispatch the step has already left. Negative space: step.started is
// not fenced, so it must not carry a full identity (no iteration).
package worker

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestWorker_StepStartedCarriesDispatchNonce(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	var handlerCalled atomic.Bool
	w := NewWorker(nc)
	w.Handle("nonce-task", func(tc TaskContext) error {
		handlerCalled.Store(true)
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	const runID, nonce = "run-nonce-1", "dispatch-nonce-1"
	payload := protocol.TaskPayload{
		RunID: runID, StepID: "step-x", DispatchNonce: nonce,
		Input: json.RawMessage(`"go"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := js.Publish("task.nonce-task."+runID, data); err != nil {
		t.Fatalf("publish task: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for !handlerCalled.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	started := waitHistoryEvent(t, js, runID, protocol.EventStepStarted)
	if started.DispatchNonce != nonce {
		t.Fatalf("step.started dispatch_nonce = %q, want %q", started.DispatchNonce, nonce)
	}
	if started.HasIdentity() || started.AttemptNumber != 1 {
		t.Fatalf("step.started = attempt %d identity=%v, want attempt 1 without a fenced identity",
			started.AttemptNumber, started.HasIdentity())
	}
}

// waitHistoryEvent returns the first history event of wantType for
// runID, failing after a bounded wait.
func waitHistoryEvent(
	t *testing.T, js nats.JetStreamContext, runID string, wantType protocol.EventType,
) protocol.Event {
	t.Helper()
	if runID == "" {
		t.Fatal("waitHistoryEvent: runID must not be empty")
	}
	sub, err := js.SubscribeSync("history."+runID, nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe: %v", err)
		}
	}()
	const eventScanMax = 64
	for range eventScanMax {
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("no %s in history for %s: %v", wantType, runID, err)
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if evt.Type == wantType {
			return evt
		}
	}
	t.Fatalf("no %s within %d history events", wantType, eventScanMax)
	return protocol.Event{}
}

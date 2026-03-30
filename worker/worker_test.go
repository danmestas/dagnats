// worker/worker_test.go
// Tests for the Worker: handler registration and task consumption.
// Methodology: start embedded NATS, register a handler, publish a task message,
// verify the handler executes and a completion event appears on history.
package worker

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestWorkerHandlesTask(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	var called atomic.Bool
	w := NewWorker(nc, observe.NewNoopTelemetry())
	w.Handle("echo", func(ctx TaskContext) error {
		called.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()
	payload := protocol.TaskPayload{RunID: "run-1", StepID: "step-a", Input: json.RawMessage(`"hello"`)}
	data, _ := json.Marshal(payload)
	_, err = js.Publish("task.echo.run-1", data)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}
	sub, _ := js.SubscribeSync("history.run-1", nats.DeliverAll())
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf("event type = %q, want %q", evt.Type, protocol.EventStepCompleted)
	}
}

func TestWorkerNaksOnHandlerError(t *testing.T) {
	// Methodology: handler returns an error on the first call so the worker
	// NakWithDelay's the message. JetStream redelivers it; on the second call
	// the handler succeeds. We count invocations to confirm redelivery happened.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	var callCount atomic.Int32
	w := NewWorker(nc, observe.NewNoopTelemetry())
	w.Handle("failing", func(ctx TaskContext) error {
		n := callCount.Add(1)
		if n == 1 {
			return fmt.Errorf("transient error on attempt %d", n)
		}
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{RunID: "run-nak", StepID: "step-b", Input: json.RawMessage(`"data"`)}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish("task.failing.run-nak", data); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for handler to be called at least twice (first error, then success).
	deadline := time.After(15 * time.Second)
	for callCount.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("handler called %d time(s), want >= 2 within 15s", callCount.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Positive: redelivery happened (count >= 2).
	if callCount.Load() < 2 {
		t.Errorf("handler call count = %d, want >= 2", callCount.Load())
	}
	// Negative: handler was not called an unreasonable number of times.
	if callCount.Load() > 5 {
		t.Errorf("handler call count = %d, want <= 5 (unexpected loop)", callCount.Load())
	}
}

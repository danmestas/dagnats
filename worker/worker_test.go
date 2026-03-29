// worker/worker_test.go
// Tests for the Worker: handler registration and task consumption.
// Methodology: start embedded NATS, register a handler, publish a task message,
// verify the handler executes and a completion event appears on history.
package worker

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
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
	w := NewWorker(nc, observe.NewNoopLogger())
	w.Handle("echo", func(ctx TaskContext) error {
		called.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()
	payload := engine.TaskPayload{RunID: "run-1", StepID: "step-a", Input: json.RawMessage(`"hello"`)}
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
	var evt engine.Event
	json.Unmarshal(msg.Data, &evt)
	if evt.Type != engine.EventStepCompleted {
		t.Fatalf("event type = %q, want %q", evt.Type, engine.EventStepCompleted)
	}
}

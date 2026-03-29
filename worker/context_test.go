// worker/context_test.go
// Tests for TaskContext: the deep interface workers use to report results.
// Methodology: create a TaskContext with a real NATS connection, call Complete/Fail/Continue,
// and verify the correct events appear on the history stream.
package worker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/nats-io/nats.go"
)

func TestTaskContextComplete(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := js.SubscribeSync("history.run-1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	ctx := newTaskContext(js, protocol.TaskPayload{RunID: "run-1", StepID: "step-a", Input: []byte(`"input"`)})
	err = ctx.Complete([]byte(`"output"`))
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}
	var evt protocol.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("Unmarshal event failed: %v", err)
	}
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf("event type = %q, want %q", evt.Type, protocol.EventStepCompleted)
	}
	if evt.RunID != "run-1" {
		t.Fatalf("RunID = %q, want %q", evt.RunID, "run-1")
	}
	if evt.StepID != "step-a" {
		t.Fatalf("StepID = %q, want %q", evt.StepID, "step-a")
	}
}

func TestTaskContextFail(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := js.SubscribeSync("history.run-2", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	ctx := newTaskContext(js, protocol.TaskPayload{RunID: "run-2", StepID: "step-b"})
	err = ctx.Fail(fmt.Errorf("something broke"))
	if err != nil {
		t.Fatalf("Fail failed: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if evt.Type != protocol.EventStepFailed {
		t.Fatalf("event type = %q, want %q", evt.Type, protocol.EventStepFailed)
	}
}

func TestTaskContextContinue(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := js.SubscribeSync("history.run-3", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	ctx := newTaskContext(js, protocol.TaskPayload{RunID: "run-3", StepID: "step-c"})
	err = ctx.Continue([]byte(`"next input"`))
	if err != nil {
		t.Fatalf("Continue failed: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if evt.Type != protocol.EventStepContinue {
		t.Fatalf("event type = %q, want %q", evt.Type, protocol.EventStepContinue)
	}
}

func TestTaskContextInput(t *testing.T) {
	ctx := newTaskContext(nil, protocol.TaskPayload{RunID: "run-4", StepID: "step-d", Input: []byte(`"hello"`)})
	got := ctx.Input()
	if string(got) != `"hello"` {
		t.Fatalf("Input() = %q, want %q", string(got), `"hello"`)
	}
	if ctx.RunID() != "run-4" {
		t.Fatalf("RunID() = %q, want %q", ctx.RunID(), "run-4")
	}
	if ctx.StepID() != "step-d" {
		t.Fatalf("StepID() = %q, want %q", ctx.StepID(), "step-d")
	}
}

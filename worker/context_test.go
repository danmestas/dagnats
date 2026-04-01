// worker/context_test.go
// Tests for TaskContext: the deep interface workers use to report results.
// Methodology: create a TaskContext with a real NATS connection, call Complete/Fail/Continue,
// and verify the correct events appear on the history stream.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
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
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{
			RunID: "run-1", StepID: "step-a",
			Input: []byte(`"input"`),
		},
		bgCtx, span, &nats.Msg{}, nil, nil,
	)
	err = tc.Complete([]byte(`"output"`))
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
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{RunID: "run-2", StepID: "step-b"},
		bgCtx, span, &nats.Msg{}, nil, nil,
	)
	err = tc.Fail(fmt.Errorf("something broke"))
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
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{RunID: "run-3", StepID: "step-c"},
		bgCtx, span, &nats.Msg{}, nil, nil,
	)
	err = tc.Continue([]byte(`"next input"`))
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
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-4", StepID: "step-d",
			Input: []byte(`"hello"`),
		},
		bgCtx, span, nil, nil, nil,
	)
	got := tc.Input()
	if string(got) != `"hello"` {
		t.Fatalf(
			"Input() = %q, want %q", string(got), `"hello"`,
		)
	}
	if tc.RunID() != "run-4" {
		t.Fatalf(
			"RunID() = %q, want %q", tc.RunID(), "run-4",
		)
	}
	if tc.StepID() != "step-d" {
		t.Fatalf(
			"StepID() = %q, want %q", tc.StepID(), "step-d",
		)
	}
}

func TestTaskContextHeartbeat(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")

	// Positive: nil msg panics — catches programmer error
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil msg")
		}
	}()
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-hb", StepID: "step-hb",
		},
		bgCtx, span, nil, nil, nil,
	)
	// Negative: calling Heartbeat with nil msg must panic
	tc.Heartbeat()
}

func TestTaskContextCheckpoint(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "checkpoints"},
			natsutil.KVConfig{Bucket: "signals"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	cpKV, _ := js.KeyValue("checkpoints")
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := &taskContext{
		nc:           nc,
		js:           js,
		runID:        "run-cp",
		stepID:       "step-cp",
		tel:          tel,
		ctx:          bgCtx,
		span:         span,
		msg:          &nats.Msg{},
		checkpointKV: cpKV,
	}

	// Positive: checkpoint writes and reads back
	err := tc.Checkpoint([]byte(`{"progress":50}`))
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	data, err := tc.LoadCheckpoint()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	// Negative: wrong data fails
	if string(data) != `{"progress":50}` {
		t.Fatalf("checkpoint = %q, want progress 50", string(data))
	}
}

func TestTaskContextSignal(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "checkpoints"},
			natsutil.KVConfig{Bucket: "signals"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	sigKV, _ := js.KeyValue("signals")
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := &taskContext{
		nc:       nc,
		js:       js,
		runID:    "run-sig",
		stepID:   "step-sig",
		tel:      tel,
		ctx:      bgCtx,
		span:     span,
		signalKV: sigKV,
	}

	// Send signal in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		tc.SendSignal("run-sig", "approval", []byte(`"approved"`))
	}()

	// Positive: WaitForSignal receives it
	data, err := tc.WaitForSignal("approval", 2*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	// Negative: wrong data fails
	if string(data) != `"approved"` {
		t.Fatalf("signal = %q, want approved", string(data))
	}
}

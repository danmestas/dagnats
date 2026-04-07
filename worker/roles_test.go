// worker/roles_test.go
// Tests for role-based TaskContext interfaces and typed handler
// registration. Compile-time interface satisfaction checks verify
// the existing taskContext struct implements all narrower roles
// without modification. Integration tests use real embedded NATS
// to verify Handle* convenience wrappers dispatch correctly.
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

// Compile-time checks: taskContext must satisfy every role interface.
// If any method is missing, the build fails here — no runtime needed.
var (
	_ SimpleTask     = (*taskContext)(nil)
	_ CheckpointTask = (*taskContext)(nil)
	_ LoopTask       = (*taskContext)(nil)
	_ StreamTask     = (*taskContext)(nil)
	_ SignalTask     = (*taskContext)(nil)
	_ TaskContext    = (*taskContext)(nil)
)

func TestTaskContextImplementsSimpleTask(t *testing.T) {
	// Compile-time guarantee via the package-level var block above.
	// This test documents the intent explicitly for test output.
	var _ SimpleTask = (*taskContext)(nil)

	// Positive: assignment compiles — interface is satisfied.
	// Negative: TaskContext (the full union) is a superset.
	var _ TaskContext = (*taskContext)(nil)
}

func TestTaskContextImplementsAllRoles(t *testing.T) {
	// Each role interface is a subset of TaskContext. The concrete
	// taskContext must satisfy all of them without modification.
	var _ SimpleTask = (*taskContext)(nil)
	var _ CheckpointTask = (*taskContext)(nil)
	var _ LoopTask = (*taskContext)(nil)
	var _ StreamTask = (*taskContext)(nil)
	var _ SignalTask = (*taskContext)(nil)

	// Positive: all five compile.
	// Negative: TaskContext union still compiles too.
	var _ TaskContext = (*taskContext)(nil)
}

func TestHandleSimpleRegistersAndExecutes(t *testing.T) {
	// Integration: register via HandleSimple, publish a task,
	// verify handler is called and can Complete.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	var called atomic.Bool
	w := NewWorker(nc)
	HandleSimple(w, "simple-echo", func(ctx SimpleTask) error {
		called.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-simple",
		StepID: "step-s",
		Input:  json.RawMessage(`"hello-simple"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(
		"task.simple-echo.run-simple", data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Positive: completion event appears in history.
	sub, err := js.SubscribeSync(
		"history.run-simple", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf(
			"event type = %q, want %q",
			evt.Type, protocol.EventStepCompleted,
		)
	}
}

func TestHandleLoopCanContinue(t *testing.T) {
	// Integration: register via HandleLoop, verify Continue works.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	var called atomic.Bool
	w := NewWorker(nc)
	HandleLoop(w, "loop-task", func(ctx LoopTask) error {
		called.Store(true)
		return ctx.Continue([]byte(`"next"`))
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-loop",
		StepID: "step-l",
		Input:  json.RawMessage(`"start"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(
		"task.loop-task.run-loop", data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Positive: step.continue event appears in history.
	sub, err := js.SubscribeSync(
		"history.run-loop", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Negative: event is continue, not completed.
	if evt.Type != protocol.EventStepContinue {
		t.Fatalf(
			"event type = %q, want %q",
			evt.Type, protocol.EventStepContinue,
		)
	}
}

func TestHandleCheckpointRegistersAndExecutes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	var called atomic.Bool
	w := NewWorker(nc)
	HandleCheckpoint(
		w, "cp-task",
		func(ctx CheckpointTask) error {
			called.Store(true)
			// LoadCheckpoint returns (nil, nil) when KV is absent,
			// so it exercises the interface without requiring the
			// optional checkpoints bucket.
			_, err := ctx.LoadCheckpoint()
			if err != nil {
				return err
			}
			return ctx.Complete(ctx.Input())
		},
	)
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-cp",
		StepID: "step-cp",
		Input:  json.RawMessage(`"cp-data"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(
		"task.cp-task.run-cp", data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Positive: handler was called.
	if !called.Load() {
		t.Fatal("handler not called")
	}
	// Negative: completion event appears.
	sub, err := js.SubscribeSync(
		"history.run-cp", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf(
			"event = %q, want %q",
			evt.Type, protocol.EventStepCompleted,
		)
	}
}

func TestHandleStreamRegistersAndExecutes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	var called atomic.Bool
	w := NewWorker(nc)
	HandleStream(
		w, "stream-task",
		func(ctx StreamTask) error {
			called.Store(true)
			if err := ctx.PutStream([]byte("token")); err != nil {
				return err
			}
			return ctx.Complete(ctx.Input())
		},
	)
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-st",
		StepID: "step-st",
		Input:  json.RawMessage(`"st-data"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(
		"task.stream-task.run-st", data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Positive: handler was called.
	if !called.Load() {
		t.Fatal("handler not called")
	}
	// Negative: completion event appears.
	sub, err := js.SubscribeSync(
		"history.run-st", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf(
			"event = %q, want %q",
			evt.Type, protocol.EventStepCompleted,
		)
	}
}

func TestHandleSignalRegistersAndExecutes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	var called atomic.Bool
	w := NewWorker(nc)
	HandleSignal(
		w, "signal-task",
		func(ctx SignalTask) error {
			called.Store(true)
			return ctx.Complete(ctx.Input())
		},
	)
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-sig",
		StepID: "step-sig",
		Input:  json.RawMessage(`"sig-data"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(
		"task.signal-task.run-sig", data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Positive: handler was called.
	if !called.Load() {
		t.Fatal("handler not called")
	}
	// Negative: completion event appears.
	sub, err := js.SubscribeSync(
		"history.run-sig", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf(
			"event = %q, want %q",
			evt.Type, protocol.EventStepCompleted,
		)
	}
}

func TestHandleSimplePanicsOnNilHandler(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc)
	defer func() {
		r := recover()
		// Positive: panics on nil handler.
		if r == nil {
			t.Fatal("expected panic for nil handler")
		}
	}()
	HandleSimple(w, "t", nil)
}

func TestHandleSimplePanicsOnEmptyTaskType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc)
	defer func() {
		r := recover()
		// Positive: panics on empty taskType.
		if r == nil {
			t.Fatal("expected panic for empty taskType")
		}
	}()
	HandleSimple(w, "", func(ctx SimpleTask) error {
		return nil
	})
}

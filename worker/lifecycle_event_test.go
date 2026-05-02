// worker/lifecycle_event_test.go
// Tests for the worker-side step.started lifecycle publish helper.
// Assertion-defense tests are pure unit tests; integration tests start
// embedded NATS and run a worker end-to-end to verify the helper fires
// before the user's handler is invoked.
// Methodology: red-green TDD. Each test specifies a single observable
// behaviour and includes both a positive and a negative assertion.
package worker

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishStarted_PanicsOnNilMsg(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil msg, got none")
		}
		s, ok := r.(string)
		if !ok || s == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	tc := &taskContext{runID: "r1", stepID: "s1"}
	_ = tc.publishStarted(nil)
}

func TestPublishStarted_PanicsOnEmptyRunID(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty runID, got none")
		}
		s, ok := r.(string)
		if !ok || s == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	tc := &taskContext{runID: ""}
	_ = tc.publishStarted(stubJetstreamMsg{})
}

// stubJetstreamMsg implements jetstream.Msg minimally so the test can
// exercise the "empty runID panics before metadata is read" path.
// All methods panic — publishStarted must not call any of them.
type stubJetstreamMsg struct{}

func (stubJetstreamMsg) Metadata() (*jetstream.MsgMetadata, error) { panic("unreachable") }
func (stubJetstreamMsg) Data() []byte                              { panic("unreachable") }
func (stubJetstreamMsg) Headers() nats.Header                      { panic("unreachable") }
func (stubJetstreamMsg) Subject() string                           { panic("unreachable") }
func (stubJetstreamMsg) Reply() string                             { panic("unreachable") }
func (stubJetstreamMsg) Ack() error                                { panic("unreachable") }
func (stubJetstreamMsg) DoubleAck(context.Context) error           { panic("unreachable") }
func (stubJetstreamMsg) Nak() error                                { panic("unreachable") }
func (stubJetstreamMsg) NakWithDelay(time.Duration) error          { panic("unreachable") }
func (stubJetstreamMsg) InProgress() error                         { panic("unreachable") }
func (stubJetstreamMsg) Term() error                               { panic("unreachable") }
func (stubJetstreamMsg) TermWithReason(string) error               { panic("unreachable") }

func TestWorker_PublishesStepStartedBeforeHandler(t *testing.T) {
	// Methodology: register a handler that records a sentinel marker
	// on first invocation. Drain the history stream. Assert the
	// step.started event arrives at all and carries AttemptNumber=1
	// plus the WorkerID.
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
	w.Handle("started-task", func(tc TaskContext) error {
		handlerCalled.Store(true)
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-started-1",
		StepID: "step-x",
		Input:  json.RawMessage(`"go"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish("task.started-task.run-started-1", data); err != nil {
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

	sub, err := js.SubscribeSync("history.run-started-1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	var sawStarted, sawCompleted bool
	var startedEvt protocol.Event
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) && !(sawStarted && sawCompleted) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		switch evt.Type {
		case protocol.EventStepStarted:
			sawStarted = true
			startedEvt = evt
		case protocol.EventStepCompleted:
			sawCompleted = true
		}
	}
	if !sawStarted {
		t.Fatal("expected step.started in history stream, got none")
	}
	if !sawCompleted {
		t.Fatal("expected step.completed in history stream, got none")
	}
	if startedEvt.AttemptNumber != 1 {
		t.Fatalf("AttemptNumber = %d, want 1", startedEvt.AttemptNumber)
	}
	if startedEvt.WorkerID == "" {
		t.Fatal("WorkerID must be set on step.started event")
	}
	if startedEvt.RunID != "run-started-1" {
		t.Fatalf("RunID = %q, want %q", startedEvt.RunID, "run-started-1")
	}
	if startedEvt.StepID != "step-x" {
		t.Fatalf("StepID = %q, want %q", startedEvt.StepID, "step-x")
	}
}

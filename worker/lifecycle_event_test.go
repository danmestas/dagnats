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
	"fmt"
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

func TestWorker_PublishStartedFailure_NaksAndRetries(t *testing.T) {
	// Methodology: inject a publishMsgFunc that fails the FIRST
	// publishStarted call and delegates to the real js.PublishMsg
	// thereafter. Worker must NAK the original task message on the
	// first delivery (handler NEVER invoked) and the retry must
	// succeed (handler invoked exactly once, completion lands in
	// history). Proves NAK-and-recover semantics without racy
	// connection-close trickery — the seam is a function pointer,
	// chosen over an interface because publishStarted needs exactly
	// one method (Ousterhout: minimum interface, deepest module).
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	jsCtx, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	// Fail only the FIRST publishStarted call; subsequent calls hit
	// the real broker so the retry can succeed. The seam is wired
	// only into publishStarted (Complete/Fail still use c.js.PublishMsg
	// directly), so every invocation here is a step.started publish —
	// no subject filtering needed.
	var startedCalls atomic.Int32
	injected := func(
		ctx context.Context, m *nats.Msg,
		opts ...jetstream.PublishOpt,
	) (*jetstream.PubAck, error) {
		if startedCalls.Add(1) == 1 {
			return nil, fmt.Errorf("injected publish failure")
		}
		return jsCtx.PublishMsg(ctx, m, opts...)
	}

	var handlerCalls atomic.Int32
	w := NewWorker(nc, withPublishMsgFunc(injected))
	w.Handle("nak-recover", func(tc TaskContext) error {
		handlerCalls.Add(1)
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-nak-1",
		StepID: "step-nak",
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := js.Publish("task.nak-recover.run-nak-1", data); err != nil {
		t.Fatalf("publish task: %v", err)
	}

	sub, err := js.SubscribeSync("history.run-nak-1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	var sawStartedAttempt2, sawCompleted bool
	for time.Now().Before(deadline) && !(sawStartedAttempt2 && sawCompleted) {
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
			if evt.AttemptNumber == 2 {
				sawStartedAttempt2 = true
			}
		case protocol.EventStepCompleted:
			sawCompleted = true
		}
	}

	// Positive assertions: redelivery published step.started with
	// AttemptNumber=2, the handler ran on the retry, completion
	// landed in history.
	if !sawStartedAttempt2 {
		t.Fatal("expected step.started with AttemptNumber=2 in history, got none")
	}
	if !sawCompleted {
		t.Fatal("expected step.completed in history, got none")
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler invoked %d times, want exactly 1 (NAK precedes handler)", got)
	}
	// Negative assertion: the seam observed at least 2 step.started
	// publishes (one rejected, one succeeded). Otherwise the test
	// would be vacuously green — the recovery path was never traversed.
	if got := startedCalls.Load(); got < 2 {
		t.Fatalf("seam saw %d step.started publishes, want >= 2 (failure + retry)", got)
	}
}

func TestWorker_AttemptNumberFromEngineRetry(t *testing.T) {
	// Methodology: handler errors on the first call, succeeds on the
	// second. The engine drives the retry loop (issue #141 fix); each
	// re-dispatch carries payload.Attempt = previous+1 so step.started
	// fires with the correct AttemptNumber. Both step.started events
	// must appear in the history stream with distinct AttemptNumber
	// values, proving payload.Attempt → c.attempt → AttemptNumber.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "lifecycle-attempt", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  2,
			Strategy:     dag.RetryFixed,
			InitialDelay: 100 * time.Millisecond,
		},
		Steps: []dag.StepDef{
			{ID: "step-r", Task: "retry-attempt", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	var calls atomic.Int32
	w := NewWorker(nc)
	w.Handle("retry-attempt", func(tc TaskContext) error {
		n := calls.Add(1)
		if n == 1 {
			return fmt.Errorf("transient error attempt %d", n)
		}
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-retry-1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	deadline := time.After(15 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("calls = %d, want 2 within 15s", calls.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}

	sub, err := js.SubscribeSync("history.run-retry-1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	attemptsSeen := make(map[int]bool)
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) && !(attemptsSeen[1] && attemptsSeen[2]) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if evt.Type == protocol.EventStepStarted {
			attemptsSeen[evt.AttemptNumber] = true
		}
	}
	if !attemptsSeen[1] {
		t.Fatal("expected step.started with AttemptNumber=1, missing")
	}
	if !attemptsSeen[2] {
		t.Fatal("expected step.started with AttemptNumber=2, missing")
	}
}

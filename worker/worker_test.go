// worker/worker_test.go
// Tests for the Worker: handler registration and task consumption.
// Methodology: start embedded NATS, register a handler, publish a task message,
// verify the handler executes and a completion event appears on history.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
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
	w := NewWorker(nc)
	w.Handle("echo", func(ctx TaskContext) error {
		called.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()
	payload := protocol.TaskPayload{
		RunID:  "run-1",
		StepID: "step-a",
		Input:  json.RawMessage(`"hello"`),
	}
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
	evt = nextEventOfType(t, sub, protocol.EventStepCompleted, 5*time.Second, evt)
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf("event type = %q, want %q", evt.Type, protocol.EventStepCompleted)
	}
}

// nextEventOfType drains events from sub until one of type want is found
// (or timeout elapses). The first event peeked by the caller is passed in
// as `first`; if it already matches, it is returned unchanged. Otherwise
// the helper consumes additional messages until match or timeout. On
// timeout it calls t.Fatalf with the last observed type. Used by tests
// that previously assumed step.completed was the first event published —
// since Task 4 of #137, step.started precedes the terminal event.
func nextEventOfType(
	t *testing.T,
	sub *nats.Subscription,
	want protocol.EventType,
	timeout time.Duration,
	first protocol.Event,
) protocol.Event {
	t.Helper()
	if first.Type == want {
		return first
	}
	deadline := time.Now().Add(timeout)
	last := first.Type
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("nextEventOfType: Unmarshal: %v", err)
		}
		last = evt.Type
		if evt.Type == want {
			return evt
		}
	}
	t.Fatalf("nextEventOfType: timed out waiting for %q, last seen %q",
		want, last)
	return protocol.Event{}
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
	w := NewWorker(nc)
	w.Handle("failing", func(ctx TaskContext) error {
		n := callCount.Add(1)
		if n == 1 {
			return fmt.Errorf("transient error on attempt %d", n)
		}
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-nak",
		StepID: "step-b",
		Input:  json.RawMessage(`"data"`),
	}
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

func TestWorkerWithGroupsOnlyHandlesGroupTasks(t *testing.T) {
	// Methodology: create a worker with groups=["gpu"], publish tasks to both
	// the gpu-specific subject and a non-group subject. Verify only the gpu
	// task is handled.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	var gpuCalled atomic.Bool
	w := NewWorker(nc, WithGroups("gpu"))
	w.Handle("ml-training", func(ctx TaskContext) error {
		gpuCalled.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	// Publish task to gpu-specific subject
	gpuPayload := protocol.TaskPayload{
		RunID:  "run-gpu",
		StepID: "train",
		Input:  json.RawMessage(`"gpu-data"`),
	}
	gpuData, _ := json.Marshal(gpuPayload)
	if _, err := js.Publish("task.ml-training.gpu.run-gpu", gpuData); err != nil {
		t.Fatalf("Publish gpu task failed: %v", err)
	}

	// Publish task to non-group subject (should be ignored)
	generalPayload := protocol.TaskPayload{
		RunID:  "run-general",
		StepID: "train",
		Input:  json.RawMessage(`"general-data"`),
	}
	generalData, _ := json.Marshal(generalPayload)
	if _, err := js.Publish("task.ml-training.run-general", generalData); err != nil {
		t.Fatalf("Publish general task failed: %v", err)
	}

	// Positive: GPU task should be handled
	deadline := time.After(5 * time.Second)
	for !gpuCalled.Load() {
		select {
		case <-deadline:
			t.Fatal("gpu handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Negative: general task completion should NOT appear in history
	generalSub, _ := js.SubscribeSync("history.run-general", nats.DeliverAll())
	_, err = generalSub.NextMsg(1 * time.Second)
	if err == nil {
		t.Fatal("general task should not be handled by gpu worker")
	}
}

func TestHandlePanicsOnEmptyTaskType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc)
	defer func() {
		r := recover()
		// Positive: panics on empty taskType
		if r == nil {
			t.Fatal("expected panic for empty taskType")
		}
		msg := fmt.Sprintf("%v", r)
		// Negative: panic message is specific
		if msg != "Worker.Handle: taskType must not be empty" {
			t.Fatalf("panic = %q, want taskType message", msg)
		}
	}()
	w.Handle("", func(ctx TaskContext) error { return nil })
}

func TestHandlePanicsOnNilHandler(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc)
	defer func() {
		r := recover()
		// Positive: panics on nil handler
		if r == nil {
			t.Fatal("expected panic for nil handler")
		}
		msg := fmt.Sprintf("%v", r)
		// Negative: message mentions handler
		if msg != "Worker.Handle: handler must not be nil" {
			t.Fatalf("panic = %q, want handler message", msg)
		}
	}()
	w.Handle("valid-type", nil)
}

func TestHandleRegistersHandler(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc)
	w.Handle("my-task", func(ctx TaskContext) error {
		return nil
	})
	// Positive: handler is stored in map
	if _, ok := w.handlers["my-task"]; !ok {
		t.Fatal("handler not found in map after Handle()")
	}
	// Negative: unregistered type is not present
	if _, ok := w.handlers["other-task"]; ok {
		t.Fatal("unexpected handler for other-task")
	}
}

func TestExtractTraceContextWithTraceparent(t *testing.T) {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	// Use valid 32-char trace ID and 16-char span ID.
	msg := &testJetstreamMsg{
		data: []byte("{}"),
		headers: nats.Header{
			"traceparent": {
				"00-" +
					"0af7651916cd43dd8448eb211c80319c-" +
					"b7ad6b7169203331-01",
			},
		},
	}
	ctx := observe.ExtractTraceContext(msg, nil)
	// Positive: context is not nil
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	// Verify remote span context was injected
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("expected valid SpanContext")
	}
	wantTrace := "0af7651916cd43dd8448eb211c80319c"
	if sc.TraceID().String() != wantTrace {
		t.Fatalf(
			"TraceID = %q, want %q",
			sc.TraceID().String(), wantTrace,
		)
	}
}

func TestExtractTraceContextNoHeader(t *testing.T) {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	msg := &testJetstreamMsg{data: []byte("{}")}
	ctx := observe.ExtractTraceContext(msg, nil)
	// Positive: returns a valid context
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	// Negative: no span context when no header
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		t.Fatal("expected invalid SpanContext without header")
	}
}

// testJetstreamMsg is a minimal mock of jetstream.Msg for unit
// tests that only need Data() and Headers().
type testJetstreamMsg struct {
	data    []byte
	headers nats.Header
}

func (m *testJetstreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, nil
}
func (m *testJetstreamMsg) Data() []byte         { return m.data }
func (m *testJetstreamMsg) Headers() nats.Header { return m.headers }
func (m *testJetstreamMsg) Subject() string      { return "" }
func (m *testJetstreamMsg) Reply() string        { return "" }
func (m *testJetstreamMsg) Ack() error           { return nil }
func (m *testJetstreamMsg) DoubleAck(context.Context) error {
	return nil
}
func (m *testJetstreamMsg) Nak() error { return nil }
func (m *testJetstreamMsg) NakWithDelay(time.Duration) error {
	return nil
}
func (m *testJetstreamMsg) InProgress() error { return nil }
func (m *testJetstreamMsg) Term() error       { return nil }
func (m *testJetstreamMsg) TermWithReason(string) error {
	return nil
}

// newTestSpan creates a noop OTel span with a valid span context
// for testing trace injection.
func newTestSpan(
	traceIDHex, spanIDHex string,
) trace.Span {
	tid, _ := trace.TraceIDFromHex(traceIDHex)
	sid, _ := trace.SpanIDFromHex(spanIDHex)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(
		context.Background(), sc,
	)
	// Use noop tracer to create a span carrying the context.
	_, span := tracenoop.NewTracerProvider().
		Tracer("test").Start(ctx, "test")
	return span
}

func TestInjectTraceContextSetsHeader(t *testing.T) {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	traceID := "0af7651916cd43dd8448eb211c80319c"
	spanID := "b7ad6b7169203331"
	span := newTestSpan(traceID, spanID)
	ctx := trace.ContextWithRemoteSpanContext(
		context.Background(), span.SpanContext(),
	)
	evt := &protocol.Event{}
	msg := &nats.Msg{}
	observe.InjectTraceContext(ctx, msg, evt)
	// Positive: traceparent header is set
	tp := msg.Header.Get("traceparent")
	want := "00-" + traceID + "-" + spanID + "-01"
	if tp != want {
		t.Fatalf("traceparent = %q, want %q", tp, want)
	}
	// Positive: event TraceParent field is set
	if evt.TraceParent != want {
		t.Fatalf(
			"evt.TraceParent = %q, want %q",
			evt.TraceParent, want,
		)
	}
}

func TestInjectTraceContextEmptyIDs(t *testing.T) {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	// Background context has no span — no injection.
	evt := &protocol.Event{}
	msg := &nats.Msg{}
	observe.InjectTraceContext(
		context.Background(), msg, evt,
	)
	// Positive: no traceparent when no span context
	tp := msg.Header.Get("traceparent")
	if tp != "" {
		t.Fatalf(
			"traceparent = %q, want empty", tp,
		)
	}
	// Negative: event TraceParent stays empty
	if evt.TraceParent != "" {
		t.Fatalf(
			"evt.TraceParent = %q, want empty",
			evt.TraceParent,
		)
	}
}

func TestWorkerNonRetryableErrorAcks(t *testing.T) {
	// Verifies that a NonRetryableError causes Fail+Ack,
	// not NakWithDelay. The handler is called exactly once.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	var callCount atomic.Int32
	w := NewWorker(nc)
	w.Handle("perm-fail", func(ctx TaskContext) error {
		callCount.Add(1)
		return NewNonRetryableError(
			fmt.Errorf("permanent"),
		)
	})
	w.Start()
	defer w.Stop()
	payload := protocol.TaskPayload{
		RunID:  "run-nre",
		StepID: "step-nre",
		Input:  json.RawMessage(`"x"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(
		"task.perm-fail.run-nre", data,
	); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	// Wait for handler to be called
	deadline := time.After(5 * time.Second)
	for callCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}
	// Small delay to ensure no redelivery
	time.Sleep(2 * time.Second)
	// Positive: handler called exactly once (acked, not nak'd)
	if callCount.Load() != 1 {
		t.Fatalf(
			"callCount = %d, want 1 (no retry)",
			callCount.Load(),
		)
	}
	// Negative: a step.failed event should appear
	sub, _ := js.SubscribeSync(
		"history.run-nre", nats.DeliverAll(),
	)
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	evt = nextEventOfType(t, sub, protocol.EventStepFailed, 2*time.Second, evt)
	if evt.Type != protocol.EventStepFailed {
		t.Fatalf(
			"event = %q, want %q",
			evt.Type, protocol.EventStepFailed,
		)
	}
}

func TestWorkerRegistersOnStart(t *testing.T) {
	// Methodology: Start a worker with a handler, verify it registers
	// in the directory with correct task types and metadata.
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("test-task", func(ctx TaskContext) error {
		return ctx.Complete(nil)
	})
	w.Start()
	defer w.Stop()

	// Give worker a moment to register
	time.Sleep(100 * time.Millisecond)

	dir := NewDirectory(js)
	workers, err := dir.List()
	// Positive: no error from List
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	// Positive: exactly 1 worker found
	if len(workers) != 1 {
		t.Fatalf("worker count = %d, want 1", len(workers))
	}
	// Positive: TaskTypes contains "test-task"
	found := false
	for _, tt := range workers[0].TaskTypes {
		if tt == "test-task" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("TaskTypes = %v, want [test-task]", workers[0].TaskTypes)
	}
	// Negative: Language and Transport are correct
	if workers[0].Language != "go" {
		t.Fatalf("Language = %q, want go", workers[0].Language)
	}
	if workers[0].Transport != "nats" {
		t.Fatalf("Transport = %q, want nats", workers[0].Transport)
	}
}

func TestWorkerDeregistersOnStop(t *testing.T) {
	// Methodology: start worker, verify registered, stop, verify deregistered.
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("cleanup-task", func(ctx TaskContext) error {
		return ctx.Complete(nil)
	})
	w.Start()

	// Give worker a moment to register
	time.Sleep(100 * time.Millisecond)

	dir := NewDirectory(js)
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	// Positive: worker is registered
	if len(workers) != 1 {
		t.Fatalf("worker count before Stop = %d, want 1", len(workers))
	}

	w.Stop()

	// Give worker a moment to deregister
	time.Sleep(100 * time.Millisecond)

	workers, err = dir.List()
	if err != nil {
		t.Fatalf("List after Stop failed: %v", err)
	}
	// Positive: worker is deregistered
	if len(workers) != 0 {
		t.Fatalf("worker count after Stop = %d, want 0", len(workers))
	}
}

func TestNonRetryableErrorPublishesNonRetriablePayload(t *testing.T) {
	// When handler returns NonRetryableError, the step.failed event
	// payload must contain failure_type: "non_retriable" so the
	// orchestrator skips retries.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	w := NewWorker(nc)
	w.Handle("fail-perm", func(ctx TaskContext) error {
		return NewNonRetryableError(fmt.Errorf("permanent"))
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		TaskID: "run-np.step-np",
		RunID:  "run-np",
		StepID: "step-np",
		Input:  []byte(`{}`),
	}
	data, _ := json.Marshal(payload)
	js.Publish("task.fail-perm.run-np", data,
		nats.MsgId("run-np.step-np.queued"))

	sub, _ := js.PullSubscribe(
		"history.run-np", "",
		nats.BindStream("WORKFLOW_HISTORY"),
	)
	// Fetch up to 2: step.started (since #137 Task 4) precedes step.failed.
	msgs, err := sub.Fetch(2, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	var evt protocol.Event
	for _, m := range msgs {
		var candidate protocol.Event
		if err := json.Unmarshal(m.Data, &candidate); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if candidate.Type == protocol.EventStepFailed {
			evt = candidate
			break
		}
	}
	if evt.Type != protocol.EventStepFailed {
		t.Fatalf("event type = %q, want step.failed", evt.Type)
	}

	var fp protocol.StepFailedPayload
	if err := json.Unmarshal(evt.Payload, &fp); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if fp.FailureType != protocol.FailureTypeNonRetriable {
		t.Fatalf("FailureType = %q, want non_retriable",
			fp.FailureType)
	}
}

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

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTaskContextComplete(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := jsLegacy.SubscribeSync(
		"history.run-1", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
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
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
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
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := jsLegacy.SubscribeSync(
		"history.run-2", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{RunID: "run-2", StepID: "step-b"},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
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
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := jsLegacy.SubscribeSync(
		"history.run-3", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{RunID: "run-3", StepID: "step-c"},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
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

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cpKV, _ := js.KeyValue(
		context.Background(), "checkpoints",
	)
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
		msg:          &testJetstreamMsg{},
		checkpointKV: cpKV,
	}

	// Positive: checkpoint writes and reads back
	err = tc.Checkpoint([]byte(`{"progress":50}`))
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

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	sigKV, _ := js.KeyValue(
		context.Background(), "signals",
	)
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

func TestTaskContextRetryCount(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-rc", StepID: "step-rc",
			Attempt: 3,
		},
		bgCtx, span, nil, nil, nil,
	)
	// Positive: returns correct attempt count
	if tc.RetryCount() != 3 {
		t.Fatalf(
			"RetryCount() = %d, want 3", tc.RetryCount(),
		)
	}
	// Negative: zero-value attempt is different
	tc2 := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{RunID: "r", StepID: "s"},
		bgCtx, span, nil, nil, nil,
	)
	if tc2.RetryCount() != 0 {
		t.Fatalf(
			"RetryCount() = %d, want 0", tc2.RetryCount(),
		)
	}
}

func TestTaskContextPutStream(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nc, tel, nil,
		protocol.TaskPayload{
			RunID: "run-ps", StepID: "step-ps",
		},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
	)
	// Subscribe to the stream subject before publishing
	sub, err := nc.SubscribeSync("stream.run-ps.step-ps")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	err = tc.PutStream([]byte("token-1"))
	if err != nil {
		t.Fatalf("PutStream failed: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg timeout: %v", err)
	}
	// Positive: data arrives on correct subject
	if string(msg.Data) != "token-1" {
		t.Fatalf(
			"data = %q, want %q", string(msg.Data), "token-1",
		)
	}
	// Negative: no message on wrong subject
	wrongSub, err := nc.SubscribeSync("stream.run-ps.other")
	if err != nil {
		t.Fatalf("Subscribe wrong failed: %v", err)
	}
	_, err = wrongSub.NextMsg(200 * time.Millisecond)
	if err == nil {
		t.Fatal("expected no message on wrong subject")
	}
}

func TestNewTaskContextPanicsOnNilTel(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil tel, got nil")
		}
		// Positive: panic message mentions tel
		msg := fmt.Sprintf("%v", r)
		if msg != "newTaskContext: tel must not be nil" {
			t.Fatalf("panic = %q, want tel message", msg)
		}
	}()
	newTaskContext(
		nil, nil, nil, protocol.TaskPayload{},
		context.Background(), nil, nil, nil, nil,
	)
}

func TestNewTaskContextPanicsOnNilCtx(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil ctx, got nil")
		}
		msg := fmt.Sprintf("%v", r)
		// Positive: panic message mentions ctx
		if msg != "newTaskContext: ctx must not be nil" {
			t.Fatalf("panic = %q, want ctx message", msg)
		}
	}()
	newTaskContext(
		nil, tel, nil, protocol.TaskPayload{},
		nil, nil, nil, nil, nil,
	)
}

func TestNewTaskContextFieldInit(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	payload := protocol.TaskPayload{
		RunID:     "run-init",
		StepID:    "step-init",
		Iteration: 7,
		Attempt:   2,
		Input:     []byte(`"data"`),
	}
	tc := newTaskContext(
		nil, tel, nil, payload,
		bgCtx, span, nil, nil, nil,
	)
	// Positive: all fields from payload are set
	if tc.runID != "run-init" {
		t.Fatalf("runID = %q, want run-init", tc.runID)
	}
	if tc.iteration != 7 {
		t.Fatalf("iteration = %d, want 7", tc.iteration)
	}
	// Negative: attempt is not iteration
	if tc.attempt == tc.iteration {
		t.Fatal("attempt should differ from iteration")
	}
}

func TestTaskContextCheckpointNilKV(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-nocp", StepID: "step-nocp",
		},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
	)
	// Positive: Checkpoint returns error when KV is nil
	err := tc.Checkpoint([]byte("state"))
	if err == nil {
		t.Fatal("expected error for nil checkpointKV")
	}
	// Negative: LoadCheckpoint returns nil,nil when KV is nil
	data, err := tc.LoadCheckpoint()
	if err != nil || data != nil {
		t.Fatalf(
			"LoadCheckpoint = (%v, %v), want (nil, nil)",
			data, err,
		)
	}
}

func TestTaskContextSignalNilKV(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-nosig", StepID: "step-nosig",
		},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
	)
	// Positive: WaitForSignal errors when signalKV is nil
	_, err := tc.WaitForSignal("sig", 1*time.Second)
	if err == nil {
		t.Fatal("expected error for nil signalKV")
	}
	// Negative: SendSignal also errors when signalKV is nil
	err = tc.SendSignal("run-nosig", "sig", []byte("data"))
	if err == nil {
		t.Fatal("expected error for nil signalKV on send")
	}
}

func TestTaskContextPause(t *testing.T) {
	// Methodology: create a worker with a handler that pauses on first call,
	// then completes on resume. Verify handler is called twice with correct
	// checkpoint data and that completion happens after the delay.
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
	var callCount int
	pauseName := "waiting-for-approval"

	w := NewWorker(nc, observe.NewNoopTelemetry())
	w.Handle("pausable", func(ctx TaskContext) error {
		callCount++
		checkpoint, err := ctx.LoadCheckpoint()
		if err != nil {
			return fmt.Errorf("load checkpoint: %w", err)
		}
		if checkpoint == nil {
			// First call: pause for 200ms
			return ctx.Pause(pauseName, 200*time.Millisecond)
		}
		// Resume detected: verify checkpoint data and complete
		var data map[string]any
		if err := json.Unmarshal(checkpoint, &data); err != nil {
			return fmt.Errorf("unmarshal checkpoint: %w", err)
		}
		if data["pause_resume"] != pauseName {
			return fmt.Errorf(
				"pause_resume = %v, want %s",
				data["pause_resume"], pauseName,
			)
		}
		return ctx.Complete([]byte(`"resumed"`))
	})
	w.Start()
	defer w.Stop()

	// Subscribe to completion event
	sub, _ := js.SubscribeSync("history.run-pause", nats.DeliverAll())

	// Publish task
	payload := protocol.TaskPayload{
		RunID:  "run-pause",
		StepID: "step-pause",
		Input:  json.RawMessage(`"start"`),
	}
	data, _ := json.Marshal(payload)
	start := time.Now()
	if _, err := js.Publish("task.pausable.run-pause", data); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait for completion event
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("completion timeout: %v", err)
	}
	elapsed := time.Since(start)

	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	// Positive: handler was called twice (initial + resume)
	if callCount != 2 {
		t.Fatalf("callCount = %d, want 2", callCount)
	}

	// Positive: completion event received
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf(
			"event type = %q, want %q",
			evt.Type, protocol.EventStepCompleted,
		)
	}

	// Negative: elapsed time should be >= pause duration
	if elapsed < 200*time.Millisecond {
		t.Fatalf(
			"elapsed = %v, want >= 200ms", elapsed,
		)
	}
}

func TestTaskContextPausePanicsOnEmptyName(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-pause-panic", StepID: "step",
		},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
	)
	defer func() {
		r := recover()
		// Positive: panics on empty name
		if r == nil {
			t.Fatal("expected panic for empty name")
		}
		msg := fmt.Sprintf("%v", r)
		// Negative: message mentions name
		if msg != "Pause: name must not be empty" {
			t.Fatalf("panic = %q, want name message", msg)
		}
	}()
	tc.Pause("", 1*time.Second)
}

func TestTaskContextPausePanicsOnZeroDuration(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-pause-zero", StepID: "step",
		},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
	)
	defer func() {
		r := recover()
		// Positive: panics on zero duration
		if r == nil {
			t.Fatal("expected panic for zero duration")
		}
		msg := fmt.Sprintf("%v", r)
		// Negative: message mentions duration must be positive
		if msg != "Pause: duration must be positive" {
			t.Fatalf("panic = %q, want duration message", msg)
		}
	}()
	tc.Pause("test", 0)
}

func TestTaskContextPausePanicsOnNegativeDuration(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")
	tc := newTaskContext(
		nil, tel, nil,
		protocol.TaskPayload{
			RunID: "run-pause-neg", StepID: "step",
		},
		bgCtx, span, &testJetstreamMsg{}, nil, nil,
	)
	defer func() {
		r := recover()
		// Positive: panics on negative duration
		if r == nil {
			t.Fatal("expected panic for negative duration")
		}
		// Negative: error mentions positive requirement
		msg := fmt.Sprintf("%v", r)
		if msg != "Pause: duration must be positive" {
			t.Fatalf("panic = %q, want positive message", msg)
		}
	}()
	tc.Pause("test", -1*time.Second)
}

func TestFailPermanentPublishesNonRetriable(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	jsLegacy, _ := nc.JetStream()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")

	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{
			RunID: "run-1", StepID: "step-a",
		},
		bgCtx, span,
		&testJetstreamMsg{}, nil, nil,
	)

	if err := tc.FailPermanent(fmt.Errorf("not found")); err != nil {
		t.Fatalf("FailPermanent: %v", err)
	}

	sub, err := jsLegacy.PullSubscribe(
		"history.run-1", "",
		nats.BindStream("WORKFLOW_HISTORY"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(msgs))
	}

	var evt protocol.Event
	if err := json.Unmarshal(msgs[0].Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Type != protocol.EventStepFailed {
		t.Fatalf("event type = %q, want step.failed", evt.Type)
	}

	var payload protocol.StepFailedPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.FailureType != protocol.FailureTypeNonRetriable {
		t.Fatalf("FailureType = %q, want non_retriable",
			payload.FailureType)
	}
	if payload.Error != "not found" {
		t.Fatalf("Error = %q, want %q",
			payload.Error, "not found")
	}
}

func TestFailRetryAfterPublishesDelay(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	jsLegacy, _ := nc.JetStream()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")

	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{
			RunID: "run-2", StepID: "step-b",
		},
		bgCtx, span,
		&testJetstreamMsg{}, nil, nil,
	)

	err = tc.FailRetryAfter(
		fmt.Errorf("rate limited"), 5*time.Second,
	)
	if err != nil {
		t.Fatalf("FailRetryAfter: %v", err)
	}

	sub, _ := jsLegacy.PullSubscribe(
		"history.run-2", "",
		nats.BindStream("WORKFLOW_HISTORY"),
	)
	msgs, _ := sub.Fetch(1, nats.MaxWait(3*time.Second))
	if len(msgs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(msgs))
	}

	var evt protocol.Event
	json.Unmarshal(msgs[0].Data, &evt)

	var payload protocol.StepFailedPayload
	json.Unmarshal(evt.Payload, &payload)
	if payload.FailureType != protocol.FailureTypeRetryAfter {
		t.Fatalf("FailureType = %q, want retry_after",
			payload.FailureType)
	}
	if payload.RetryAfterMs != 5000 {
		t.Fatalf("RetryAfterMs = %d, want 5000",
			payload.RetryAfterMs)
	}
}

func TestFailRetryAfterClampsBounds(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	jsLegacy, _ := nc.JetStream()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tel := observe.NewNoopTelemetry()
	bgCtx := context.Background()
	_, span := tel.Tracer.Start(bgCtx, "test")

	tc := newTaskContext(
		nc, tel, js,
		protocol.TaskPayload{
			RunID: "run-3", StepID: "step-c",
		},
		bgCtx, span,
		&testJetstreamMsg{}, nil, nil,
	)
	tc.FailRetryAfter(fmt.Errorf("slow"), 2*time.Hour)

	sub, _ := jsLegacy.PullSubscribe(
		"history.run-3", "",
		nats.BindStream("WORKFLOW_HISTORY"),
	)
	msgs, _ := sub.Fetch(1, nats.MaxWait(3*time.Second))
	var evt protocol.Event
	json.Unmarshal(msgs[0].Data, &evt)
	var payload protocol.StepFailedPayload
	json.Unmarshal(evt.Payload, &payload)
	if payload.RetryAfterMs != 3_600_000 {
		t.Fatalf("RetryAfterMs = %d, want 3600000 (clamped)",
			payload.RetryAfterMs)
	}
}

func TestFailPermanentPanicsOnNilErr(t *testing.T) {
	tc := &taskContext{msg: &testJetstreamMsg{}}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil err")
		}
	}()
	tc.FailPermanent(nil)
}

func TestFailRetryAfterPanicsOnNegativeDuration(t *testing.T) {
	tc := &taskContext{msg: &testJetstreamMsg{}}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for negative duration")
		}
	}()
	tc.FailRetryAfter(fmt.Errorf("err"), -1*time.Second)
}

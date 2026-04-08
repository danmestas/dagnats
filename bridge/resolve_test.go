// resolve_test.go
// Tests for the /v1/tasks/{id}/resolve endpoint.
// Methodology: real NATS server, poll a task, resolve it via HTTP,
// verify events on the history stream.
package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// publishAndPollTask is a test helper that publishes a task and
// polls it via the bridge, returning the task ID.
func publishAndPollTask(
	t *testing.T,
	nc *nats.Conn,
	b *Bridge,
	ts *httptest.Server,
	runID, stepID string,
) string {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: stepID,
		Input:  json.RawMessage(`{"x":1}`),
	}
	data, _ := json.Marshal(payload)
	ctx := context.Background()
	_, err = js.Publish(ctx, "task.echo."+runID, data)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	pollBody := `{
		"task_types":["echo"],
		"max_tasks":1,
		"timeout_ms":5000
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(pollBody),
	)
	if err != nil {
		t.Fatalf("poll failed: %v", err)
	}
	defer resp.Body.Close()

	var tasks []pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	return tasks[0].TaskID
}

// consumeHistoryEvent reads a single event from the history stream
// for the given run via the new jetstream API.
func consumeHistoryEvent(
	t *testing.T,
	nc *nats.Conn,
	runID string,
	timeout time.Duration,
) protocol.Event {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx := context.Background()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream(HISTORY) failed: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			FilterSubject:     "history." + runID,
			AckPolicy:         jetstream.AckNonePolicy,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			InactiveThreshold: timeout,
		},
	)
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer failed: %v", err)
	}
	fetchResult, err := cons.Fetch(
		1, jetstream.FetchMaxWait(timeout),
	)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	msg, ok := <-fetchResult.Messages()
	if !ok {
		if fetchResult.Error() != nil {
			t.Fatalf("fetch error: %v", fetchResult.Error())
		}
		t.Fatal("no history event received")
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		t.Fatalf("unmarshal event failed: %v", err)
	}
	return evt
}

func TestResolveComplete(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-c", "step-c",
	)

	// Resolve as complete
	body := `{"action":"complete","output":{"result":"ok"}}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Task should be removed from ackMap
	_, ok := b.ackMap.Load(taskID)
	if ok {
		t.Fatal("expected task to be removed from ackMap")
	}

	// Verify event on history stream
	evt := consumeHistoryEvent(t, nc, "run-c", 2*time.Second)
	if evt.Type != protocol.EventStepCompleted {
		t.Fatalf(
			"expected step.completed, got %s", evt.Type,
		)
	}
}

func TestResolveFail(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-f", "step-f",
	)

	body := `{"action":"fail","error":"something broke"}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Task should be removed from ackMap
	_, ok := b.ackMap.Load(taskID)
	if ok {
		t.Fatal("expected task to be removed from ackMap")
	}

	// Verify fail event on history stream
	evt := consumeHistoryEvent(t, nc, "run-f", 2*time.Second)
	if evt.Type != protocol.EventStepFailed {
		t.Fatalf("expected step.failed, got %s", evt.Type)
	}
}

func TestResolvePause(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "checkpoints"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-p", "step-p",
	)

	body := `{
		"action":"pause",
		"name":"wait-for-approval",
		"duration_ms":5000,
		"checkpoint":{"state":"paused"}
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Task should be removed from ackMap
	_, ok := b.ackMap.Load(taskID)
	if ok {
		t.Fatal("expected task to be removed from ackMap")
	}

	// Verify checkpoint was written to KV
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, "checkpoints")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	entry, err := kv.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get checkpoint failed: %v", err)
	}
	if string(entry.Value()) != `{"state":"paused"}` {
		t.Fatalf(
			"unexpected checkpoint: %s", string(entry.Value()),
		)
	}
}

func TestResolveNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	body := `{"action":"complete","output":{}}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/nonexistent/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestResolveBadAction(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-bad", "step-bad",
	)

	body := `{"action":"explode"}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	// Task should still be in ackMap (not consumed)
	_, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Fatal("expected task to remain in ackMap")
	}
}

func TestResolveCheckpoint(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "checkpoints"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-chk", "step-chk",
	)

	// Resolve with checkpoint action
	body := `{"action":"checkpoint","data":{"progress":50}}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify checkpoint was written to KV
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, "checkpoints")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	entry, err := kv.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get checkpoint failed: %v", err)
	}
	if string(entry.Value()) != `{"progress":50}` {
		t.Fatalf(
			"unexpected checkpoint: %s", string(entry.Value()),
		)
	}

	// Task should still be in ackMap (checkpoint extends deadline)
	_, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Fatal(
			"expected task to remain in ackMap after checkpoint",
		)
	}
}

func TestResolveSendSignal(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "signals"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-sig", "step-sig",
	)

	// Send a signal to a different run
	body := `{
		"action":"send_signal",
		"run_id":"run-target",
		"name":"approval",
		"data":{"approved":true}
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify signal was written to KV
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, "signals")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	entry, err := kv.Get(ctx, "run-target.approval")
	if err != nil {
		t.Fatalf("Get signal failed: %v", err)
	}
	expected := `{"approved":true}`
	if string(entry.Value()) != expected {
		t.Fatalf(
			"unexpected signal data: %s", string(entry.Value()),
		)
	}

	// Task should still be in ackMap (send_signal extends deadline)
	_, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Fatal(
			"expected task to remain in ackMap after send_signal",
		)
	}
}

func TestResolveWaitSignalImmediate(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "signals"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Pre-populate the signal in KV
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, "signals")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	signalData := []byte(`{"status":"ready"}`)
	_, err = kv.Put(ctx, "run-wait.approval", signalData)
	if err != nil {
		t.Fatalf("Put signal failed: %v", err)
	}

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-wait", "step-wait",
	)

	// Wait for the signal (should return immediately)
	body := `{
		"action":"wait_signal",
		"name":"approval",
		"timeout_ms":5000
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify signal data in response
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if result["status"] != "ready" {
		t.Fatalf("unexpected signal data: %v", result)
	}
}

func TestResolveWaitSignalTimeout(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "signals"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-timeout", "step-timeout",
	)

	// Wait for signal that will never arrive
	body := `{
		"action":"wait_signal",
		"name":"missing",
		"timeout_ms":200
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("expected 408, got %d", resp.StatusCode)
	}
}

func TestResolveFailWithFailureType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-bf", "step-bf",
	)

	body := `{
		"action": "fail",
		"error": "not found",
		"failure_type": "non_retriable"
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	evt := consumeHistoryEvent(t, nc, "run-bf", 3*time.Second)
	if evt.Type != protocol.EventStepFailed {
		t.Fatalf("event type = %q, want step.failed", evt.Type)
	}
	var payload protocol.StepFailedPayload
	json.Unmarshal(evt.Payload, &payload)
	if payload.FailureType != protocol.FailureTypeNonRetriable {
		t.Fatalf("FailureType = %q, want non_retriable",
			payload.FailureType)
	}
}

func TestResolveFailDefaultsToRetriable(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-bfd", "step-bfd",
	)

	body := `{"action":"fail","error":"transient"}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	evt := consumeHistoryEvent(t, nc, "run-bfd", 3*time.Second)
	var payload protocol.StepFailedPayload
	json.Unmarshal(evt.Payload, &payload)
	if payload.FailureType != protocol.FailureTypeRetriable {
		t.Fatalf("FailureType = %q, want retriable",
			payload.FailureType)
	}
}

func TestResolvePauseInvalidDuration(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-pause-bad", "step1")

	// Negative: duration_ms = 0 should fail
	body := `{"action":"pause","duration_ms":0}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}

	// Negative: duration_ms too large
	body2 := `{"action":"pause","duration_ms":3600001}`
	resp2, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body2),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500",
			resp2.StatusCode)
	}
}

func TestResolveSendSignalMissingFields(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "signals"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-sig-bad", "step1")

	// Negative: missing name
	body := `{"action":"send_signal","run_id":"r1","data":{}}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("missing name: status = %d, want 500",
			resp.StatusCode)
	}

	// Positive: task should remain in ackMap after validation error
	_, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Error("expected task to remain in ackMap after error")
	}

	// Negative: missing run_id - test with same task
	body2 := `{"action":"send_signal","name":"s","data":{}}`
	resp2, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body2),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusInternalServerError {
		t.Errorf("missing run_id: status = %d, want 500",
			resp2.StatusCode)
	}
}

func TestResolveWaitSignalInvalidTimeout(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "signals"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-wait-bad", "step1")

	// Negative: timeout_ms = 0 should fail
	body := `{
		"action":"wait_signal",
		"name":"sig",
		"timeout_ms":0
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

func TestResolveInvalidJSON(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-badjson", "step1")

	// Negative: malformed JSON
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(`{not json}`),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}

	// Positive: task should still be in ackMap (not lost)
	if b.ackMap.Count() == 0 {
		t.Error("ackMap should still hold the task")
	}
}

func TestResolveContinuePublishesEvent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-cont", "step-cont",
	)

	body := `{"action":"continue","output":{"next":"step2"}}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify continue event published
	evt := consumeHistoryEvent(t, nc, "run-cont", 5*time.Second)
	if evt.Type != protocol.EventStepContinue {
		t.Errorf("event type = %s, want %s",
			evt.Type, protocol.EventStepContinue)
	}

	// Verify ackMap entry removed
	_, ok := b.ackMap.Load(taskID)
	if ok {
		t.Error("expected task removed from ackMap after continue")
	}
}

func TestResolveHeartbeatExtendsDeadline(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-hb", "step-hb",
	)

	body := `{"action":"heartbeat"}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify task is still in ackMap (heartbeat keeps it alive)
	_, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Error("expected task to remain in ackMap after heartbeat")
	}
}

func TestResolveStreamPublishesData(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Subscribe to stream subject before publishing
	sub, err := nc.SubscribeSync("stream.run-str.step-str")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-str", "step-str",
	)

	body := `{"action":"stream","data":{"token":"hello"}}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify stream message received
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no stream message received: %v", err)
	}
	if !strings.Contains(string(msg.Data), "hello") {
		t.Errorf("stream data = %s, want to contain 'hello'",
			string(msg.Data))
	}

	// Verify task still in ackMap (stream keeps task alive)
	_, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Error("expected task to remain in ackMap after stream")
	}
}

func TestResolveInvalidActionRejected(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(
		t, nc, b, ts, "run-inv", "step-inv",
	)

	body := `{"action":"destroy"}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	// Positive: task should still be in ackMap (not lost)
	_, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Fatal("expected task to remain in ackMap")
	}
}

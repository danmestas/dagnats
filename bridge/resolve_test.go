// resolve_test.go
// Tests for the /v1/tasks/{id}/resolve endpoint.
// Methodology: real NATS server, poll a task, resolve it via HTTP,
// verify events on the history stream.
package bridge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
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
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: stepID,
		Input:  json.RawMessage(`{"x":1}`),
	}
	data, _ := json.Marshal(payload)
	_, err = js.Publish("task.echo."+runID, data)
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
	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync(
		"history.run-c", nats.DeliverAll(),
	)
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("no history event: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event failed: %v", err)
	}
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
	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync(
		"history.run-f", nats.DeliverAll(),
	)
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("no history event: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event failed: %v", err)
	}
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
	js, _ := nc.JetStream()
	kv, err := js.KeyValue("checkpoints")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	entry, err := kv.Get(taskID)
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
	js, _ := nc.JetStream()
	kv, err := js.KeyValue("checkpoints")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	entry, err := kv.Get(taskID)
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
	js, _ := nc.JetStream()
	kv, err := js.KeyValue("signals")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	entry, err := kv.Get("run-target.approval")
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
	js, _ := nc.JetStream()
	kv, err := js.KeyValue("signals")
	if err != nil {
		t.Fatalf("KeyValue failed: %v", err)
	}
	signalData := []byte(`{"status":"ready"}`)
	_, err = kv.Put("run-wait.approval", signalData)
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

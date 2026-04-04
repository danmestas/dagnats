// poll_test.go
// Tests for the /v1/tasks/poll endpoint.
// Methodology: real NATS server, publish task messages, poll via HTTP.
package bridge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestPollReturnsTask(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	// Publish a task message
	payload := protocol.TaskPayload{
		RunID:  "run-1",
		StepID: "step-1",
		Input:  json.RawMessage(`{"key":"value"}`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	_, err = js.Publish("task.echo.run-1", data)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	body := `{
		"task_types":["echo"],
		"max_tasks":5,
		"timeout_ms":5000
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var tasks []pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	task := tasks[0]
	if task.TaskID != "run-1.step-1" {
		t.Fatalf("expected task_id run-1.step-1, got %s", task.TaskID)
	}
	if task.RunID != "run-1" {
		t.Fatalf("expected run_id run-1, got %s", task.RunID)
	}
	if task.StepID != "step-1" {
		t.Fatalf("expected step_id step-1, got %s", task.StepID)
	}

	// Verify message is in ackMap
	_, ok := b.ackMap.Load("run-1.step-1")
	if !ok {
		t.Fatal("expected message to be in ackMap")
	}
}

func TestPollTimeoutReturnsEmptyArray(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Poll with very short timeout, no messages published
	body := `{
		"task_types":["echo"],
		"max_tasks":1,
		"timeout_ms":100
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var tasks []pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks on timeout, got %d", len(tasks))
	}

	// AckMap should be empty
	if b.ackMap.Count() != 0 {
		t.Fatalf(
			"expected ackMap count 0, got %d", b.ackMap.Count(),
		)
	}
}

func TestPollBadRequest(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Missing task_types
	body := `{"max_tasks":1}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
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
}

func TestPollMultipleTasks(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	// Publish 3 tasks
	for i := 0; i < 3; i++ {
		payload := protocol.TaskPayload{
			RunID:  "run-m",
			StepID: "step-" + string(rune('a'+i)),
			Input:  json.RawMessage(`{}`),
		}
		data, _ := json.Marshal(payload)
		_, err := js.Publish(
			"task.echo.run-m",
			data,
			nats.MsgId("dedup-"+string(rune('a'+i))),
		)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	b := NewBridge(nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	body := `{
		"task_types":["echo"],
		"max_tasks":10,
		"timeout_ms":2000
	}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var tasks []pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}

	if b.ackMap.Count() != 3 {
		t.Fatalf(
			"expected ackMap count 3, got %d", b.ackMap.Count(),
		)
	}
}

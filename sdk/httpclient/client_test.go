// Methodology: E2E integration test with a real DagNats server. Each test
// gets an isolated server on a random port. Validates the full HTTP worker
// protocol: connect, poll, resolve, and workflow completion.
package httpclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/sdk/httpclient"
	"github.com/danmestas/dagnats/server"
)

// testServerAddr starts a full DagNats server and returns the HTTP
// base URL and a cleanup function. The server runs in a background
// goroutine and shuts down when cleanup is called.
func testServerAddr(t *testing.T) (string, func()) {
	if t == nil {
		panic("testServerAddr: t must not be nil")
	}
	t.Helper()

	cfg := server.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.NATSPort = -1

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()
	cfg.HTTPAddr = addr

	srv := server.New(cfg)
	if srv == nil {
		panic("server.New returned nil")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()

	// Wait for /ready (bounded 10s, 50ms poll interval)
	readyURL := "http://" + addr + "/ready"
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) && !ready {
		resp, getErr := http.Get(readyURL)
		if getErr == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			ready = true
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("server /ready did not return 200 within 10s")
	}

	baseURL := "http://" + addr
	cleanup := func() {
		srv.Stop()
		select {
		case runErr := <-errCh:
			if runErr != nil {
				t.Errorf("server.Run() error: %v", runErr)
			}
		case <-time.After(20 * time.Second):
			t.Error("server.Run() did not return within 20s")
		}
	}
	return baseURL, cleanup
}

// registerWorkflow registers a workflow via the REST API.
func registerWorkflow(
	t *testing.T, baseURL string, body string,
) {
	t.Helper()
	if baseURL == "" {
		panic("registerWorkflow: baseURL must not be empty")
	}
	if body == "" {
		panic("registerWorkflow: body must not be empty")
	}
	resp, err := http.Post(
		baseURL+"/workflows",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated &&
		resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("register workflow: %d %s",
			resp.StatusCode, string(b))
	}
}

// startRun starts a workflow run and returns the run ID.
func startRun(
	t *testing.T, baseURL string, workflow string,
	input json.RawMessage,
) string {
	t.Helper()
	if baseURL == "" {
		panic("startRun: baseURL must not be empty")
	}
	if workflow == "" {
		panic("startRun: workflow must not be empty")
	}

	body := map[string]any{
		"workflow": workflow,
		"input":    input,
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal start run: %v", err)
	}

	resp, err := http.Post(
		baseURL+"/runs",
		"application/json",
		strings.NewReader(string(data)),
	)
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated &&
		resp.StatusCode != http.StatusOK {
		t.Fatalf("start run: %d %s",
			resp.StatusCode, string(respBody))
	}

	var result struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("unmarshal run response: %v", err)
	}
	if result.RunID == "" {
		t.Fatal("run_id is empty in response")
	}
	return result.RunID
}

// waitRunCompleted polls run status until completed or timeout.
func waitRunCompleted(
	t *testing.T, baseURL string, runID string,
	timeout time.Duration,
) {
	t.Helper()
	if runID == "" {
		panic("waitRunCompleted: runID must not be empty")
	}
	if timeout <= 0 {
		panic("waitRunCompleted: timeout must be positive")
	}

	runURL := fmt.Sprintf("%s/runs/%s", baseURL, runID)
	deadline := time.Now().Add(timeout)
	const maxIterations = 300
	iterations := 0

	for time.Now().Before(deadline) && iterations < maxIterations {
		iterations++
		resp, err := http.Get(runURL)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var runState struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(body, &runState); err == nil {
			if runState.Status == "completed" {
				return
			}
			if runState.Status == "failed" {
				t.Fatalf("run %s failed", runID)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run %s did not complete within %v", runID, timeout)
}

// TestClientE2EWorkflowCompletion proves the full HTTP worker
// protocol: connect, poll, complete, and workflow finishes.
func TestClientE2EWorkflowCompletion(t *testing.T) {
	baseURL, cleanup := testServerAddr(t)
	defer cleanup()

	// Register a workflow with one "echo" task
	registerWorkflow(t, baseURL, `{
		"name": "sdk-e2e-test",
		"version": "1.0",
		"steps": [{"id": "echo", "task": "echo"}]
	}`)

	// Create HTTP client and connect as worker
	ctx := context.Background()
	client := httpclient.New(baseURL)
	if client == nil {
		t.Fatal("httpclient.New returned nil")
	}

	err := client.Connect(
		ctx, "sdk-test-worker",
		[]string{"echo"}, 1,
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Disconnect()

	// Start a workflow run
	runID := startRun(
		t, baseURL, "sdk-e2e-test",
		json.RawMessage(`"hello"`),
	)

	// Poll for tasks (bounded 10s)
	tasks, err := client.Poll(
		ctx, []string{"echo"}, 1, 10*time.Second,
	)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// Positive: got exactly one task
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	// Positive: task belongs to our run
	task := tasks[0]
	if task.RunID != runID {
		t.Errorf("task.RunID = %q, want %q",
			task.RunID, runID)
	}

	// Negative: task ID is non-empty
	if task.TaskID == "" {
		t.Error("task.TaskID is empty")
	}

	// Complete the task
	output := json.RawMessage(`"HELLO"`)
	err = client.Complete(ctx, task.TaskID, output)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Wait for workflow to finish
	waitRunCompleted(t, baseURL, runID, 15*time.Second)
}

// TestClientE2ETaskFailure proves that a failed task propagates
// the failure to the workflow run.
func TestClientE2ETaskFailure(t *testing.T) {
	baseURL, cleanup := testServerAddr(t)
	defer cleanup()

	registerWorkflow(t, baseURL, `{
		"name": "sdk-fail-test",
		"version": "1.0",
		"steps": [{"id": "fail-step", "task": "fail-task"}]
	}`)

	ctx := context.Background()
	client := httpclient.New(baseURL)

	err := client.Connect(
		ctx, "sdk-fail-worker",
		[]string{"fail-task"}, 1,
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Disconnect()

	_ = startRun(
		t, baseURL, "sdk-fail-test",
		json.RawMessage(`"input"`),
	)

	tasks, err := client.Poll(
		ctx, []string{"fail-task"}, 1, 10*time.Second,
	)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// Positive: got a task
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	// Fail the task
	err = client.Fail(ctx, tasks[0].TaskID, "intentional error")
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// Wait for workflow run to reach "failed" status
	runURL := fmt.Sprintf(
		"%s/runs/%s", baseURL, tasks[0].RunID,
	)
	deadline := time.Now().Add(15 * time.Second)
	reachedFailed := false
	const maxIterations = 150

	for i := 0; i < maxIterations && time.Now().Before(deadline); i++ {
		resp, getErr := http.Get(runURL)
		if getErr != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var state struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(body, &state) == nil &&
			state.Status == "failed" {
			reachedFailed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Positive: run failed as expected
	if !reachedFailed {
		t.Fatal("run did not reach 'failed' status within 15s")
	}

	// Negative: the run URL still responds (server is healthy)
	resp, err := http.Get(runURL)
	if err != nil {
		t.Fatalf("GET run after failure: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET run status: got %d, want 200",
			resp.StatusCode)
	}
}

// TestClientPollTimeout proves that polling with no available tasks
// returns an empty slice after timeout rather than erroring.
func TestClientPollTimeout(t *testing.T) {
	baseURL, cleanup := testServerAddr(t)
	defer cleanup()

	ctx := context.Background()
	client := httpclient.New(baseURL)

	err := client.Connect(
		ctx, "sdk-timeout-worker",
		[]string{"nonexistent"}, 1,
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Disconnect()

	// Poll for a task type nobody will ever publish — should
	// return empty after timeout
	tasks, err := client.Poll(
		ctx, []string{"nonexistent"}, 1, 1*time.Second,
	)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// Positive: returns empty, not nil
	if tasks == nil {
		t.Fatal("Poll returned nil, expected empty slice")
	}

	// Negative: no tasks were returned
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks))
	}
}

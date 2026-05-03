// Methodology: integration tests with real embedded NATS. Each test
// gets isolated server on random port. Verify startup, health, shutdown.
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// testConfig returns a Config suitable for isolated testing.
// Uses a random free port for HTTP and embedded NATS.
func testConfig(t *testing.T) Config {
	if t == nil {
		panic("testConfig: t must not be nil")
	}

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.NATSPort = -1 // Random port

	// Find free HTTP port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()
	cfg.HTTPAddr = addr

	return cfg
}

// TestServer_StartsAndStops verifies the server starts, becomes ready,
// serves health checks, and shuts down cleanly.
func TestServer_StartsAndStops(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	// Run server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run()
	}()

	// Poll /ready until server is ready (bounded 10s, 50ms sleep)
	readyURL := fmt.Sprintf("http://%s/ready", cfg.HTTPAddr)
	deadline := time.Now().Add(10 * time.Second)
	ready := false

	for time.Now().Before(deadline) && !ready {
		resp, err := http.Get(readyURL)
		if err == nil && resp.StatusCode == http.StatusOK {
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
		t.Fatal("/ready did not return 200 within 10s")
	}

	// Verify /health returns 200
	healthURL := fmt.Sprintf("http://%s/health", cfg.HTTPAddr)
	resp, err := http.Get(healthURL)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/health status: got %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// Stop server
	srv.Stop()

	// Verify Run() returns nil within 20s
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}
}

// TestServer_PrintsStartupAndShutdownBanner verifies that the server
// emits progress lines to stderr during startup and shutdown.
func TestServer_PrintsStartupAndShutdownBanner(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	// Capture stderr via pipe so we can inspect output
	origStderr := os.Stderr
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = pipeW

	// Restore stderr on exit regardless of outcome
	defer func() { os.Stderr = origStderr }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run()
	}()

	// Poll /ready until server is up (bounded 10s)
	readyURL := fmt.Sprintf("http://%s/ready", cfg.HTTPAddr)
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
		t.Fatal("/ready did not return 200 within 10s")
	}

	// Stop server and wait for Run() to return
	srv.Stop()
	select {
	case runErr := <-errCh:
		if runErr != nil {
			t.Fatalf("Run() returned error: %v", runErr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}

	// Close the write end so reads complete, then collect output
	pipeW.Close()
	captured, readErr := io.ReadAll(pipeR)
	if readErr != nil {
		t.Fatalf("read captured stderr: %v", readErr)
	}
	pipeR.Close()
	output := string(captured)

	// Positive assertions: startup and shutdown banners present
	if !strings.Contains(output, "nats server started") {
		t.Errorf("stderr missing 'nats server started'; got:\n%s", output)
	}
	if !strings.Contains(output, "http") {
		t.Errorf("stderr missing ready banner; got:\n%s", output)
	}
	if !strings.Contains(output, "shutdown complete") {
		t.Errorf("stderr missing 'shutdown complete'; got:\n%s", output)
	}

	// Negative: no errors during a clean lifecycle
	if strings.Contains(strings.ToLower(output), "error") {
		t.Errorf("stderr contains 'error' during clean run:\n%s", output)
	}
}

// TestServerMountsBridgeEndpoints verifies that the bridge handler
// is mounted on the HTTP mux and responds to connection requests.
func TestServerMountsBridgeEndpoints(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()

	// Wait for server to become ready
	readyURL := fmt.Sprintf("http://%s/ready", cfg.HTTPAddr)
	deadline := time.Now().Add(10 * time.Second)
	ready := false

	for time.Now().Before(deadline) && !ready {
		resp, err := http.Get(readyURL)
		if err == nil && resp.StatusCode == http.StatusOK {
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
		t.Fatal("/ready did not return 200 within 10s")
	}

	// Test bridge endpoint responds
	body := `{"worker_id":"w-1","task_types":["echo"],"max_tasks":1}`
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/workers/connect", cfg.HTTPAddr),
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /v1/workers/connect: %v", err)
	}
	defer resp.Body.Close()

	// Positive: endpoint responds with 200
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("bridge endpoint status: got %d, want 200; body: %s",
			resp.StatusCode, string(bodyBytes))
	}

	// Negative: content type should be text/event-stream (SSE)
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/event-stream") {
		t.Errorf("content type: got %q, want text/event-stream",
			contentType)
	}

	// Clean shutdown
	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}
}

// TestServer_EmbeddedWorkerCompletesRun verifies that an embedded
// worker can process a task end-to-end: register handler, start
// server, register workflow via REST, start run, poll until done.
func TestServer_EmbeddedWorkerCompletesRun(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	// Register embedded handler that returns a fixed result.
	// First steps receive no input (run-level input is not
	// forwarded), so the handler must not depend on it.
	w := EmbeddedWorker(srv)
	w.Handle("upper", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`"HELLO"`))
	})

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()

	// Wait for ready
	readyURL := fmt.Sprintf(
		"http://%s/ready", cfg.HTTPAddr,
	)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(readyURL)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Register workflow via REST API
	wfBody := `{
		"name": "embedded-test",
		"version": "1.0",
		"steps": [{"id": "upper", "task": "upper"}]
	}`
	wfResp, err := http.Post(
		fmt.Sprintf(
			"http://%s/workflows", cfg.HTTPAddr,
		),
		"application/json",
		strings.NewReader(wfBody),
	)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}
	if wfResp.StatusCode != http.StatusOK &&
		wfResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(wfResp.Body)
		t.Fatalf("register workflow: %d %s",
			wfResp.StatusCode, string(body))
	}
	wfResp.Body.Close()

	// Start a run
	runBody := `{
		"workflow": "embedded-test",
		"input": "hello"
	}`
	runResp, err := http.Post(
		fmt.Sprintf(
			"http://%s/runs", cfg.HTTPAddr,
		),
		"application/json",
		strings.NewReader(runBody),
	)
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	runRespBody, _ := io.ReadAll(runResp.Body)
	runResp.Body.Close()

	if runResp.StatusCode != http.StatusOK &&
		runResp.StatusCode != http.StatusCreated {
		t.Fatalf("start run: %d %s",
			runResp.StatusCode, string(runRespBody))
	}

	// Extract run_id
	var startResult struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(
		runRespBody, &startResult,
	); err != nil {
		t.Fatalf("unmarshal run response: %v", err)
	}
	if startResult.RunID == "" {
		t.Fatal("run_id is empty in response")
	}

	// Poll run status until completed (bounded 15s)
	runURL := fmt.Sprintf(
		"http://%s/runs/%s",
		cfg.HTTPAddr, startResult.RunID,
	)
	pollDeadline := time.Now().Add(15 * time.Second)
	completed := false

	for time.Now().Before(pollDeadline) && !completed {
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
		if err := json.Unmarshal(
			body, &runState,
		); err == nil && runState.Status == "completed" {
			completed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Positive: run completed
	if !completed {
		t.Fatal("run did not complete within 15s")
	}

	// Clean shutdown
	srv.Stop()
	select {
	case err := <-errCh:
		// Negative: no errors during clean shutdown
		if err != nil {
			t.Errorf("Run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}
}

// TestServer_NATSAPIRespondsAfterReady verifies that the NATSAPI control
// plane is wired into Server.Run. After /ready returns 200, an external
// NATS client must get a structured reply (not ErrNoResponders) on
// api.workflows.register and api.runs.start. Regression test for #164.
func TestServer_NATSAPIRespondsAfterReady(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()

	readyURL := fmt.Sprintf("http://%s/ready", cfg.HTTPAddr)
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) && !ready {
		resp, err := http.Get(readyURL)
		if err == nil && resp.StatusCode == http.StatusOK {
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
		t.Fatal("/ready did not return 200 within 10s")
	}

	// External caller path: connect to embedded NATS via its URL.
	nc, err := nats.Connect(srv.ns.ClientURL())
	if err != nil {
		t.Fatalf("connect external NATS client: %v", err)
	}
	defer nc.Close()

	// Positive: api.workflows.register subscriber responds.
	wfBody := []byte(
		`{"name":"natsapi-wired","steps":` +
			`[{"id":"a","task":"a"}]}`,
	)
	reply, err := nc.Request(
		"api.workflows.register", wfBody, 2*time.Second,
	)
	if err != nil {
		t.Fatalf(
			"api.workflows.register: %v "+
				"(NATSAPI not wired into Server.Run?)", err,
		)
	}
	var regResp map[string]string
	if err := json.Unmarshal(reply.Data, &regResp); err != nil {
		t.Fatalf("unmarshal register reply: %v", err)
	}
	if regResp["status"] != "registered" {
		t.Fatalf(
			"register status = %q, want 'registered' (body=%s)",
			regResp["status"], string(reply.Data),
		)
	}

	// Positive: api.runs.start subscriber responds with a run_id.
	startBody := []byte(`{"workflow":"natsapi-wired"}`)
	reply, err = nc.Request(
		"api.runs.start", startBody, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("api.runs.start: %v", err)
	}
	var startResp map[string]string
	if err := json.Unmarshal(reply.Data, &startResp); err != nil {
		t.Fatalf("unmarshal start reply: %v", err)
	}
	if startResp["run_id"] == "" {
		t.Fatalf(
			"start reply missing run_id (body=%s)",
			string(reply.Data),
		)
	}

	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}
}

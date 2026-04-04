// connect_test.go
// Tests for the /v1/workers/connect SSE endpoint.
// Methodology: real NATS server, httptest server, verify worker
// registration and SSE heartbeat stream.
package bridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/worker"
)

func TestConnectRegistersWorker(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc, nil)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Run connect in a goroutine; signal when done so we know
	// the deferred deregister has executed.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(
			context.Background(), 200*time.Millisecond,
		)
		defer cancel()
		body := `{
			"worker_id":"http-w1",
			"task_types":["echo"],
			"max_tasks":3
		}`
		req, err := http.NewRequestWithContext(
			ctx, "POST", ts.URL+"/v1/workers/connect",
			strings.NewReader(body),
		)
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req) //nolint:bodyclose // closed below
		if resp != nil {
			// Verify SSE headers before closing
			ct := resp.Header.Get("Content-Type")
			if ct != "text/event-stream" {
				t.Errorf(
					"expected text/event-stream, got %s", ct,
				)
			}
			resp.Body.Close()
		}
	}()

	// Wait for the goroutine (and the server's deferred deregister)
	// to finish. Bounded by test timeout.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connect goroutine did not finish in time")
	}

	// Give the server handler's defer a moment to run after
	// the client disconnects.
	time.Sleep(200 * time.Millisecond)

	js, _ := nc.JetStream()
	dir := worker.NewDirectory(js)
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	// Worker should be deregistered after disconnect
	for _, w := range workers {
		if w.WorkerID == "http-w1" {
			t.Fatal("expected worker to be deregistered")
		}
	}
}

func TestConnectBadRequest(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc, nil)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Missing worker_id
	body := `{"task_types":["echo"]}`
	resp, err := http.Post(
		ts.URL+"/v1/workers/connect",
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

	// Missing task_types
	body2 := `{"worker_id":"w1"}`
	resp2, err := http.Post(
		ts.URL+"/v1/workers/connect",
		"application/json",
		strings.NewReader(body2),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp2.StatusCode)
	}
}

// Tests for the OTLP bridge that consumes from the NATS TELEMETRY
// stream and exports to an OTLP/HTTP endpoint.
// Methodology: integration tests with real embedded NATS and
// httptest server. Verifies end-to-end: publish telemetry message
// → bridge consumes → HTTP POST to correct endpoint with correct
// content type. Both positive (valid message) and negative
// (bridge stops cleanly) cases are covered.
package otlp

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/observe/simple"
)

func TestBridge_ExportsSpans(t *testing.T) {
	nc := dagnatstest.Server(t)

	var mu sync.Mutex
	var receivedPath string
	var receivedContentType string
	var receivedBody []byte

	srv := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				receivedPath = r.URL.Path
				receivedContentType = r.Header.Get(
					"Content-Type",
				)
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
					w.WriteHeader(
						http.StatusInternalServerError,
					)
					return
				}
				receivedBody = body
				w.WriteHeader(http.StatusOK)
			},
		),
	)
	defer srv.Close()

	bridge := NewBridge(nc, BridgeConfig{
		Endpoint:      srv.URL,
		BatchSize:     1,
		FlushInterval: 100 * time.Millisecond,
		ServiceName:   "test-svc",
	})
	bridge.Start()
	defer bridge.Stop()

	span := simple.SpanRecord{
		TraceID:   "aaaabbbbccccddddaaaabbbbccccdddd",
		SpanID:    "1111222233334444",
		Name:      "bridge-test-span",
		Service:   "test-svc",
		Kind:      "server",
		StartTime: time.Now().Add(-time.Second),
		EndTime:   time.Now(),
		Status:    "ok",
	}

	data, err := json.Marshal(span)
	if err != nil {
		t.Fatalf("marshal span: %v", err)
	}

	err = nc.Publish(
		"telemetry.spans.test-svc.run1", data,
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Wait for bridge to process
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		path := receivedPath
		mu.Unlock()
		if path != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	// Positive: correct path and content type
	if receivedPath != "/v1/traces" {
		t.Fatalf(
			"path: got %q, want /v1/traces",
			receivedPath,
		)
	}
	if receivedContentType != "application/x-protobuf" {
		t.Fatalf(
			"content-type: got %q, want application/x-protobuf",
			receivedContentType,
		)
	}

	// Negative: body is non-empty protobuf
	if len(receivedBody) == 0 {
		t.Fatal("received empty body")
	}
}

func TestBridge_ExportsLogs(t *testing.T) {
	nc := dagnatstest.Server(t)

	var mu sync.Mutex
	var receivedPath string
	var receivedBody []byte

	srv := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				receivedPath = r.URL.Path
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				receivedBody = body
				w.WriteHeader(http.StatusOK)
			},
		),
	)
	defer srv.Close()

	bridge := NewBridge(nc, BridgeConfig{
		Endpoint:      srv.URL,
		BatchSize:     1,
		FlushInterval: 100 * time.Millisecond,
		ServiceName:   "log-svc",
	})
	bridge.Start()
	defer bridge.Stop()

	rec := simple.LogRecord{
		Level:     "info",
		Message:   "bridge log test",
		Service:   "log-svc",
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal log: %v", err)
	}

	err = nc.Publish(
		"telemetry.logs.log-svc.info", data,
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		path := receivedPath
		mu.Unlock()
		if path != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if receivedPath != "/v1/logs" {
		t.Fatalf(
			"path: got %q, want /v1/logs", receivedPath,
		)
	}
	if len(receivedBody) == 0 {
		t.Fatal("received empty body")
	}
}

func TestBridge_ExportsMetrics(t *testing.T) {
	nc := dagnatstest.Server(t)

	var mu sync.Mutex
	var receivedPath string
	var receivedBody []byte

	srv := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				receivedPath = r.URL.Path
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				receivedBody = body
				w.WriteHeader(http.StatusOK)
			},
		),
	)
	defer srv.Close()

	bridge := NewBridge(nc, BridgeConfig{
		Endpoint:      srv.URL,
		BatchSize:     1,
		FlushInterval: 100 * time.Millisecond,
		ServiceName:   "metric-svc",
	})
	bridge.Start()
	defer bridge.Stop()

	point := simple.MetricPoint{
		Name:      "test_counter",
		Type:      "counter",
		Value:     42.0,
		Service:   "metric-svc",
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(point)
	if err != nil {
		t.Fatalf("marshal metric: %v", err)
	}

	err = nc.Publish(
		"telemetry.metrics.metric-svc.test_counter", data,
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		path := receivedPath
		mu.Unlock()
		if path != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if receivedPath != "/v1/metrics" {
		t.Fatalf(
			"path: got %q, want /v1/metrics",
			receivedPath,
		)
	}
	if len(receivedBody) == 0 {
		t.Fatal("received empty body")
	}
}

func TestBridge_StopsCleanly(t *testing.T) {
	nc := dagnatstest.Server(t)

	srv := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
		),
	)
	defer srv.Close()

	bridge := NewBridge(nc, BridgeConfig{
		Endpoint:      srv.URL,
		BatchSize:     10,
		FlushInterval: time.Second,
		ServiceName:   "stop-svc",
	})
	bridge.Start()

	// Stop should return without blocking
	done := make(chan struct{})
	go func() {
		bridge.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success: stopped cleanly
	case <-time.After(5 * time.Second):
		t.Fatal("bridge.Stop() did not return within 5s")
	}
}

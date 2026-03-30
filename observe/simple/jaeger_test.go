// observe/simple/jaeger_test.go
// Tests for Jaeger OTLP/HTTP exporter. Methodology: real embedded NATS +
// mock HTTP server. Verify batching, error handling, shutdown.
package simple

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestExportToJaegerHappyPath(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received.Add(1)
			w.WriteHeader(http.StatusOK)
		},
	))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	logger := observe.NewNoopLogger()
	go ExportToJaeger(ctx, js, srv.URL, logger)

	// Give exporter time to subscribe
	time.Sleep(200 * time.Millisecond)

	rec := SpanRecord{
		TraceID: "t1", SpanID: "s1", Name: "test",
		Service: "engine", Status: "ok",
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(
		"telemetry.spans.engine.r1", data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for received.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("exporter did not POST within 10s")
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()

	if received.Load() < 1 {
		t.Fatal("expected at least 1 POST to Jaeger")
	}
}

func TestExportToJaegerHandlesFailure(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	var requestCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		},
	))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 8*time.Second)
	defer cancel()

	logger := observe.NewNoopLogger()
	go ExportToJaeger(ctx, js, srv.URL, logger)

	time.Sleep(200 * time.Millisecond)

	rec := SpanRecord{
		TraceID: "t1", SpanID: "s1", Name: "test",
		Service: "engine", Status: "ok",
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(
		"telemetry.spans.engine.r1", data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(7 * time.Second)
	for requestCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal(
				"exporter did not attempt POST within 7s",
			)
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()

	if requestCount.Load() < 1 {
		t.Fatal(
			"exporter should have attempted at least 1 POST",
		)
	}
}

func TestExportToJaegerPanicsOnBadArgs(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	logger := observe.NewNoopLogger()

	assertPanics(t, "nil js", func() {
		ExportToJaeger(
			context.Background(), nil, "http://x", logger,
		)
	})
	assertPanics(t, "empty endpoint", func() {
		ExportToJaeger(
			context.Background(), js, "", logger,
		)
	})
	assertPanics(t, "nil logger", func() {
		ExportToJaeger(
			context.Background(), js, "http://x", nil,
		)
	})
}

func assertPanics(
	t *testing.T, name string, fn func(),
) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("%s: expected panic, got none", name)
		}
	}()
	fn()
}

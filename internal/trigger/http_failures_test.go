// trigger/http_failures_test.go
//
// Methodology: integration tests with embedded NATS exercising the
// ADR-013 §"Failure handling" matrix. Each test stands up its own
// NATS server, registers the HTTPHandler, drives a request through
// httptest, then publishes the failure-signalling event the engine
// would normally emit (workflow.failed / workflow.cancelled). The
// handler must map each signal to the documented HTTP outcome and
// always preserve the X-Dagnats-Run-Id header. Bounded waits — no
// unbounded polling.
package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startFakeFailingEngine watches history.> for the workflow.started
// event the handler publishes, then publishes the configured failure
// event (workflow.failed or workflow.cancelled) for that runID. The
// signal arrives via the same history.<runID> subject the real engine
// uses, so the handler's failure-observer subscription sees it.
func startFakeFailingEngine(
	t *testing.T, nc *nats.Conn,
	failEvent protocol.EventType,
	delay time.Duration,
) func() {
	t.Helper()
	if failEvent == "" {
		t.Fatal("startFakeFailingEngine: failEvent must not be empty")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync(
		"history.>", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	seen := make(map[string]bool, 4)
	var mu sync.Mutex

	go func() {
		defer close(done)
		for i := 0; i < 4096; i++ {
			select {
			case <-stop:
				return
			default:
			}
			msg, err := sub.NextMsg(50 * time.Millisecond)
			if err != nil {
				continue
			}
			var evt protocol.Event
			if err := json.Unmarshal(msg.Data, &evt); err != nil {
				continue
			}
			if evt.Type != protocol.EventWorkflowStarted {
				continue
			}
			mu.Lock()
			if seen[evt.RunID] {
				mu.Unlock()
				continue
			}
			seen[evt.RunID] = true
			mu.Unlock()
			go publishFailureAfter(t, nc, evt.RunID, failEvent, delay)
		}
	}()

	return func() {
		close(stop)
		<-done
		_ = sub.Unsubscribe()
	}
}

// publishFailureAfter sleeps then publishes the failure event so the
// handler's failure observer sees it after the subscription is open.
func publishFailureAfter(
	t *testing.T, nc *nats.Conn, runID string,
	failEvent protocol.EventType, delay time.Duration,
) {
	t.Helper()
	if runID == "" {
		t.Errorf("publishFailureAfter: empty runID")
		return
	}
	time.Sleep(delay)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Errorf("jetstream.New: %v", err)
		return
	}
	evt := protocol.NewWorkflowEvent(failEvent, runID, nil)
	data, err := evt.Marshal()
	if err != nil {
		t.Errorf("marshal failure event: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	_, err = js.Publish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	if err != nil {
		t.Errorf("publish failure event: %v", err)
	}
}

func TestHTTPHandlerWorkflowFailedReturns500(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-fail",
		WorkflowID: "wf-fail",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/fail",
			Method:       http.MethodPost,
			TimeoutMs:    5_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := NewHTTPHandler(nc, def)

	stop := startFakeFailingEngine(
		t, nc, protocol.EventWorkflowFailed, 50*time.Millisecond,
	)
	defer stop()

	req := httptest.NewRequest(
		http.MethodPost, "/api/fail",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"error":"workflow_failed"`) {
		t.Fatalf("body = %q, want workflow_failed error", rec.Body.String())
	}
	if rec.Header().Get("X-Dagnats-Run-Id") == "" {
		t.Fatal("X-Dagnats-Run-Id missing on failure")
	}
}

func TestHTTPHandlerWorkflowCancelledByEngineReturns503(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-cancelled",
		WorkflowID: "wf-cancelled",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/cancel",
			Method:       http.MethodPost,
			TimeoutMs:    5_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := NewHTTPHandler(nc, def)

	stop := startFakeFailingEngine(
		t, nc, protocol.EventWorkflowCancelled, 50*time.Millisecond,
	)
	defer stop()

	req := httptest.NewRequest(
		http.MethodPost, "/api/cancel",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"error":"workflow_cancelled"`) {
		t.Fatalf("body = %q, want workflow_cancelled error",
			rec.Body.String())
	}
	if rec.Header().Get("X-Dagnats-Run-Id") == "" {
		t.Fatal("X-Dagnats-Run-Id missing on cancel")
	}
}

func TestHTTPHandlerClientClosedReturns499(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-clientclose",
		WorkflowID: "wf-clientclose",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/clientclose",
			Method:       http.MethodPost,
			TimeoutMs:    5_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := NewHTTPHandler(nc, def)

	// No fake engine — request context cancels before any signal.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	req := httptest.NewRequestWithContext(
		ctx, http.MethodPost, "/api/clientclose",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// 499 is the nginx-style "client closed request". We adopt it
	// because no standard HTTP code distinguishes it from 5xx;
	// observability tooling that already knows 499 benefits.
	if rec.Code != 499 {
		t.Fatalf("status = %d, want 499 (client closed)", rec.Code)
	}
	if rec.Header().Get("X-Dagnats-Run-Id") == "" {
		t.Fatal("X-Dagnats-Run-Id missing on client close")
	}
}

func TestHTTPHandlerTimeoutReturns504(t *testing.T) {
	// Regression for the per-request timeout path. The handler should
	// already 504 on timeout (PR 2 covered this); PR 3 adds the
	// failure-observer subscription which must NOT break the timeout
	// path when no failure signal arrives.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-timeout",
		WorkflowID: "wf-timeout",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/timeout",
			Method:       http.MethodPost,
			TimeoutMs:    200,
			MaxBodyBytes: 1024,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := NewHTTPHandler(nc, def)

	req := httptest.NewRequest(
		http.MethodPost, "/api/timeout",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"error":"workflow_timeout"`) {
		t.Fatalf("body = %q, want workflow_timeout error",
			rec.Body.String())
	}
}

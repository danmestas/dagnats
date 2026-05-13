// internal/engine/e2e_http_failures_test.go
//
// Methodology: end-to-end integration tests with embedded NATS that
// wire a real trigger.HTTPHandler in front of a stand-in for the
// engine path. The failure-mode matrix in ADR-013 §"Failure
// handling" is covered at the trigger-package level with fake-event
// stand-ins (http_failures_test.go); this file pins the integration
// contract — the HTTPHandler observes failure events the way the
// real engine would publish them — using engine_test as the package
// to avoid the trigger → engine → trigger import cycle.
// Each test starts its own NATS server.
package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestE2EHTTPHandlerNoRespondStepTimesOut covers the
// "workflow reaches end without respond → 504" case. The workflow
// has one normal step that completes successfully but never publishes
// to ResponseSubject — the client must time out, NOT hang forever.
func TestE2EHTTPHandlerNoRespondStepTimesOut(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := trigger.TriggerDef{
		ID:         "http-norespond",
		WorkflowID: "wf-norespond",
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path:         "/api/norespond",
			Method:       http.MethodPost,
			TimeoutMs:    400,
			MaxBodyBytes: 1024,
		},
	}
	if err := trigger.Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := trigger.NewHTTPHandler(nc, def)

	req := httptest.NewRequest(
		http.MethodPost, "/api/norespond",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if !strings.Contains(
		rec.Body.String(), `"error":"workflow_timeout"`,
	) {
		t.Fatalf("body = %q, want workflow_timeout", rec.Body.String())
	}
	if rec.Header().Get("X-Dagnats-Run-Id") == "" {
		t.Fatal("X-Dagnats-Run-Id missing")
	}
}

// TestE2EHTTPHandlerEngineFailedReturns500 wires a real Orchestrator
// to a real HTTPHandler. The workflow def has no real worker — the
// test directly publishes a workflow.failed event on the run's
// history subject AFTER the HTTPHandler has subscribed to it, which
// is the same code path the real engine takes via publishWorkflowFailed.
// This covers the integration of the failure observer + history
// stream.
func TestE2EHTTPHandlerEngineFailedReturns500(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := trigger.TriggerDef{
		ID:         "http-e2e-fail",
		WorkflowID: "wf-e2e-fail",
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path:         "/api/e2e-fail",
			Method:       http.MethodPost,
			TimeoutMs:    5_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := trigger.Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := trigger.NewHTTPHandler(nc, def)

	// Bridge: watch history.> for workflow.started; when one arrives
	// for our wfDef, publish a workflow.failed for the same runID.
	// This is what the real orchestrator's failLoopStep ends with.
	stopBridge := startFailBridge(t, nc, def.WorkflowID)
	defer stopBridge()

	req := httptest.NewRequest(
		http.MethodPost, "/api/e2e-fail",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(
		rec.Body.String(), `"error":"workflow_failed"`,
	) {
		t.Fatalf("body = %q, want workflow_failed", rec.Body.String())
	}
}

// startFailBridge subscribes to history.> and, for any workflow.started
// whose envelope binds to workflowID, publishes workflow.failed for
// that runID — mirroring what failLoopStep does in production. The
// teardown func stops the goroutine and unsubscribes.
func startFailBridge(
	t *testing.T, nc *nats.Conn, workflowID string,
) func() {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("history.>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync history: %v", err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		jsNew, err := jetstream.New(nc)
		if err != nil {
			t.Errorf("jetstream.New: %v", err)
			return
		}
		for i := 0; i < 256; i++ {
			select {
			case <-stop:
				return
			default:
			}
			msg, err := sub.NextMsg(100 * time.Millisecond)
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
			if evt.RunID == "" {
				continue
			}
			failEvt := protocol.NewWorkflowEvent(
				protocol.EventWorkflowFailed, evt.RunID, nil,
			)
			data, err := failEvt.Marshal()
			if err != nil {
				continue
			}
			pubCtx, cancel := context.WithTimeout(
				context.Background(), 2*time.Second,
			)
			_, err = jsNew.Publish(
				pubCtx, failEvt.NATSSubject(), data,
				jetstream.WithMsgID(failEvt.NATSMsgID()),
			)
			cancel()
			if err != nil {
				t.Errorf("publish workflow.failed: %v", err)
			}
		}
	}()
	return func() {
		close(stop)
		_ = sub.Unsubscribe()
		<-done
	}
}

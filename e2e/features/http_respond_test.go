// e2e/features/http_respond_test.go
//
// Methodology: end-to-end happy path for ADR-013. Registers a
// workflow with an HTTP trigger and a single respond step, starts
// the engine, opens an in-process HTTP test request, and asserts
// that the response comes from the respond step's published body.
//
// "End-to-end" here means: real NATS, real engine, real trigger
// service, real HTTPHandler dispatching to the respond step
// executor. No fakes — the only test surface is httptest for the
// inbound HTTP request because we don't want to bind a real socket
// inside CI.
package features

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
)

func TestHTTPRespondHappyPath(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStream: %v", err)
		}
		// triggers/trigger_state KV buckets are not provisioned by
		// the default harness — match webhook_test.go's pattern.
		if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: "triggers",
		}); err != nil {
			t.Fatalf("create triggers KV: %v", err)
		}
		if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: "trigger_state",
		}); err != nil {
			t.Fatalf("create trigger_state KV: %v", err)
		}

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		wfName := harness.UniqueName(t, "http-respond-wf")
		wfDef := buildHTTPRespondWorkflow(t, wfName)
		if err := svc.RegisterWorkflow(ctx, wfDef); err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Start the engine so respond steps actually execute.
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(orch.Stop)

		// Start the trigger service so the HTTPHandler is registered.
		ts, err := trigger.NewTriggerService(nc, "1.0.0")
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		if err := ts.Start(); err != nil {
			t.Fatalf("TriggerService.Start: %v", err)
		}
		t.Cleanup(ts.Stop)

		triggerPath := "/" + harness.UniqueName(t, "respond")
		triggerID := harness.UniqueName(t, "http-trig")
		triggerDef := trigger.TriggerDef{
			ID:         triggerID,
			WorkflowID: wfName,
			Enabled:    true,
			HTTP: &trigger.HTTPConfig{
				Path:         triggerPath,
				Method:       http.MethodPost,
				TimeoutMs:    10_000,
				MaxBodyBytes: 1024,
			},
		}
		if err := svc.CreateTrigger(ctx, triggerDef); err != nil {
			t.Fatalf("CreateTrigger: %v", err)
		}

		// Allow the trigger KV watcher to react.
		waitForRoute(t, ts, http.MethodPost, triggerPath, 3*time.Second)

		router := ts.HTTPRouter()
		req := httptest.NewRequest(
			http.MethodPost, triggerPath,
			bytes.NewReader([]byte(`{"name":"alice"}`)),
		)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != 201 {
			t.Fatalf(
				"status = %d, want 201; body=%s",
				rec.Code, rec.Body.String(),
			)
		}
		if got := rec.Header().Get("X-Dagnats-Run-Id"); got == "" {
			t.Fatal("X-Dagnats-Run-Id header missing on response")
		}
		// The respond step echoes the workflow input verbatim
		// (BodyFrom unset). The orchestrator sets run.Input to the
		// TriggerEnvelope JSON, so the response body is the envelope
		// we publish. Spot-check that the trigger payload made the
		// round-trip rather than asserting the exact bytes — the
		// timestamp and request_id in the envelope vary per run.
		body := rec.Body.String()
		if !bytes.Contains([]byte(body), []byte(`"trigger":"http"`)) {
			t.Fatalf("body = %q, want envelope echo", body)
		}
		if !bytes.Contains([]byte(body), []byte(wfName)) {
			t.Fatalf(
				"body = %q, must contain workflow id %q",
				body, wfName,
			)
		}
	})
}

// buildHTTPRespondWorkflow defines a one-step workflow whose only
// node is a respond step echoing the workflow input (the trigger
// envelope JSON) with a 201 status.
func buildHTTPRespondWorkflow(
	t *testing.T, wfName string,
) dag.WorkflowDef {
	t.Helper()
	respCfg, err := json.Marshal(dag.RespondConfig{
		Status: 201,
	})
	if err != nil {
		t.Fatalf("marshal RespondConfig: %v", err)
	}
	return dag.WorkflowDef{
		Name:    wfName,
		Version: "v1",
		Steps: []dag.StepDef{
			{
				ID:     "respond",
				Type:   dag.StepTypeRespond,
				Config: respCfg,
			},
		},
	}
}

// waitForRoute polls the HTTPRouter until the (method, path) route
// resolves to anything other than 404. Bounded — does not poll
// forever. This is the deterministic alternative to time.Sleep
// after a KV.Put: the watcher's reaction time is non-deterministic
// across topologies (slower under supercluster), so a fixed sleep
// is the wrong tool.
func waitForRoute(
	t *testing.T, ts *trigger.TriggerService,
	method string, path string, timeout time.Duration,
) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatalf(
				"waitForRoute: %s %s not registered within %s",
				method, path, timeout,
			)
		default:
		}
		probe := httptest.NewRequest(method, path, nil)
		rec := httptest.NewRecorder()
		ts.HTTPRouter().ServeHTTP(rec, probe)
		// Anything other than 404 means the route is wired. The
		// trigger handler may return 504/413/400 depending on the
		// probe body; we only care that 404 has gone away.
		if rec.Code != http.StatusNotFound {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

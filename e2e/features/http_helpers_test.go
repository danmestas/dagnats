// e2e/features/http_helpers_test.go
//
// Methodology: shared end-to-end helpers for the ADR-013 HTTP trigger
// + respond step test suite. Each helper assumes the harness has
// stood up a real embedded NATS server and SetupAll has provisioned
// the full KV/stream surface (including http_idempotency). These
// helpers DO NOT fake the engine or the trigger service — the only
// stand-in we accept is httptest, because binding a real socket
// inside CI is undesirable.
//
// All helpers live in features_test (not in e2e/harness) because
// they are HTTP-trigger-specific and would otherwise pollute the
// general E2E harness with ADR-013-only plumbing.
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
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
)

// httpE2EStack groups the five pieces every HTTP-trigger E2E test
// needs: NATS connection (for worker subscription), API service for
// control-plane mutations, orchestrator for step execution, trigger
// service for the HTTP router, and a context for trigger CRUD.
// Returned struct + cleanup keeps individual tests short.
type httpE2EStack struct {
	nc     *nats.Conn
	svc    *api.Service
	orch   *engine.Orchestrator
	ts     *trigger.TriggerService
	router http.Handler
	ctx    context.Context
}

// startHTTPE2EStack provisions the trigger KV buckets the default
// harness does not create, wires the API service, orchestrator, and
// trigger service, and returns them ready-to-use. Cleanup is
// registered on t so callers do not have to track lifecycles.
func startHTTPE2EStack(
	t *testing.T, nc *nats.Conn,
) *httpE2EStack {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("startHTTPE2EStack: JetStream: %v", err)
	}
	// Trigger KV buckets are NOT in the default harness setup; mirror
	// http_respond_test.go (already-shipped happy path) here.
	if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "triggers",
	}); err != nil {
		t.Fatalf("startHTTPE2EStack: triggers KV: %v", err)
	}
	if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "trigger_state",
	}); err != nil {
		t.Fatalf("startHTTPE2EStack: trigger_state KV: %v", err)
	}

	svc := harness.NewTestService(t, nc)
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	ts, err := trigger.NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("startHTTPE2EStack: NewTriggerService: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("startHTTPE2EStack: ts.Start: %v", err)
	}
	t.Cleanup(ts.Stop)

	return &httpE2EStack{
		nc:     nc,
		svc:    svc,
		orch:   orch,
		ts:     ts,
		router: ts.HTTPRouter(),
		ctx:    context.Background(),
	}
}

// registerHTTPTrigger registers a workflow + HTTP trigger pair and
// waits until the trigger service's HTTPRouter resolves the route
// (no 404). Returns the trigger ID + path so callers can issue
// requests or remove the trigger later. Bounded wait: 3s ceiling
// matches http_respond_test.go's waitForRoute helper.
func (s *httpE2EStack) registerHTTPTrigger(
	t *testing.T,
	wfDef dag.WorkflowDef,
	cfg *trigger.HTTPConfig,
) (triggerID string, triggerPath string) {
	t.Helper()
	if cfg == nil {
		panic("registerHTTPTrigger: cfg must not be nil")
	}
	if cfg.Path == "" {
		panic("registerHTTPTrigger: cfg.Path must not be empty")
	}
	if err := s.svc.RegisterWorkflow(s.ctx, wfDef); err != nil {
		t.Fatalf("RegisterWorkflow %q: %v", wfDef.Name, err)
	}
	tid := harness.UniqueName(t, "http-trig")
	def := trigger.TriggerDef{
		ID:         tid,
		WorkflowID: wfDef.Name,
		Enabled:    true,
		HTTP:       cfg,
	}
	if err := s.svc.CreateTrigger(s.ctx, def); err != nil {
		t.Fatalf("CreateTrigger %q: %v", tid, err)
	}
	waitForHTTPRoute(t, s.ts, cfg.Method, cfg.Path, 3*time.Second)
	return tid, cfg.Path
}

// waitForHTTPRoute polls the HTTPRouter until the (method, path)
// route resolves to "registered". Bounded — the KV watcher reacts
// asynchronously to KV.Put, so a fixed sleep would be flaky across
// topologies.
//
// Probe technique: send an OPTIONS request (not in the v1 allowed
// method set). The router returns 405 once the path is registered
// (regardless of which method is registered there) and 404 while
// the watcher has not yet wired the route. This is critical for
// tests with blocking workers — a probe that uses the registered
// method would FIRE the workflow and the worker would block on the
// probe's event, starving the actual test request.
func waitForHTTPRoute(
	t *testing.T, ts *trigger.TriggerService,
	method string, path string, budget time.Duration,
) {
	t.Helper()
	_ = method // probe always uses OPTIONS — see above
	deadline := time.Now().Add(budget)
	// Bounded iteration: budget/50ms + safety margin caps the loop.
	const maxIter = 1000
	for i := 0; i < maxIter; i++ {
		if time.Now().After(deadline) {
			t.Fatalf("waitForHTTPRoute: %s %s not ready in %s",
				method, path, budget)
		}
		probe := httptest.NewRequest(http.MethodOptions, path, nil)
		rec := httptest.NewRecorder()
		ts.HTTPRouter().ServeHTTP(rec, probe)
		// 405 means the path is in httpRoutes but the method
		// doesn't match — exactly the "wired but won't fire the
		// workflow" state we need. 404 means the watcher hasn't
		// reacted yet.
		if rec.Code == http.StatusMethodNotAllowed {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waitForHTTPRoute: exceeded %d iterations", maxIter)
}

// respondStepDef builds a StepDef for a respond step. Keeping it in
// one place avoids drift between the many tests that need the
// equivalent of `{Type: Respond, Config: {Status: N, BodyFrom: X}}`.
func respondStepDef(
	t *testing.T, id string,
	dependsOn []string,
	cfg dag.RespondConfig,
) dag.StepDef {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("respondStepDef: marshal RespondConfig: %v", err)
	}
	return dag.StepDef{
		ID:        id,
		Type:      dag.StepTypeRespond,
		DependsOn: dependsOn,
		Config:    raw,
	}
}

// postOnRouter sends a request through the HTTPRouter using
// httptest. Returns the recorder so callers can inspect status,
// headers, and body. Body is supplied as a byte slice so binary or
// pre-signed payloads flow unchanged.
func postOnRouter(
	t *testing.T, router http.Handler,
	method string, path string, body []byte,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	if router == nil {
		panic("postOnRouter: router must not be nil")
	}
	var req *http.Request
	if len(body) == 0 {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path,
			bytes.NewReader(body))
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// api/rest_test.go
// Tests for REST API endpoints using net/http/httptest.
// Methodology: create a test service with real NATS, make HTTP requests
// via httptest.Server, verify response codes and JSON bodies.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestRESTRegisterWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()
	wb := dag.NewWorkflow("rest-test")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	body, _ := json.Marshal(wfDef)
	resp, err := http.Post(
		server.URL+"/workflows",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusCreated)
	}
}

func TestRESTStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()
	wb := dag.NewWorkflow("rest-run")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)
	body := []byte(`{"workflow": "rest-run", "input": "test"}`)
	resp, err := http.Post(
		server.URL+"/runs",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusCreated)
	}
	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if result["run_id"] == "" {
		t.Fatal("response missing run_id")
	}
}

func TestRESTGetRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()
	wb := dag.NewWorkflow("rest-get")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)
	runID, _ := svc.StartRun(
		context.Background(), "rest-get", nil,
	)

	// Poll until snapshot is available (bounded to 5s).
	deadline := time.After(5 * time.Second)
	var run dag.WorkflowRun
	for {
		resp, err := http.Get(server.URL + "/runs/" + runID)
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		if resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			break
		}
		resp.Body.Close()
		select {
		case <-deadline:
			t.Fatalf("run snapshot did not appear within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if run.RunID != runID {
		t.Fatalf("RunID = %q, want %q", run.RunID, runID)
	}
}

func TestRESTGetRunNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()
	resp, err := http.Get(server.URL + "/runs/nonexistent")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusNotFound)
	}
}

func TestRESTHealthBasic(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()
	resp, err := http.Get(server.URL + "/health/telemetry")
	if err != nil {
		t.Fatalf("GET /health/telemetry failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusOK)
	}
	var health healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if health.Status != "healthy" {
		t.Fatalf("Status = %q, want %q",
			health.Status, "healthy")
	}
	// SetupAll creates the TELEMETRY stream, so telemetry info
	// should be present with the stream data populated.
	if health.Telemetry == nil {
		t.Fatal("expected telemetry info when stream exists")
	}
	if health.Telemetry.Stream == nil {
		t.Fatal("expected stream info when TELEMETRY exists")
	}
}

func TestNewRESTHandlerPanicsNilSvc(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil svc")
		}
	}()
	NewRESTHandler(nil)
}

func TestRESTRegisterWorkflowBadJSON(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: invalid JSON returns 400.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost, "/workflows",
		bytes.NewReader([]byte("not-json")),
	)
	handleRegisterWorkflow(svc, rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusBadRequest)
	}

	// Negative: valid JSON with invalid def also returns 400.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(
		http.MethodPost, "/workflows",
		bytes.NewReader([]byte(`{"name":""}`)),
	)
	handleRegisterWorkflow(svc, rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			rec2.Code, http.StatusBadRequest)
	}
}

func TestRESTStartRunBadJSON(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: invalid JSON returns 400.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost, "/runs",
		bytes.NewReader([]byte("bad")),
	)
	handleStartRun(svc, rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusBadRequest)
	}

	// Negative: valid JSON but unknown workflow returns 400.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(
		http.MethodPost, "/runs",
		bytes.NewReader([]byte(`{"workflow":"nope"}`)),
	)
	handleStartRun(svc, rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			rec2.Code, http.StatusBadRequest)
	}
}

func TestRESTGetRunMissingID(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: missing run ID returns 400.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet, "/runs/", nil,
	)
	handleGetRun(svc, rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusBadRequest)
	}

	// Negative: nonexistent run returns 404.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(
		http.MethodGet, "/runs/no-such-run", nil,
	)
	handleGetRun(svc, rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d",
			rec2.Code, http.StatusNotFound)
	}
}

func TestRESTRegisterWorkflowInvalidDef(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Positive: empty workflow def returns 400.
	body := []byte(`{"name":"","steps":[]}`)
	resp, err := http.Post(
		server.URL+"/workflows",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusBadRequest)
	}

	// Negative: valid def does not return 400.
	wb := dag.NewWorkflow("rest-valid")
	wb.Task("a", "task-a")
	def, _ := wb.Build()
	goodBody, _ := json.Marshal(def)
	resp2, err := http.Post(
		server.URL+"/workflows",
		"application/json",
		bytes.NewReader(goodBody),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp2.StatusCode == http.StatusBadRequest {
		t.Fatal("valid def should not return 400")
	}
}

func TestRESTStartRunUnknownWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Positive: unknown workflow returns 400.
	body := []byte(`{"workflow":"nonexistent"}`)
	resp, err := http.Post(
		server.URL+"/runs",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusBadRequest)
	}

	// Negative: known workflow does not return 400.
	wb := dag.NewWorkflow("rest-known")
	wb.Task("a", "task-a")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)
	body2 := []byte(`{"workflow":"rest-known"}`)
	resp2, err := http.Post(
		server.URL+"/runs",
		"application/json",
		bytes.NewReader(body2),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp2.StatusCode == http.StatusBadRequest {
		t.Fatal("known workflow should not return 400")
	}
}

func TestRouteWorkflowsRejectsGET(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: GET returns 405.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	svc.routeWorkflows(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusMethodNotAllowed)
	}

	// Negative: POST is not rejected (non-405).
	rec2 := httptest.NewRecorder()
	body := []byte(`{"name":"x","steps":[]}`)
	req2 := httptest.NewRequest(
		http.MethodPost, "/workflows",
		bytes.NewReader(body),
	)
	svc.routeWorkflows(rec2, req2)
	if rec2.Code == http.StatusMethodNotAllowed {
		t.Fatal("POST should not return 405")
	}
}

func TestRouteRunsRejectsGET(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: GET returns 405.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/runs", nil)
	svc.routeRuns(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusMethodNotAllowed)
	}

	// Negative: POST is not rejected.
	rec2 := httptest.NewRecorder()
	body := []byte(`{"workflow":"x"}`)
	req2 := httptest.NewRequest(
		http.MethodPost, "/runs",
		bytes.NewReader(body),
	)
	svc.routeRuns(rec2, req2)
	if rec2.Code == http.StatusMethodNotAllowed {
		t.Fatal("POST should not return 405")
	}
}

func TestRouteRunByIDRejectsPOST(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: POST returns 405.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost, "/runs/some-id", nil,
	)
	svc.routeRunByID(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusMethodNotAllowed)
	}

	// Negative: GET is not rejected.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(
		http.MethodGet, "/runs/some-id", nil,
	)
	svc.routeRunByID(rec2, req2)
	if rec2.Code == http.StatusMethodNotAllowed {
		t.Fatal("GET should not return 405")
	}
}

func TestRouteHealthRejectsPOST(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: POST returns 405.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost, "/health/telemetry", nil,
	)
	svc.routeHealth(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusMethodNotAllowed)
	}

	// Negative: GET is not rejected.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(
		http.MethodGet, "/health/telemetry", nil,
	)
	svc.routeHealth(rec2, req2)
	if rec2.Code == http.StatusMethodNotAllowed {
		t.Fatal("GET should not return 405")
	}
}

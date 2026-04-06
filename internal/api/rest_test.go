// api/rest_test.go
// Tests for REST API endpoints using net/http/httptest.
// Methodology: create a test service with real NATS, make HTTP requests
// via httptest.Server, verify response codes and JSON bodies.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestRESTRegisterWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
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
	svc := NewService(nc)
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

	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)

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
		bytes.NewReader([]byte(`{"name":"bad","steps":[]}`)),
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
	svc := NewService(nc)

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
	svc := NewService(nc)

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
	svc := NewService(nc)
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Positive: empty workflow def returns 400.
	body := []byte(`{"name":"bad-wf","steps":[]}`)
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
	svc := NewService(nc)
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

func TestRouteWorkflowsRejectsPUT(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)

	// Positive: PUT returns 405.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/workflows", nil)
	svc.routeWorkflows(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusMethodNotAllowed)
	}

	// Negative: GET is not rejected.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(
		http.MethodGet, "/workflows", nil,
	)
	svc.routeWorkflows(rec2, req2)
	if rec2.Code == http.StatusMethodNotAllowed {
		t.Fatal("GET should not return 405")
	}
}

func TestRESTListWorkflows(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Register a workflow first.
	wb := dag.NewWorkflow("list-test")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Positive: GET /workflows returns 200 with array.
	resp, err := http.Get(server.URL + "/workflows")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusOK)
	}
	var defs []dag.WorkflowDef
	if err := json.NewDecoder(resp.Body).Decode(&defs); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if len(defs) == 0 {
		t.Fatal("expected at least one workflow")
	}

	// Negative: list is not empty after registration.
	found := false
	for _, d := range defs {
		if d.Name == "list-test" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("registered workflow not in list")
	}
}

func TestRouteRunsRejectsPUT(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)

	// Positive: PUT returns 405.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/runs", nil)
	svc.routeRuns(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d",
			rec.Code, http.StatusMethodNotAllowed)
	}

	// Negative: GET is not rejected.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(
		http.MethodGet, "/runs", nil,
	)
	svc.routeRuns(rec2, req2)
	if rec2.Code == http.StatusMethodNotAllowed {
		t.Fatal("GET should not return 405")
	}
}

func TestRESTListRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	wb := dag.NewWorkflow("list-runs-test")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)
	runID, _ := svc.StartRun(
		context.Background(), "list-runs-test", nil,
	)

	// Poll until snapshot exists.
	deadline := time.After(5 * time.Second)
	for {
		_, err := svc.GetRun(context.Background(), runID)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("run snapshot did not appear within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Positive: GET /runs returns 200 with array.
	resp, err := http.Get(server.URL + "/runs")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusOK)
	}
	var runs []dag.WorkflowRun
	if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Negative: list should not be empty.
	if len(runs) == 0 {
		t.Fatal("expected at least one run")
	}
}

func TestRouteRunByIDRejectsPUT(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)

	// Positive: PUT returns 405.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPut, "/runs/some-id", nil,
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

func TestRESTCancelRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	wb := dag.NewWorkflow("cancel-test")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)
	runID, _ := svc.StartRun(
		context.Background(), "cancel-test", nil,
	)

	// Positive: POST /runs/{id}/cancel returns 200.
	resp, err := http.Post(
		server.URL+"/runs/"+runID+"/cancel",
		"application/json", nil,
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusOK)
	}

	// Negative: GET on cancel path returns 405.
	resp2, err := http.Get(
		server.URL + "/runs/" + runID + "/cancel",
	)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d",
			resp2.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestRESTSendSignal(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "signals"},
		),
	)
	svc := NewService(nc)
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Positive: POST /runs/{id}/signal/{name} returns 200.
	resp, err := http.Post(
		server.URL+"/runs/test-run/signal/approve",
		"application/json",
		bytes.NewReader([]byte(`{"approved":true}`)),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusOK)
	}

	// Negative: missing signal name returns 400.
	resp2, err := http.Post(
		server.URL+"/runs/test-run/signal/",
		"application/json", nil,
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d",
			resp2.StatusCode, http.StatusBadRequest)
	}
}

func TestRouteHealthRejectsPOST(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)

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

func TestRESTStartScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("rest-sched")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	handler := NewRESTHandler(svc)

	runAt := time.Now().Add(1 * time.Hour).UTC()
	body := fmt.Sprintf(
		`{"workflow":"rest-sched","run_at":"%s"}`,
		runAt.Format(time.RFC3339),
	)
	req := httptest.NewRequest(
		"POST", "/runs",
		strings.NewReader(body),
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Positive: returns 201.
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s",
			w.Code, w.Body.String())
	}

	// Positive: response contains run_id and status=scheduled.
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["run_id"] == "" {
		t.Fatal("run_id should not be empty")
	}
	if resp["status"] != "scheduled" {
		t.Fatalf("status = %q, want scheduled", resp["status"])
	}
}

func TestRESTGetScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("rest-get-sched")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	runAt := time.Now().Add(1 * time.Hour)
	runID, err := svc.ScheduleRun(
		context.Background(), "rest-get-sched", nil, runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	handler := NewRESTHandler(svc)
	req := httptest.NewRequest(
		"GET", "/runs/"+runID+"/scheduled", nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Positive: returns 200.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	// Positive: response contains correct RunID.
	var sr ScheduledRun
	json.Unmarshal(w.Body.Bytes(), &sr)
	if sr.RunID != runID {
		t.Fatalf("RunID = %q, want %q", sr.RunID, runID)
	}
}

func TestRESTCancelScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("rest-cancel-sched")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	runAt := time.Now().Add(1 * time.Hour)
	runID, err := svc.ScheduleRun(
		context.Background(), "rest-cancel-sched", nil, runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	handler := NewRESTHandler(svc)
	req := httptest.NewRequest(
		"DELETE", "/runs/"+runID+"/scheduled", nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Positive: returns 200.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	// Positive: status is now cancelled in KV.
	sr, err := svc.GetScheduledRun(runID)
	if err != nil {
		t.Fatalf("GetScheduledRun: %v", err)
	}
	if sr.Status != "cancelled" {
		t.Fatalf("Status = %q, want cancelled", sr.Status)
	}
}

func TestRESTBulkCancel(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("rest-bulk-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)
	svc.StartRun(context.Background(), "rest-bulk-wf", nil)
	svc.StartRun(context.Background(), "rest-bulk-wf", nil)
	time.Sleep(200 * time.Millisecond)

	handler := NewRESTHandler(svc)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	body := `{"workflow_id":"rest-bulk-wf"}`
	resp, err := http.Post(
		ts.URL+"/runs/cancel",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /runs/cancel: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result BulkCancelResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if len(result.Cancelled) != 2 {
		t.Fatalf("cancelled = %d, want 2",
			len(result.Cancelled))
	}

	// Negative: missing workflow_id returns 400
	resp2, err2 := http.Post(
		ts.URL+"/runs/cancel",
		"application/json",
		strings.NewReader(`{}`),
	)
	if err2 != nil {
		t.Fatalf("POST /runs/cancel: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp2.StatusCode)
	}
}

func TestRESTBulkRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	wb := dag.NewWorkflow("rest-bulk-run")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	handler := NewRESTHandler(svc)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	body := `{"workflow_id":"rest-bulk-run","inputs":[{"a":1},{"a":2}]}`
	resp, err := http.Post(
		ts.URL+"/runs/bulk", "application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /runs/bulk: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var result BulkRunResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.RunIDs) != 2 {
		t.Fatalf("run_ids = %d, want 2", len(result.RunIDs))
	}

	// Negative: empty inputs returns 400
	resp2, err2 := http.Post(
		ts.URL+"/runs/bulk", "application/json",
		strings.NewReader(`{"workflow_id":"rest-bulk-run","inputs":[]}`),
	)
	if err2 != nil {
		t.Fatalf("POST /runs/bulk: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp2.StatusCode)
	}
}

func TestRESTBulkRetry(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("rest-retry-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	runID, _ := svc.StartRun(
		context.Background(), "rest-retry-wf", []byte(`{"x":1}`),
	)

	// Wait for run snapshot to appear
	deadline := time.After(5 * time.Second)
	var run dag.WorkflowRun
	for {
		r, err := svc.GetRun(context.Background(), runID)
		if err == nil {
			run = r
			break
		}
		select {
		case <-deadline:
			t.Fatalf("run snapshot did not appear within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Mark as failed and save
	run.Status = dag.RunStatusFailed
	svc.store.Save(context.Background(), run)

	handler := NewRESTHandler(svc)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	body := `{"workflow_id":"rest-retry-wf","mode":"rerun"}`
	resp, err := http.Post(
		ts.URL+"/runs/retry", "application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /runs/retry: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var result BulkRetryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Retried) != 1 {
		t.Fatalf("retried = %d, want 1", len(result.Retried))
	}

	// Negative: missing mode returns 400
	resp2, err2 := http.Post(
		ts.URL+"/runs/retry", "application/json",
		strings.NewReader(`{"workflow_id":"rest-retry-wf"}`),
	)
	if err2 != nil {
		t.Fatalf("POST /runs/retry: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp2.StatusCode)
	}
}

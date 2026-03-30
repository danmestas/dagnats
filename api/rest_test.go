// api/rest_test.go
// Tests for REST API endpoints using net/http/httptest.
// Methodology: create a test service with real NATS, make HTTP requests via
// httptest.Server, verify response codes and JSON bodies.
package api

import (
	"bytes"
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
	wfDef, _ := dag.NewWorkflow("rest-test").Task("a", "task-a").Build()
	body, _ := json.Marshal(wfDef)
	resp, err := http.Post(server.URL+"/workflows", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestRESTStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()
	wfDef, _ := dag.NewWorkflow("rest-run").Task("a", "task-a").Build()
	svc.RegisterWorkflow(wfDef)
	body := []byte(`{"workflow": "rest-run", "input": "test"}`)
	resp, err := http.Post(server.URL+"/runs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
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

	// The orchestrator owns run state — start it so the snapshot is created
	// before the REST handler queries it.
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	defer server.Close()
	wfDef, _ := dag.NewWorkflow("rest-get").Task("a", "task-a").Build()
	svc.RegisterWorkflow(wfDef)
	runID, _ := svc.StartRun("rest-get", nil)

	// Poll until the snapshot is available (orchestrator processes asynchronously,
	// bounded to 5s).
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
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

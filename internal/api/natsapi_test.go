// api/natsapi_test.go
// Tests for NATS request/reply control plane API.
// Methodology: real NATS, send request messages, verify reply payloads.
package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestNATSAPIRegisterAndStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()
	defer natsAPI.Stop()

	// Register workflow via NATS request.
	wb := dag.NewWorkflow("nats-test")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	reqData, _ := json.Marshal(wfDef)
	reply, err := nc.Request(
		"api.workflows.register", reqData, 5*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var regResp map[string]string
	if err := json.Unmarshal(reply.Data, &regResp); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if regResp["status"] != "registered" {
		t.Fatalf(
			"status = %q, want 'registered'", regResp["status"],
		)
	}

	// Start run via NATS request.
	startReq, _ := json.Marshal(
		startRunRequest{Workflow: "nats-test"},
	)
	reply, err = nc.Request(
		"api.runs.start", startReq, 5*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var startResp map[string]string
	if err := json.Unmarshal(reply.Data, &startResp); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if startResp["run_id"] == "" {
		t.Fatal("response missing run_id")
	}

	// Poll for snapshot via NATS request (bounded to 5s).
	runID := startResp["run_id"]
	deadline := time.After(5 * time.Second)
	var run dag.WorkflowRun
	for {
		reply, err = nc.Request(
			"api.runs.get", []byte(runID), 5*time.Second,
		)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		if err = json.Unmarshal(reply.Data, &run); err == nil &&
			run.RunID == runID {
			break
		}
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

func TestNewNATSAPIPanicsNilSvc(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil svc")
		}
	}()
	NewNATSAPI(nil, nc)
}

func TestNewNATSAPIPanicsNilNC(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil nc")
		}
	}()
	NewNATSAPI(svc, nil)
}

func TestNATSAPIStartCreatesSubscriptions(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()
	defer natsAPI.Stop()

	// Positive: register subject responds to requests.
	wb := dag.NewWorkflow("sub-test")
	wb.Task("a", "task-a")
	def, _ := wb.Build()
	data, _ := json.Marshal(def)
	reply, err := nc.Request(
		"api.workflows.register", data, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request to register failed: %v", err)
	}
	var resp map[string]string
	json.Unmarshal(reply.Data, &resp)
	if resp["status"] != "registered" {
		t.Fatalf("status = %q, want registered", resp["status"])
	}

	// Positive: runs.start subject responds.
	startReq, _ := json.Marshal(
		startRunRequest{Workflow: "sub-test"},
	)
	reply, err = nc.Request(
		"api.runs.start", startReq, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request to start failed: %v", err)
	}
	json.Unmarshal(reply.Data, &resp)
	if resp["run_id"] == "" {
		t.Fatal("expected non-empty run_id in reply")
	}
}

func TestNATSAPIHandleRegisterInvalidJSON(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()
	defer natsAPI.Stop()

	// Positive: invalid JSON returns error in reply.
	reply, err := nc.Request(
		"api.workflows.register",
		[]byte("not-json"),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var resp map[string]string
	json.Unmarshal(reply.Data, &resp)
	if resp["error"] == "" {
		t.Fatal("expected error for invalid JSON")
	}

	// Negative: valid JSON but no steps triggers validation error.
	reply, err = nc.Request(
		"api.workflows.register",
		[]byte(`{"name":"bad-wf","steps":[]}`),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	json.Unmarshal(reply.Data, &resp)
	if resp["error"] == "" {
		t.Fatal("expected error for empty steps")
	}
}

func TestNATSAPIHandleStartRunInvalidJSON(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()
	defer natsAPI.Stop()

	// Positive: invalid JSON returns error in reply.
	reply, err := nc.Request(
		"api.runs.start",
		[]byte("bad-json"),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var resp map[string]string
	json.Unmarshal(reply.Data, &resp)
	if resp["error"] == "" {
		t.Fatal("expected error for invalid JSON")
	}

	// Negative: valid JSON but unknown workflow returns error.
	startReq, _ := json.Marshal(
		startRunRequest{Workflow: "nonexistent"},
	)
	reply, err = nc.Request(
		"api.runs.start", startReq, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	json.Unmarshal(reply.Data, &resp)
	if resp["error"] == "" {
		t.Fatal("expected error for unknown workflow")
	}
}

func TestNATSAPIHandleGetRunNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()
	defer natsAPI.Stop()

	// Positive: nonexistent run returns error in reply.
	reply, err := nc.Request(
		"api.runs.get",
		[]byte("no-such-run"),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var resp map[string]string
	json.Unmarshal(reply.Data, &resp)
	if resp["error"] == "" {
		t.Fatal("expected error for nonexistent run")
	}

	// Negative: another nonexistent run also returns error.
	reply, err = nc.Request(
		"api.runs.get",
		[]byte("also-not-found"),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	json.Unmarshal(reply.Data, &resp)
	if resp["error"] == "" {
		t.Fatal("expected error for another nonexistent run")
	}
}

func TestNATSAPIStopUnsubscribes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()

	// Positive: subscriptions exist before Stop.
	if len(natsAPI.subs) == 0 {
		t.Fatal("expected subscriptions after Start")
	}

	natsAPI.Stop()

	// Negative: after Stop, requests should time out (no handler).
	_, err := nc.Request(
		"api.workflows.register",
		[]byte("{}"),
		200*time.Millisecond,
	)
	if err == nil {
		t.Fatal("expected timeout after Stop")
	}
}

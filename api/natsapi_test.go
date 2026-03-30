// api/natsapi_test.go
// Tests for NATS request/reply control plane API.
// Methodology: real NATS, send request messages, verify reply payloads.
package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestNATSAPIRegisterAndStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)

	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	natsAPI := NewNATSAPI(svc, nc, observe.NewNoopLogger())
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

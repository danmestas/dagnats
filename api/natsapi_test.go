// api/natsapi_test.go
// Tests for NATS request/reply control plane API.
// Methodology: real NATS, send request messages, verify reply payloads.
package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestNATSAPIRegisterAndStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc, observe.NewNoopLogger())
	natsAPI := NewNATSAPI(svc, nc)
	natsAPI.Start()
	defer natsAPI.Stop()

	// Register workflow via NATS request
	wfDef, _ := dag.NewWorkflow("nats-test").Task("a", "task-a").Build()
	reqData, _ := json.Marshal(wfDef)
	reply, err := nc.Request("api.workflows.register", reqData, 5*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var regResp map[string]string
	json.Unmarshal(reply.Data, &regResp)
	if regResp["status"] != "registered" {
		t.Fatalf("status = %q, want 'registered'", regResp["status"])
	}

	// Start run via NATS request
	startReq, _ := json.Marshal(startRunRequest{Workflow: "nats-test"})
	reply, err = nc.Request("api.runs.start", startReq, 5*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var startResp map[string]string
	json.Unmarshal(reply.Data, &startResp)
	if startResp["run_id"] == "" {
		t.Fatal("response missing run_id")
	}

	// Get run status via NATS request
	reply, err = nc.Request("api.runs.get", []byte(startResp["run_id"]), 5*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	var run dag.WorkflowRun
	json.Unmarshal(reply.Data, &run)
	if run.RunID != startResp["run_id"] {
		t.Fatalf("RunID = %q, want %q", run.RunID, startResp["run_id"])
	}
}


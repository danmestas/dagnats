// api/rest_workflow_delete_test.go
// Methodology: exercise DELETE /workflows/{name} via httptest.Server
// against a real embedded NATS server (#682). 404 on an unregistered
// name, 409 (JSON body naming the offending run IDs or trigger IDs)
// while a run is non-terminal (#682) or a trigger still references
// the workflow (#607) unless ?force=true, and 204 + removal from the
// list on success. Both guards live in Service.DeleteWorkflow, so this
// file is the REST-surface proof that it inherits both, not a
// reimplementation of either.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go/jetstream"
)

func newDeleteRESTTestServer(t *testing.T) (*httptest.Server, *engine.SnapshotStore) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := engine.NewSnapshotStore(js)
	server := httptest.NewServer(NewRESTHandler(svc))
	t.Cleanup(server.Close)
	return server, store
}

func registerRESTTestWorkflow(t *testing.T, server *httptest.Server, name string) {
	t.Helper()
	def := dag.WorkflowDef{
		Name:  name,
		Steps: []dag.StepDef{{ID: "a", Task: "task-a"}},
	}
	body := mustMarshal(t, def)
	resp, err := http.Post(
		server.URL+"/workflows", "application/json", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /workflows: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /workflows status = %d, want %d",
			resp.StatusCode, http.StatusCreated)
	}
}

func TestRESTDeleteWorkflow_NotFound(t *testing.T) {
	server, _ := newDeleteRESTTestServer(t)
	req, _ := http.NewRequest(
		http.MethodDelete, server.URL+"/workflows/nope", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	// Positive: unregistered name is 404.
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestRESTDeleteWorkflow_Success(t *testing.T) {
	server, _ := newDeleteRESTTestServer(t)
	registerRESTTestWorkflow(t, server, "rest-del")

	req, _ := http.NewRequest(
		http.MethodDelete, server.URL+"/workflows/rest-del", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	// Positive: successful delete is 204.
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Negative: it no longer appears in GET /workflows.
	listResp, err := http.Get(server.URL + "/workflows")
	if err != nil {
		t.Fatalf("GET /workflows: %v", err)
	}
	defer listResp.Body.Close()
	var entries []workflowListEntry
	if err := json.NewDecoder(listResp.Body).Decode(&entries); err != nil {
		t.Fatalf("decode /workflows: %v", err)
	}
	for _, e := range entries {
		if e.Name == "rest-del" {
			t.Fatal("deleted workflow still present in GET /workflows")
		}
	}

	// Negative: a repeated delete now 404s.
	req2, _ := http.NewRequest(
		http.MethodDelete, server.URL+"/workflows/rest-del", nil,
	)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second DELETE: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("second DELETE status = %d, want %d",
			resp2.StatusCode, http.StatusNotFound)
	}
}

func TestRESTDeleteWorkflow_ConflictOnNonTerminalRun(t *testing.T) {
	server, store := newDeleteRESTTestServer(t)
	registerRESTTestWorkflow(t, server, "rest-del-conflict")
	run := dag.WorkflowRun{
		RunID:      "run-conflict-1",
		WorkflowID: "rest-del-conflict",
		Status:     dag.RunStatusRunning,
	}
	if err := store.SaveInitial(context.Background(), run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}

	req, _ := http.NewRequest(
		http.MethodDelete, server.URL+"/workflows/rest-del-conflict", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	// Positive: non-terminal run refuses with 409.
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	var body struct {
		Error            string   `json:"error"`
		RunIDs           []string `json:"run_ids"`
		TotalNonTerminal int      `json:"total_non_terminal"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode 409 body: %v", err)
	}
	// Negative: the body names the offending run.
	found := false
	for _, id := range body.RunIDs {
		if id == "run-conflict-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("409 body should list run-conflict-1, got %v", body.RunIDs)
	}
	if body.TotalNonTerminal != 1 {
		t.Fatalf("TotalNonTerminal = %d, want 1", body.TotalNonTerminal)
	}
}

// newDeleteRESTTestServerWithTriggers is newDeleteRESTTestServer plus
// the "triggers" KV bucket, for tests exercising the trigger-reference
// guard (#607) over HTTP.
func newDeleteRESTTestServerWithTriggers(t *testing.T) (*Service, *httptest.Server) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	server := httptest.NewServer(NewRESTHandler(svc))
	t.Cleanup(server.Close)
	return svc, server
}

func TestRESTDeleteWorkflow_ConflictOnReferencingTrigger(t *testing.T) {
	svc, server := newDeleteRESTTestServerWithTriggers(t)
	registerRESTTestWorkflow(t, server, "rest-del-trig")
	trig := trigger.TriggerDef{
		ID:         "trig-ref-1",
		WorkflowID: "rest-del-trig",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "* * * * *"},
	}
	if err := svc.CreateTrigger(context.Background(), trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	req, _ := http.NewRequest(
		http.MethodDelete, server.URL+"/workflows/rest-del-trig", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	// Positive: a referencing trigger refuses with 409.
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	var body struct {
		Error      string   `json:"error"`
		TriggerIDs []string `json:"trigger_ids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode 409 body: %v", err)
	}
	// Negative: the body names the offending trigger.
	found := false
	for _, id := range body.TriggerIDs {
		if id == "trig-ref-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("409 body should list trig-ref-1, got %v", body.TriggerIDs)
	}
}

func TestRESTDeleteWorkflow_ForceBypassesTriggerConflict(t *testing.T) {
	svc, server := newDeleteRESTTestServerWithTriggers(t)
	registerRESTTestWorkflow(t, server, "rest-del-trig-force")
	trig := trigger.TriggerDef{
		ID:         "trig-ref-2",
		WorkflowID: "rest-del-trig-force",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "* * * * *"},
	}
	if err := svc.CreateTrigger(context.Background(), trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	req, _ := http.NewRequest(
		http.MethodDelete,
		server.URL+"/workflows/rest-del-trig-force?force=true", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	// Positive: force=true bypasses the trigger-reference 409.
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestRESTDeleteWorkflow_ForceBypassesConflict(t *testing.T) {
	server, store := newDeleteRESTTestServer(t)
	registerRESTTestWorkflow(t, server, "rest-del-force")
	run := dag.WorkflowRun{
		RunID:      "run-force-1",
		WorkflowID: "rest-del-force",
		Status:     dag.RunStatusRunning,
	}
	if err := store.SaveInitial(context.Background(), run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}

	req, _ := http.NewRequest(
		http.MethodDelete,
		server.URL+"/workflows/rest-del-force?force=true", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	// Positive: force=true bypasses the 409.
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

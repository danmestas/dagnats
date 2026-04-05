// api/bulk_run_test.go
// Tests for BulkStartRuns.
// Uses real embedded NATS server.
package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestBulkRunStartsMultiple(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("bulk-run-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	inputs := []json.RawMessage{
		json.RawMessage(`{"item":"a"}`),
		json.RawMessage(`{"item":"b"}`),
		json.RawMessage(`{"item":"c"}`),
	}
	resp, err := svc.BulkStartRuns(context.Background(),
		BulkRunRequest{WorkflowID: "bulk-run-wf", Inputs: inputs},
	)
	if err != nil {
		t.Fatalf("BulkStartRuns: %v", err)
	}
	if len(resp.RunIDs) != 3 {
		t.Fatalf("run_ids = %d, want 3", len(resp.RunIDs))
	}
	if resp.Total != 3 {
		t.Fatalf("total = %d, want 3", resp.Total)
	}
	seen := map[string]bool{}
	for _, id := range resp.RunIDs {
		if id == "" {
			t.Fatal("run ID must not be empty")
		}
		if seen[id] {
			t.Fatalf("duplicate run ID: %s", id)
		}
		seen[id] = true
	}
}

func TestBulkRunRequiresWorkflowID(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	_, err := svc.BulkStartRuns(context.Background(),
		BulkRunRequest{Inputs: []json.RawMessage{json.RawMessage(`{}`)}},
	)
	if err == nil {
		t.Fatal("expected error for empty workflow_id")
	}
	if err.Error() != "workflow_id is required" {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestBulkRunEmptyInputsError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	_, err := svc.BulkStartRuns(context.Background(),
		BulkRunRequest{WorkflowID: "wf"},
	)
	if err == nil {
		t.Fatal("expected error for empty inputs")
	}
	if err.Error() != "inputs must not be empty" {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestBulkRunValidationFailsAtomically(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("bulk-schema-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	def.InputSchema = json.RawMessage(
		`{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}`,
	)
	svc.RegisterWorkflow(context.Background(), def)

	inputs := []json.RawMessage{
		json.RawMessage(`{"name":"valid"}`),
		json.RawMessage(`{"wrong":"field"}`),
	}
	_, err := svc.BulkStartRuns(context.Background(),
		BulkRunRequest{WorkflowID: "bulk-schema-wf", Inputs: inputs},
	)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "input[1]") {
		t.Fatalf("error should mention input[1], got %q", err.Error())
	}
	runs, _ := svc.ListRuns(context.Background(), "bulk-schema-wf")
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs after validation fail, got %d", len(runs))
	}
}

func TestBulkRunWorkflowNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	_, err := svc.BulkStartRuns(context.Background(),
		BulkRunRequest{WorkflowID: "nonexistent", Inputs: []json.RawMessage{json.RawMessage(`{}`)}},
	)
	if err == nil {
		t.Fatal("expected error for nonexistent workflow")
	}
	if err.Error() == "" {
		t.Fatal("error message must not be empty")
	}
}

// api/service_workflow_delete_test.go
// Methodology: exercise Service.DeleteWorkflow against a real embedded
// NATS server. Delete removes the definition record only; it must fail
// on an unknown name and must never touch the workflow_runs bucket that
// holds historical run snapshots (#607).
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestDeleteWorkflowRemovesDefinition(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc)
	ctx := context.Background()

	def := dag.WorkflowDef{
		Name:  "wf-del",
		Steps: []dag.StepDef{{ID: "a", Task: "task-a"}},
	}
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	if err := svc.DeleteWorkflow(ctx, "wf-del"); err != nil {
		t.Fatalf("DeleteWorkflow: %v", err)
	}

	// Positive: definition no longer resolvable.
	if _, err := svc.GetWorkflow("wf-del"); err == nil {
		t.Fatal("workflow should be gone after delete")
	}

	// Negative: it no longer appears in the list.
	defs, err := svc.ListWorkflows(ctx)
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	for _, d := range defs {
		if d.Name == "wf-del" {
			t.Fatal("deleted workflow still present in list")
		}
	}
}

func TestDeleteWorkflowNonexistent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc)

	// Positive: deleting an unregistered name is an error, not a
	// silent success.
	err := svc.DeleteWorkflow(context.Background(), "nope")
	if err == nil {
		t.Fatal("expected error deleting nonexistent workflow")
	}

	// Negative: the error names the missing workflow.
	if !contains(err.Error(), "nope") {
		t.Fatalf("error should name the workflow, got: %v", err)
	}
}

func TestDeleteWorkflowDoesNotTouchRunHistory(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc)
	ctx := context.Background()

	def := dag.WorkflowDef{
		Name:  "wf-del",
		Steps: []dag.StepDef{{ID: "a", Task: "task-a"}},
	}
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := engine.NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID:      "run-1",
		WorkflowID: "wf-del",
		Status:     dag.RunStatusCompleted,
		CreatedAt:  time.Now(),
	}
	if err := store.Save(ctx, run); err != nil {
		t.Fatalf("store.Save: %v", err)
	}

	if err := svc.DeleteWorkflow(ctx, "wf-del"); err != nil {
		t.Fatalf("DeleteWorkflow: %v", err)
	}

	// Positive: the historical run snapshot survives the delete.
	got, err := store.Load(ctx, "run-1")
	if err != nil {
		t.Fatalf("run history destroyed by workflow delete: %v", err)
	}
	// Negative: it is the same run, not a zero value.
	if got.RunID != "run-1" || got.WorkflowID != "wf-del" {
		t.Fatalf("run snapshot corrupted: %#v", got)
	}
}

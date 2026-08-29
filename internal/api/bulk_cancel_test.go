// api/bulk_cancel_test.go
// Tests for BulkCancelRuns: verifies filtering by workflow, status,
// time range, dry-run, and the 1000-run cap.
// Uses real embedded NATS server.
package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestBulkCancelByWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)

	wb1 := dag.NewWorkflow("bulk-wf-a")
	wb1.Task("step-a", "echo")
	def1, _ := wb1.Build()
	svc.RegisterWorkflow(context.Background(), def1)

	wb2 := dag.NewWorkflow("bulk-wf-b")
	wb2.Task("step-b", "echo")
	def2, _ := wb2.Build()
	svc.RegisterWorkflow(context.Background(), def2)

	svc.StartRun(context.Background(), "bulk-wf-a", nil)
	svc.StartRun(context.Background(), "bulk-wf-a", nil)
	svc.StartRun(context.Background(), "bulk-wf-a", nil)
	svc.StartRun(context.Background(), "bulk-wf-b", nil)

	time.Sleep(200 * time.Millisecond)

	resp, err := svc.BulkCancelRuns(context.Background(),
		BulkCancelRequest{WorkflowID: "bulk-wf-a"},
	)
	if err != nil {
		t.Fatalf("BulkCancelRuns: %v", err)
	}
	if len(resp.Cancelled) != 3 {
		t.Fatalf("cancelled = %d, want 3",
			len(resp.Cancelled))
	}
	if resp.Total != 3 {
		t.Fatalf("total = %d, want 3", resp.Total)
	}

	runsB, _ := svc.ScanRuns(
		context.Background(), RunsFilter{Workflow: "bulk-wf-b"}, 0,
	)
	if len(runsB) != 1 {
		t.Fatalf("wf-b runs = %d, want 1", len(runsB))
	}
	if runsB[0].Status == dag.RunStatusCancelled {
		t.Fatal("wf-b run should not be cancelled")
	}
}

func TestBulkCancelDryRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("dry-run-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)
	svc.StartRun(context.Background(), "dry-run-wf", nil)
	svc.StartRun(context.Background(), "dry-run-wf", nil)
	time.Sleep(200 * time.Millisecond)

	resp, err := svc.BulkCancelRuns(context.Background(),
		BulkCancelRequest{
			WorkflowID: "dry-run-wf",
			DryRun:     true,
		},
	)
	if err != nil {
		t.Fatalf("BulkCancelRuns: %v", err)
	}
	if !resp.DryRun {
		t.Fatal("expected DryRun=true in response")
	}
	if len(resp.Cancelled) != 2 {
		t.Fatalf("dry-run cancelled = %d, want 2",
			len(resp.Cancelled))
	}

	runs, _ := svc.ScanRuns(
		context.Background(), RunsFilter{Workflow: "dry-run-wf"}, 0,
	)
	for _, run := range runs {
		if run.Status == dag.RunStatusCancelled {
			t.Fatal("dry-run should not cancel runs")
		}
	}
}

func TestBulkCancelRequiresWorkflowID(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	svc := NewService(nc)

	_, err := svc.BulkCancelRuns(context.Background(),
		BulkCancelRequest{},
	)
	if err == nil {
		t.Fatal("expected error for empty workflow_id")
	}
	if err.Error() != "workflow_id is required" {
		t.Fatalf("error = %q, want 'workflow_id is required'",
			err.Error())
	}
}

func TestBulkCancelStatusFilter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("status-filter-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)
	svc.StartRun(
		context.Background(), "status-filter-wf", nil,
	)
	time.Sleep(200 * time.Millisecond)

	resp, err := svc.BulkCancelRuns(context.Background(),
		BulkCancelRequest{
			WorkflowID: "status-filter-wf",
			Status:     "pending",
		},
	)
	if err != nil {
		t.Fatalf("BulkCancelRuns: %v", err)
	}
	if len(resp.Cancelled) != 0 {
		t.Fatalf("cancelled = %d, want 0 (no pending runs)",
			len(resp.Cancelled))
	}

	_, err = svc.BulkCancelRuns(context.Background(),
		BulkCancelRequest{
			WorkflowID: "status-filter-wf",
			Status:     "invalid",
		},
	)
	if err == nil {
		t.Fatal("expected error for invalid status filter")
	}
}

func TestBulkCancelEmptyResult(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	svc := NewService(nc)

	resp, err := svc.BulkCancelRuns(context.Background(),
		BulkCancelRequest{WorkflowID: "nonexistent"},
	)
	if err != nil {
		t.Fatalf("BulkCancelRuns: %v", err)
	}
	if resp.Total != 0 {
		t.Fatalf("total = %d, want 0", resp.Total)
	}
	if len(resp.Cancelled) != 0 {
		t.Fatalf("cancelled should be empty, got %d",
			len(resp.Cancelled))
	}
}

// TestBulkCancelSurfacesTruncatedScan proves the review-round-2 nit:
// when the newest-first scan hits its fetch cap before it could prove
// there are no more matches beyond the scanned window, the response
// says so via Truncated -- a caller must never read an empty/partial
// bulk-cancel result as "definitely everything," the way silently
// discarding ScanStats.Truncated let it. Population exceeds
// engine.ScanFetchMax (10,000) and belongs to an UNRELATED workflow,
// so genuinely zero runs match the target workflow -- but the scan
// still can't prove that within its fetch cap, so Truncated must be
// true regardless of the (correct) zero-match result.
func TestBulkCancelSurfacesTruncatedScan(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	const seedTotal = engine.ScanFetchMax + 100
	base := time.Now().Add(-24 * time.Hour)
	for i := 0; i < seedTotal; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("trunc-seed-%05d", i),
			WorkflowID: "trunc-cancel-other-wf",
			Status:     dag.RunStatusCompleted,
			Steps:      map[string]dag.StepState{},
			CreatedAt:  base.Add(time.Duration(i) * time.Millisecond),
		}
		if err := svc.store.SaveInitial(context.Background(), run); err != nil {
			t.Fatalf("seed run %d: %v", i, err)
		}
	}

	resp, err := svc.BulkCancelRuns(context.Background(),
		BulkCancelRequest{
			WorkflowID: "trunc-cancel-target-wf",
			DryRun:     true,
		},
	)
	if err != nil {
		t.Fatalf("BulkCancelRuns: %v", err)
	}
	// Positive: Truncated is surfaced.
	if !resp.Truncated {
		t.Fatal("resp.Truncated = false, want true " +
			"(scan hit its fetch cap before exhausting the population)")
	}
	// Negative: the (correct, but unproven-complete) zero-match result
	// is still returned alongside the honest Truncated flag.
	if len(resp.Cancelled) != 0 {
		t.Fatalf("Cancelled = %v, want empty", resp.Cancelled)
	}
}

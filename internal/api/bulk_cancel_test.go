// api/bulk_cancel_test.go
// Tests for BulkCancelRuns: verifies filtering by workflow, status,
// time range, dry-run, and the 1000-run cap.
// Uses real embedded NATS server. Runs are started through the real
// service and the tests wait until a scan lists them (startRunsListed)
// rather than sleeping a fixed time: a run's snapshot lands before its
// index entry, so a scan right after StartRun can miss it.
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

	registerEchoWorkflow(t, svc, "bulk-wf-a")
	registerEchoWorkflow(t, svc, "bulk-wf-b")
	startRunsListed(t, svc, "bulk-wf-a", 3)
	startRunsListed(t, svc, "bulk-wf-b", 1)

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
	registerEchoWorkflow(t, svc, "dry-run-wf")
	startRunsListed(t, svc, "dry-run-wf", 2)

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
	registerEchoWorkflow(t, svc, "status-filter-wf")
	startRunsListed(t, svc, "status-filter-wf", 1)

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

// registerEchoWorkflow registers a one-step workflow named name.
func registerEchoWorkflow(t *testing.T, svc *Service, name string) {
	t.Helper()
	wb := dag.NewWorkflow(name)
	wb.Task("s", "echo")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build(%s): %v", name, err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow(%s): %v", name, err)
	}
}

// startRunsListed starts count runs of workflowID and waits, bounded by
// 10s, until a workflow-filtered ScanRuns lists all of them.
func startRunsListed(t *testing.T, svc *Service, workflowID string, count int) []string {
	t.Helper()
	runIDs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		runID, err := svc.StartRun(context.Background(), workflowID, nil)
		if err != nil {
			t.Fatalf("StartRun(%s): %v", workflowID, err)
		}
		runIDs = append(runIDs, runID)
	}
	waitRunsListed(t, svc, workflowID, runIDs)
	return runIDs
}

// waitRunsListed polls ScanRuns until every id in runIDs is listed.
func waitRunsListed(t *testing.T, svc *Service, workflowID string, runIDs []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := svc.ScanRuns(
			context.Background(), RunsFilter{Workflow: workflowID}, 0,
		)
		if err == nil && allListed(runs, runIDs) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("runs %v of %s not listed within 10s", runIDs, workflowID)
}

func allListed(runs []dag.WorkflowRun, runIDs []string) bool {
	listed := make(map[string]bool, len(runs))
	for _, run := range runs {
		listed[run.RunID] = true
	}
	for _, runID := range runIDs {
		if !listed[runID] {
			return false
		}
	}
	return true
}

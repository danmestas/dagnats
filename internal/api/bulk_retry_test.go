// internal/api/bulk_retry_test.go
// Tests for BulkRetryRuns: verifies rerun and replay modes,
// filtering by workflow and time range.
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

func TestBulkRetryRerunMode(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("retry-rerun-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	runID1, _ := svc.StartRun(
		context.Background(), "retry-rerun-wf",
		[]byte(`{"item":"a"}`),
	)
	runID2, _ := svc.StartRun(
		context.Background(), "retry-rerun-wf",
		[]byte(`{"item":"b"}`),
	)

	// Wait for orchestrator to create snapshots.
	deadline := time.After(5 * time.Second)
	var run1, run2 dag.WorkflowRun
	for {
		var err1, err2 error
		run1, err1 = svc.GetRun(context.Background(), runID1)
		run2, err2 = svc.GetRun(context.Background(), runID2)
		if err1 == nil && err2 == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("snapshots did not appear within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Stop the orchestrator before forcing terminal state. Without this
	// fence, the live orchestrator can process queued events between
	// our Save and BulkRetryRuns, overwriting Failed back to Running
	// and leaving the test asserting against the wrong snapshot.
	orch.Stop()

	run1.Status = dag.RunStatusFailed
	if err := svc.store.Save(context.Background(), run1); err != nil {
		t.Fatalf("force state run1: %v", err)
	}
	run2.Status = dag.RunStatusFailed
	if err := svc.store.Save(context.Background(), run2); err != nil {
		t.Fatalf("force state run2: %v", err)
	}

	resp, err := svc.BulkRetryRuns(context.Background(),
		BulkRetryRequest{
			WorkflowID: "retry-rerun-wf",
			Mode:       "rerun",
		},
	)
	if err != nil {
		t.Fatalf("BulkRetryRuns: %v", err)
	}

	if len(resp.Retried) != 2 {
		t.Fatalf("retried = %d, want 2",
			len(resp.Retried))
	}
	if resp.Total != 2 {
		t.Fatalf("total = %d, want 2", resp.Total)
	}

	for _, item := range resp.Retried {
		if item.NewRunID == "" {
			t.Fatal("rerun must produce new run ID")
		}
		if item.NewRunID == item.OriginalRunID {
			t.Fatal("new ID must differ from original")
		}
	}
}

func TestBulkRetryDryRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("retry-dry-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	runID, _ := svc.StartRun(
		context.Background(), "retry-dry-wf", nil,
	)

	// Wait for orchestrator to create snapshot.
	deadline := time.After(5 * time.Second)
	var run dag.WorkflowRun
	for {
		var err error
		run, err = svc.GetRun(context.Background(), runID)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("snapshot did not appear within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Stop orchestrator before forcing terminal state — see
	// TestBulkRetryRerunMode for the race rationale.
	orch.Stop()

	run.Status = dag.RunStatusFailed
	if err := svc.store.Save(context.Background(), run); err != nil {
		t.Fatalf("force state: %v", err)
	}

	resp, err := svc.BulkRetryRuns(context.Background(),
		BulkRetryRequest{
			WorkflowID: "retry-dry-wf",
			Mode:       "rerun",
			DryRun:     true,
		},
	)
	if err != nil {
		t.Fatalf("BulkRetryRuns: %v", err)
	}

	if !resp.DryRun {
		t.Fatal("expected DryRun=true")
	}
	if len(resp.Retried) != 1 {
		t.Fatalf("retried = %d, want 1",
			len(resp.Retried))
	}
	if resp.Retried[0].NewRunID != "" {
		t.Fatal("dry run should not have new run ID")
	}
}

func TestBulkRetryRequiresMode(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	_, err := svc.BulkRetryRuns(context.Background(),
		BulkRetryRequest{
			WorkflowID: "wf",
			Mode:       "invalid",
		},
	)
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}

	_, err = svc.BulkRetryRuns(context.Background(),
		BulkRetryRequest{
			WorkflowID: "nonexistent",
			Mode:       "rerun",
		},
	)
	if err != nil &&
		err.Error() == `mode must be "rerun" or "replay"` {
		t.Fatal("rerun mode should pass validation")
	}
}

func TestBulkRetrySkipsNonFailed(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("retry-skip-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	runID, _ := svc.StartRun(
		context.Background(), "retry-skip-wf", nil,
	)

	// Wait for orchestrator to create snapshot
	deadline := time.After(5 * time.Second)
	for {
		_, err := svc.GetRun(context.Background(), runID)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("snapshot did not appear within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	resp, err := svc.BulkRetryRuns(context.Background(),
		BulkRetryRequest{
			WorkflowID: "retry-skip-wf",
			Mode:       "rerun",
		},
	)
	if err != nil {
		t.Fatalf("BulkRetryRuns: %v", err)
	}

	if resp.Total != 0 {
		t.Fatalf("total = %d, want 0", resp.Total)
	}
	if len(resp.Retried) != 0 {
		t.Fatalf("retried = %d, want 0",
			len(resp.Retried))
	}
}

// TestBulkRetryFindsNewFailedRunAmongManyOldRuns reproduces the same
// #659 bias bulk cancel had, for bulk retry's ListAll+filterFailedRuns
// path (review-round nit: same shape, same fix): a Failed run for the
// target workflow, seeded AFTER 1200+ Failed runs belonging to an
// unrelated workflow, must still be found -- the unrelated population
// must not crowd it out of an order-agnostic bounded sample.
func TestBulkRetryFindsNewFailedRunAmongManyOldRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	svc := NewService(nc)
	ctx := context.Background()
	base := time.Now().Add(-24 * time.Hour)
	const total = 1200
	for i := 0; i < total; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("retry-seed-%05d", i),
			WorkflowID: "retry-scan-order-other-wf",
			Status:     dag.RunStatusFailed,
			Steps:      map[string]dag.StepState{},
			CreatedAt:  base.Add(time.Duration(i) * time.Millisecond),
		}
		if err := svc.store.Save(ctx, run); err != nil {
			t.Fatalf("seed run %d: %v", i, err)
		}
	}

	const wf = "retry-scan-order-wf"
	newRun := dag.WorkflowRun{
		RunID: "zzzz-new-failed", WorkflowID: wf,
		Status: dag.RunStatusFailed, Steps: map[string]dag.StepState{},
		Input: []byte(`{"x":1}`), CreatedAt: time.Now().UTC(),
	}
	if err := svc.store.Save(ctx, newRun); err != nil {
		t.Fatalf("save new run: %v", err)
	}

	resp, err := svc.BulkRetryRuns(ctx, BulkRetryRequest{
		WorkflowID: wf, Mode: "rerun", DryRun: true,
	})
	if err != nil {
		t.Fatalf("BulkRetryRuns: %v", err)
	}
	// Positive: exactly the new run was found.
	if len(resp.Retried) != 1 || resp.Retried[0].OriginalRunID != "zzzz-new-failed" {
		t.Fatalf("Retried = %+v, want exactly [zzzz-new-failed]", resp.Retried)
	}
	// Negative: none of the unrelated-workflow seeded runs leaked in.
	if resp.Total != 1 {
		t.Fatalf("Total = %d, want 1", resp.Total)
	}
}

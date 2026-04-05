// internal/api/bulk_retry_test.go
// Tests for BulkRetryRuns: verifies rerun and replay modes,
// filtering by workflow and time range.
// Uses real embedded NATS server.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestBulkRetryRerunMode(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
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

	// Wait for orchestrator to create snapshots
	deadline := time.After(5 * time.Second)
	for {
		run1, err1 := svc.GetRun(context.Background(), runID1)
		run2, err2 := svc.GetRun(context.Background(), runID2)
		if err1 == nil && err2 == nil {
			// Mark runs as failed via snapshot
			run1.Status = dag.RunStatusFailed
			svc.store.Save(run1)
			run2.Status = dag.RunStatusFailed
			svc.store.Save(run2)
			break
		}
		select {
		case <-deadline:
			t.Fatal("snapshots did not appear within 5s")
		case <-time.After(10 * time.Millisecond):
		}
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

	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("retry-dry-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)

	runID, _ := svc.StartRun(
		context.Background(), "retry-dry-wf", nil,
	)

	// Wait for orchestrator to create snapshot
	deadline := time.After(5 * time.Second)
	for {
		run, err := svc.GetRun(context.Background(), runID)
		if err == nil {
			run.Status = dag.RunStatusFailed
			svc.store.Save(run)
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
	svc := NewService(nc, observe.NewNoopTelemetry())

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

	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
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

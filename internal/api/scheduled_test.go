// api/scheduled_test.go
// Tests for scheduled run operations: schedule, get, cancel, list.
// Methodology: real embedded NATS. Verify KV state after each operation.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestScheduleRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("sched-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	err = svc.RegisterWorkflow(context.Background(), wfDef)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	runAt := time.Now().Add(1 * time.Hour)
	runID, err := svc.ScheduleRun(
		context.Background(), "sched-test", []byte(`"input"`), runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	// Positive: runID is non-empty.
	if runID == "" {
		t.Fatal("runID should not be empty")
	}

	// Positive: can retrieve the scheduled run.
	sr, err := svc.GetScheduledRun(runID)
	if err != nil {
		t.Fatalf("GetScheduledRun: %v", err)
	}
	if sr.RunID != runID {
		t.Fatalf("RunID = %q, want %q", sr.RunID, runID)
	}
	if sr.Status != "scheduled" {
		t.Fatalf("Status = %q, want scheduled", sr.Status)
	}

	// Negative: GetRun should NOT find it (it hasn't started).
	_, err = svc.GetRun(context.Background(), runID)
	if err == nil {
		t.Fatal("GetRun should fail for scheduled (not-yet-started) run")
	}
}

func TestCancelScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("cancel-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	runAt := time.Now().Add(1 * time.Hour)
	runID, err := svc.ScheduleRun(
		context.Background(), "cancel-test", nil, runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	// Positive: cancel succeeds.
	err = svc.CancelScheduledRun(runID)
	if err != nil {
		t.Fatalf("CancelScheduledRun: %v", err)
	}

	// Positive: status is now cancelled.
	sr, err := svc.GetScheduledRun(runID)
	if err != nil {
		t.Fatalf("GetScheduledRun: %v", err)
	}
	if sr.Status != "cancelled" {
		t.Fatalf("Status = %q, want cancelled", sr.Status)
	}

	// Negative: cancelling again should fail.
	err = svc.CancelScheduledRun(runID)
	if err == nil {
		t.Fatal("double cancel should fail")
	}
}

func TestListScheduledRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("list-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Schedule two runs at different times.
	runAt1 := time.Now().Add(2 * time.Hour)
	runAt2 := time.Now().Add(1 * time.Hour)
	_, err = svc.ScheduleRun(
		context.Background(), "list-test", nil, runAt1,
	)
	if err != nil {
		t.Fatalf("ScheduleRun 1: %v", err)
	}
	_, err = svc.ScheduleRun(
		context.Background(), "list-test", nil, runAt2,
	)
	if err != nil {
		t.Fatalf("ScheduleRun 2: %v", err)
	}

	runs, err := svc.ListScheduledRuns()
	if err != nil {
		t.Fatalf("ListScheduledRuns: %v", err)
	}

	// Positive: both runs returned.
	if len(runs) != 2 {
		t.Fatalf("len = %d, want 2", len(runs))
	}

	// Negative: empty list returns nil, not error.
	// Cancel both and list again.
	for _, r := range runs {
		svc.CancelScheduledRun(r.RunID)
	}
	active, err := svc.ListScheduledRuns()
	if err != nil {
		t.Fatalf("ListScheduledRuns after cancel: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("len = %d, want 0 after cancel", len(active))
	}
}

func TestScheduleRunValidation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("valid-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Negative: run_at in the past should fail.
	_, err = svc.ScheduleRun(
		context.Background(), "valid-test", nil,
		time.Now().Add(-1*time.Hour),
	)
	if err == nil {
		t.Fatal("past run_at should fail")
	}

	// Negative: run_at beyond 365 days should fail.
	_, err = svc.ScheduleRun(
		context.Background(), "valid-test", nil,
		time.Now().Add(366*24*time.Hour),
	)
	if err == nil {
		t.Fatal("run_at beyond 365 days should fail")
	}

	// Negative: non-existent workflow should fail.
	_, err = svc.ScheduleRun(
		context.Background(), "no-such-wf", nil,
		time.Now().Add(1*time.Hour),
	)
	if err == nil {
		t.Fatal("non-existent workflow should fail")
	}
}

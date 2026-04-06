// api/timer_test.go
// Tests that the scheduled run timer consumer fires workflows
// when the timer expires.
// Methodology: real embedded NATS. Use short delay, verify run starts.
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

func TestScheduledRunTimerFires(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	// Start orchestrator so workflow.started events get processed.
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	t.Cleanup(func() { orch.Stop() })

	wb := dag.NewWorkflow("timer-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Schedule 2 seconds from now.
	runAt := time.Now().Add(2 * time.Second)
	runID, err := svc.ScheduleRun(
		context.Background(), "timer-test", nil, runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	// Start the timer consumer.
	timer := NewTimerConsumer(svc)
	if err := timer.Start(); err != nil {
		t.Fatalf("timer.Start: %v", err)
	}
	t.Cleanup(func() { timer.Stop() })

	// Poll with bounded deadline.
	deadline := time.Now().Add(10 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		run, err = svc.GetRun(context.Background(), runID)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GetRun not found within deadline: %v", err)
	}

	// Positive: the run exists in workflow_runs.
	if run.RunID != runID {
		t.Fatalf("RunID = %q, want %q", run.RunID, runID)
	}

	// Negative: scheduled_runs entry should be deleted.
	_, err = svc.GetScheduledRun(runID)
	if err == nil {
		t.Fatal(
			"GetScheduledRun should fail after timer fired",
		)
	}
}

func TestScheduledRunTimerCancelled(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("cancel-timer-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Schedule 2 seconds from now, then cancel immediately.
	runAt := time.Now().Add(2 * time.Second)
	runID, err := svc.ScheduleRun(
		context.Background(), "cancel-timer-test", nil, runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}
	err = svc.CancelScheduledRun(runID)
	if err != nil {
		t.Fatalf("CancelScheduledRun: %v", err)
	}

	timer := NewTimerConsumer(svc)
	if err := timer.Start(); err != nil {
		t.Fatalf("timer.Start: %v", err)
	}
	t.Cleanup(func() { timer.Stop() })

	// Wait past the timer fire time.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}

	// Negative: GetRun should fail — cancelled run should not start.
	_, err = svc.GetRun(context.Background(), runID)
	if err == nil {
		t.Fatal("cancelled scheduled run should not start")
	}

	// Positive: KV entry should be cleaned up by timer.
	sr, serr := svc.GetScheduledRun(runID)
	if serr == nil && sr.Status == "scheduled" {
		t.Fatal("KV entry should not still be 'scheduled'")
	}
}

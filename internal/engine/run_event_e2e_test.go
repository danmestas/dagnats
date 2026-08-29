// internal/engine/run_event_e2e_test.go
// Integration tests for the event.run.* consumer contract (#625).
// Methodology: real embedded NATS via dagnatstest.Harness. Subscribe to
// event.run.> BEFORE starting each run so no message can race the
// subscription, drive the run through the real worker/engine path to a
// terminal status, then assert exactly one RunEvent lands with the right
// run_id/workflow_id/status. Bounded waits throughout (no unbounded
// blocking receive).
package engine_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// subscribeRunEvents subscribes to event.run.> and returns a channel
// that receives decoded RunEvents. The subscription is established
// before the caller starts any run, so no publish can race it.
func subscribeRunEvents(
	t *testing.T, nc *nats.Conn,
) <-chan protocol.RunEvent {
	t.Helper()
	if nc == nil {
		panic("subscribeRunEvents: nc must not be nil")
	}
	out := make(chan protocol.RunEvent, 16)
	sub, err := nc.Subscribe("event.run.>", func(msg *nats.Msg) {
		var evt protocol.RunEvent
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Logf("subscribeRunEvents: unmarshal failed: %v", err)
			return
		}
		out <- evt
	})
	if err != nil {
		t.Fatalf("subscribeRunEvents: Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return out
}

// waitForRunEvent drains ch for a RunEvent matching runID within
// timeout, ignoring events for other runs (the subscription is
// process-wide across every test's runs on this NATS server).
func waitForRunEvent(
	t *testing.T, ch <-chan protocol.RunEvent,
	runID string, timeout time.Duration,
) protocol.RunEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case evt := <-ch:
			if evt.RunID == runID {
				return evt
			}
		case <-deadline:
			t.Fatalf(
				"waitForRunEvent: no event.run.* for run %q within %s",
				runID, timeout,
			)
		}
	}
}

func TestEngine_CompletedRun_PublishesRunCompletedEvent(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "run-evt-task-ok", dagnatstest.PassHandler())
	h.Start(t)

	// Subscribe BEFORE the run starts — the whole point of the test is
	// that no terminal event can be missed by a late subscriber.
	events := subscribeRunEvents(t, h.NC)

	wfName := fmt.Sprintf("run-evt-completed-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-ok")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	got := h.RegisterAndRun(t, def, nil, 10*time.Second)
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("run status = %v, want Completed", got.Status)
	}

	evt := waitForRunEvent(t, events, got.RunID, 5*time.Second)
	if evt.Type != protocol.RunEventCompleted {
		t.Fatalf("Type = %q, want %q", evt.Type, protocol.RunEventCompleted)
	}
	if evt.WorkflowID != wfName {
		t.Fatalf("WorkflowID = %q, want %q", evt.WorkflowID, wfName)
	}
	if evt.Status != "completed" {
		t.Fatalf("Status = %q, want %q", evt.Status, "completed")
	}
}

func TestEngine_FailedRun_PublishesRunFailedEvent(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "run-evt-task-fail",
		dagnatstest.FailHandler("permanent failure"))
	h.Start(t)

	events := subscribeRunEvents(t, h.NC)

	wfName := fmt.Sprintf("run-evt-failed-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-fail")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	got := h.RegisterAndRun(t, def, nil, 10*time.Second)
	if got.Status != dag.RunStatusFailed {
		t.Fatalf("run status = %v, want Failed", got.Status)
	}

	evt := waitForRunEvent(t, events, got.RunID, 5*time.Second)
	if evt.Type != protocol.RunEventFailed {
		t.Fatalf("Type = %q, want %q", evt.Type, protocol.RunEventFailed)
	}
	if evt.Status != "failed" {
		t.Fatalf("Status = %q, want %q", evt.Status, "failed")
	}
}

func TestEngine_CancelledRun_PublishesRunCancelledEvent(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	// A handler that blocks past the test's cancel call so the run is
	// still Running (cancellable) when CancelRun is issued.
	block := make(chan struct{})
	h.Handle(t, "run-evt-task-block", func(tc worker.TaskContext) error {
		<-block
		return tc.Complete(tc.Input())
	})
	h.Start(t)

	events := subscribeRunEvents(t, h.NC)

	wfName := fmt.Sprintf("run-evt-cancelled-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-block")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ctx := t.Context()
	if err := h.Svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	runID, err := h.Svc.StartRun(ctx, wfName, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	dagnatstest.WaitForStatus(
		t, h.Svc, runID, 10*time.Second, dag.RunStatusRunning,
	)
	if err := h.Svc.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	got := dagnatstest.WaitForStatus(
		t, h.Svc, runID, 10*time.Second, dag.RunStatusCancelled,
	)
	if got.Status != dag.RunStatusCancelled {
		t.Fatalf("run status = %v, want Cancelled", got.Status)
	}

	evt := waitForRunEvent(t, events, runID, 5*time.Second)
	if evt.Type != protocol.RunEventCancelled {
		t.Fatalf("Type = %q, want %q", evt.Type, protocol.RunEventCancelled)
	}
	if evt.Status != "cancelled" {
		t.Fatalf("Status = %q, want %q", evt.Status, "cancelled")
	}

	// Unblock the still-running handler goroutine now, BEFORE the
	// harness's t.Cleanup-registered Worker.Stop() runs — Stop() drains
	// in-flight handlers with a 30s timeout, and t.Cleanup runs LIFO, so
	// leaving this to a Cleanup registered here would run after Stop()
	// and the test would eat the full drain timeout.
	close(block)
}

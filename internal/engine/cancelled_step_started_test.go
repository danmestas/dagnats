// engine/cancelled_step_started_test.go
// Regression test for #737's recording half: a step.started for a step of
// a cancelled run must not move that step out of Cancelled. Dispatch
// withholds such tasks (bridge poll, native worker), but a task already
// in flight when the run was cancelled can still announce its start, and
// handleStepStarted only guarded Completed and Failed steps, so the
// step flipped back to Running under the cancelled run.
//
// Methodology: one grouped step started through the real orchestrator,
// cancelled with a workflow.cancelled event, then sent a late
// step.started. The step must read Cancelled throughout a bounded
// window. Pre-fix it read Running within milliseconds.
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestStepStartedAfterCancelLeavesStepCancelled(t *testing.T) {
	js, store := startCancelledRun(t, "cancel-started-1")
	publishLateStepEvent(t, js, "cancel-started-1", protocol.EventStepStarted)
	requireStaysCancelled(t, store, "cancel-started-1")
}

// TestStepQueuedAfterCancelLeavesStepCancelled covers the same hole in
// handleStepQueued. The engine's own step.queued, emitted at dispatch,
// can land after a cancel that raced the first enqueue. The test sends
// one deliberately after the cancel so the guard, not the race, decides.
func TestStepQueuedAfterCancelLeavesStepCancelled(t *testing.T) {
	js, store := startCancelledRun(t, "cancel-queued-1")
	publishLateStepEvent(t, js, "cancel-queued-1", protocol.EventStepQueued)
	requireStaysCancelled(t, store, "cancel-queued-1")
}

// startCancelledRun starts the one grouped step, waits until it is
// queued (so the engine's own step.queued is already processed and cannot
// race the cancel), cancels the run and waits until it reads cancelled.
func startCancelledRun(
	t *testing.T, runID string,
) (nats.JetStreamContext, *SnapshotStore) {
	t.Helper()
	if runID == "" {
		t.Fatal("startCancelledRun: runID must not be empty")
	}
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	orch := startGroupedRetryWorkflow(t, nc, js, runID, nil)
	t.Cleanup(orch.Stop)
	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, runID, "train", dag.StepStatusQueued, 5*time.Second)
	cancel := protocol.NewWorkflowEvent(protocol.EventWorkflowCancelled, runID, nil)
	data, err := cancel.Marshal()
	if err != nil {
		t.Fatalf("marshal cancel: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: cancel.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {cancel.NATSMsgID()}},
	})
	waitForRunStatus(t, store, runID, dag.RunStatusCancelled, 5*time.Second)
	waitForStepStatus(t, store, runID, "train", dag.StepStatusCancelled, 5*time.Second)
	return js, store
}

// publishLateStepEvent sends one step event for the train step with a
// message ID of its own, so it is not deduplicated against the engine's.
func publishLateStepEvent(
	t *testing.T, js nats.JetStreamContext, runID string, typ protocol.EventType,
) {
	t.Helper()
	if js == nil {
		t.Fatal("publishLateStepEvent: js must not be nil")
	}
	evt := protocol.NewStepEvent(typ, runID, "train", nil)
	evt.AttemptNumber = 1
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal %s: %v", typ, err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: evt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {runID + ".late." + string(typ)}},
	})
}

// requireStaysCancelled asserts the run and its step read cancelled
// throughout a bounded window after the late event.
func requireStaysCancelled(t *testing.T, store *SnapshotStore, runID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(t.Context(), runID)
		if err != nil {
			t.Fatalf("load run: %v", err)
		}
		if got := run.Steps["train"].Status; got != dag.StepStatusCancelled {
			t.Fatalf("step reads %v after a late event, want Cancelled", got)
		}
		if run.Status != dag.RunStatusCancelled {
			t.Fatalf("run reads %v, want Cancelled", run.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

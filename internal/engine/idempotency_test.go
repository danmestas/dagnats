// idempotency_test.go
// Tests for handler idempotency under event-stream redelivery.
// Methodology: seed workflow_runs KV with a terminal-state run,
// publish a redelivered workflow.started event for the same
// run_id, and verify that the engine does not overwrite the
// existing terminal state. See #196.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestHandleWorkflowStarted_IdempotentForCompletedRun(t *testing.T) {
	// The bug from #196: when WORKFLOW_HISTORY replays a
	// historical workflow.started event after a dagnats
	// restart, handleWorkflowStarted overwrites the existing
	// run's Completed KV state with a fresh Pending run and
	// dispatches its first step again, causing duplicate
	// workflow.completed events and worker storms.
	//
	// After fix: a redelivered workflow.started for a run
	// that already exists in non-Pending state is a no-op;
	// the existing terminal state is preserved.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "redeliver-completed", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)

	// Seed the KV with a Completed run for run_id
	// "redelivered-run". Mirrors the post-restart state of a
	// run that genuinely completed before the redelivery
	// window opens.
	completed := dag.WorkflowRun{
		RunID:      "redelivered-run",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusCompleted,
		CreatedAt:  time.Now().UTC().Add(-1 * time.Hour),
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
		},
	}
	ctx := context.Background()
	if err := orch.store.Save(ctx, completed); err != nil {
		t.Fatalf("seed completed run: %v", err)
	}

	orch.Start()
	defer orch.Stop()

	// Publish the redelivered workflow.started event.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"redelivered-run", defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Give the engine a short window to process the event.
	// Pre-fix the bug would manifest within ~100ms — the
	// engine would overwrite KV and dispatch step `a`. Post
	// fix the engine should log-and-skip and KV remains
	// Completed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	after, err := orch.store.Load(ctx, "redelivered-run")
	if err != nil {
		t.Fatalf("load after replay: %v", err)
	}

	if after.Status != dag.RunStatusCompleted {
		t.Errorf(
			"after redelivered workflow.started: "+
				"Status = %v, want Completed (run "+
				"state should not have been overwritten)",
			after.Status,
		)
	}
	if after.Steps["a"].Status != dag.StepStatusCompleted {
		t.Errorf(
			"after redelivered workflow.started: "+
				"Steps[a].Status = %v, want Completed",
			after.Steps["a"].Status,
		)
	}
}

func TestHandleWorkflowStarted_IdempotentForRunningRun(t *testing.T) {
	// Same shape as the Completed case, but with a Running
	// run — this guards against a redelivered workflow.started
	// also overwriting state for a run that's currently
	// in flight.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "redeliver-running", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)

	running := dag.WorkflowRun{
		RunID:      "running-run",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusRunning,
		CreatedAt:  time.Now().UTC().Add(-1 * time.Minute),
		Steps: map[string]dag.StepState{
			"a": {
				Status:   dag.StepStatusRunning,
				Attempts: 1,
			},
		},
	}
	ctx := context.Background()
	if err := orch.store.Save(ctx, running); err != nil {
		t.Fatalf("seed running run: %v", err)
	}

	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"running-run", defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	after, err := orch.store.Load(ctx, "running-run")
	if err != nil {
		t.Fatalf("load after replay: %v", err)
	}

	if after.Steps["a"].Status != dag.StepStatusRunning {
		t.Errorf(
			"after redelivered workflow.started: "+
				"Steps[a].Status = %v, want Running "+
				"(in-flight step state should not have "+
				"been reset to Pending)",
			after.Steps["a"].Status,
		)
	}
	if after.Steps["a"].Attempts != 1 {
		t.Errorf(
			"after redelivered workflow.started: "+
				"Steps[a].Attempts = %d, want 1 "+
				"(attempts counter should not have "+
				"been reset)",
			after.Steps["a"].Attempts,
		)
	}
}

func TestHandleStepCompleted_SkipsTerminalRun(t *testing.T) {
	// Defense-in-depth half of #196: even with the
	// handleWorkflowStarted guard in place, replayed
	// step.completed events for already-terminal runs would
	// cause Advance + completeWorkflow to re-fire, double-
	// decrementing runsActive and republishing
	// workflow.completed. The terminal-run guard in
	// handleStepCompleted prevents this.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "redeliver-step-completed", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	terminal := dag.WorkflowRun{
		RunID:      "completed-run",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusCompleted,
		CreatedAt:  time.Now().UTC().Add(-1 * time.Hour),
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
		},
	}
	ctx := context.Background()
	if err := orch.store.Save(ctx, terminal); err != nil {
		t.Fatalf("seed terminal run: %v", err)
	}

	orch.Start()
	defer orch.Stop()

	// Capture workflow.completed publishes via a core NATS
	// subscription so we can verify zero new ones fire.
	completedSub, err := nc.SubscribeSync(
		"history.completed-run.workflow.completed",
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer completedSub.Unsubscribe()

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted,
		"completed-run", "a", []byte(`"done"`),
	)
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait briefly. Pre-fix the engine would observe the event,
	// re-fire completeWorkflow, and publish workflow.completed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := completedSub.NextMsg(
			50 * time.Millisecond,
		); err == nil {
			t.Fatal(
				"received unexpected workflow.completed " +
					"after redelivered step.completed " +
					"for terminal run",
			)
		}
	}
}

func TestRestartedOrchestratorDoesNotReplayHistory(t *testing.T) {
	// Verifies the durable consumer half of #196: after a
	// stop+start cycle, a fresh Orchestrator must NOT
	// re-deliver and re-process events that were already
	// acked by the prior instance. Pre-fix (ephemeral
	// consumer + DeliverAllPolicy) would replay every
	// historical event on each restart.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "restart-replay", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	// First instance: process a workflow.started, dispatch a
	// step, complete it.
	orch1 := NewOrchestrator(nc)
	orch1.Start()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"durable-test-run", defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	waitForStepStatus(t, orch1.store, "durable-test-run", "a",
		dag.StepStatusQueued, 5*time.Second)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted,
		"durable-test-run", "a", []byte(`"done"`),
	)
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	waitForRunStatus(t, orch1.store, "durable-test-run",
		dag.RunStatusCompleted, 5*time.Second)

	orch1.Stop()

	// Second instance: subscribe to workflow.completed BEFORE
	// starting so we can see any spurious republish.
	republishSub, err := nc.SubscribeSync(
		"history.durable-test-run.workflow.completed",
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer republishSub.Unsubscribe()

	orch2 := NewOrchestrator(nc)
	orch2.Start()
	defer orch2.Stop()

	// Wait long enough that any replay-driven re-processing
	// would have surfaced. Pre-fix this saw both replayed
	// workflow.started and step.completed re-execute the run
	// and publish a fresh workflow.completed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := republishSub.NextMsg(
			50 * time.Millisecond,
		)
		if err == nil && msg != nil {
			t.Fatal(
				"second orchestrator re-published " +
					"workflow.completed — durable consumer " +
					"replayed history",
			)
		}
	}

	// Sanity: KV state should still reflect the original
	// completion, untouched.
	after, err := orch2.store.Load(
		context.Background(), "durable-test-run",
	)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if after.Status != dag.RunStatusCompleted {
		t.Errorf(
			"Status = %v after restart, want Completed",
			after.Status,
		)
	}
}

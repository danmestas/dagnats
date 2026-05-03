// advance_exec_test.go
// Methodology: Integration tests for the side-effect executor and the
// Advance → executeSideEffects wiring. Uses real embedded NATS server.
// Tests verify that pure Advance output drives real I/O through the
// orchestrator shell. Red-green TDD: tests written first, then wired.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestExecuteSideEffects_EnqueueTask(t *testing.T) {
	// Verify that EnqueueTask effects result in task messages
	// on the TASK_QUEUES stream.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "exec-test",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defData := mustMarshal(t, wfDef)
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)

	run := dag.NewWorkflowRun(wfDef, "run-exec-1")
	run.Status = dag.RunStatusRunning
	if err := orch.store.Save(
		context.Background(), run,
	); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	effects := []SideEffect{
		EnqueueTask{
			Step:  wfDef.Steps[0],
			Input: []byte(`"hello"`),
		},
	}

	err = orch.executeSideEffects(
		context.Background(), wfDef, run, effects,
	)
	if err != nil {
		t.Fatalf("executeSideEffects failed: %v", err)
	}

	// Positive: task message appears on TASK_QUEUES.
	sub, err := js.PullSubscribe(
		"task.task-a.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task message, got %d", len(msgs))
	}

	// Negative: no extra messages beyond the one we sent.
	extra, _ := sub.Fetch(
		1, nats.MaxWait(200*time.Millisecond),
	)
	if len(extra) > 0 {
		t.Fatal("expected no extra task messages")
	}
}

func TestExecuteSideEffects_CompleteWorkflow(t *testing.T) {
	// Verify that CompleteWorkflow effect publishes a
	// workflow.completed event to the history stream.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "complete-test",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "only",
				Task: "task-only",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defData := mustMarshal(t, wfDef)
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)

	run := dag.NewWorkflowRun(wfDef, "run-complete-1")
	run.Status = dag.RunStatusRunning
	// Mark the only step as completed so snapshot is valid.
	state := run.Steps["only"]
	state.Status = dag.StepStatusCompleted
	state.Output = []byte(`"done"`)
	run.Steps["only"] = state

	if err := orch.store.Save(
		context.Background(), run,
	); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// Bump runsActive so the decrement in completeWorkflow
	// does not go negative.
	orch.metrics.runsActive.Add(context.Background(), 1)

	effects := []SideEffect{CompleteWorkflow{}}
	err = orch.executeSideEffects(
		context.Background(), wfDef, run, effects,
	)
	if err != nil {
		t.Fatalf("executeSideEffects failed: %v", err)
	}

	// Positive: workflow.completed event on history stream.
	sub, err := js.PullSubscribe(
		"history.>", "",
		nats.BindStream("WORKFLOW_HISTORY"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		msgs, _ := sub.Fetch(
			10, nats.MaxWait(500*time.Millisecond),
		)
		for _, msg := range msgs {
			var evt protocol.Event
			if err := json.Unmarshal(
				msg.Data, &evt,
			); err != nil {
				continue
			}
			if evt.Type == protocol.EventWorkflowCompleted {
				found = true
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal(
			"expected workflow.completed event on history stream",
		)
	}

	// Negative: run snapshot status should be Completed.
	loaded, err := orch.store.Load(
		context.Background(), "run-complete-1",
	)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if loaded.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected run status Completed, got %s",
			loaded.Status,
		)
	}
}

func TestAdvanceIntegration_StepCompletedTriggersEnqueue(
	t *testing.T,
) {
	// Full integration: start orchestrator, register 2-step workflow,
	// start run, complete step "a" → verify "b" enqueued via Advance,
	// complete step "b" → verify workflow completes.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "advance-integ",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID:        "b",
				Task:      "task-b",
				DependsOn: []string{"a"},
				Type:      dag.StepTypeNormal,
			},
		},
	}
	defData := mustMarshal(t, wfDef)
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start the workflow run.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-adv-1", defData,
	)
	startData, _ := startEvt.Marshal()
	if _, err := js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish start event: %v", err)
	}

	// Consume task-a.
	subA, err := js.PullSubscribe(
		"task.task-a.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("subscribe task-a: %v", err)
	}
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch task-a: %v", err)
	}
	if len(msgsA) != 1 {
		t.Fatalf("expected 1 task-a msg, got %d", len(msgsA))
	}
	msgsA[0].Ack()

	// Complete step "a".
	compA := protocol.NewStepEvent(
		protocol.EventStepCompleted,
		"run-adv-1", "a", []byte(`"output-a"`),
	)
	compAData, _ := compA.Marshal()
	if _, err := js.Publish(
		compA.NATSSubject(), compAData,
		nats.MsgId(compA.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish step-a completed: %v", err)
	}

	// Positive: task-b should be enqueued.
	subB, err := js.PullSubscribe(
		"task.task-b.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("subscribe task-b: %v", err)
	}
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch task-b: %v", err)
	}
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b msg, got %d", len(msgsB))
	}
	msgsB[0].Ack()

	// Complete step "b".
	compB := protocol.NewStepEvent(
		protocol.EventStepCompleted,
		"run-adv-1", "b", []byte(`"output-b"`),
	)
	compBData, _ := compB.Marshal()
	if _, err := js.Publish(
		compB.NATSSubject(), compBData,
		nats.MsgId(compB.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish step-b completed: %v", err)
	}

	// Positive: workflow should complete.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		loaded, loadErr := orch.store.Load(
			context.Background(), "run-adv-1",
		)
		if loadErr == nil &&
			loaded.Status == dag.RunStatusCompleted {
			// Negative: run must not be Failed.
			if loaded.Status == dag.RunStatusFailed {
				t.Fatal("run should be Completed, not Failed")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("workflow did not complete within deadline")
}

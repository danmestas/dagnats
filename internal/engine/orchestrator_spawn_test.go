// engine/orchestrator_spawn_test.go
// Sub-workflow spawn tests for the orchestrator: spawning child workflows,
// child completion/failure notifying the parent, and nesting-depth limits.
// Uses real embedded NATS server.
// Methodology: publish spawn/child events to the history stream, let the
// orchestrator process them, then verify child runs and parent notification
// events. Each test gets its own embedded server.

package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrchestratorHandlesWorkflowSpawn(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	// Register a child workflow definition
	childDef := dag.WorkflowDef{
		Name:    "child-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "child-step", Task: "child-task",
				Type: dag.StepTypeNormal},
		},
	}
	childDefData := mustMarshal(t, childDef)
	if _, err := defKV.Put("child-wf", childDefData); err != nil {
		t.Fatalf("put child def: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Publish spawn event
	spawnPayload := mustMarshal(t, map[string]string{
		"child_run_id":   "child-run-1",
		"child_workflow": "child-wf",
		"parent_step_id": "parent-step-a",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-run-1", spawnPayload)
	data, err := spawnEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))

	// Wait for child run to appear in snapshot store
	store := NewSnapshotStore(jsNew)
	var childRun dag.WorkflowRun
	var loadErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		childRun, loadErr = store.Load(context.Background(), "child-run-1")
		if loadErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if loadErr != nil {
		t.Fatalf("child run should exist: %v", loadErr)
	}
	if childRun.ParentRunID != "parent-run-1" {
		t.Fatalf("ParentRunID = %q, want parent-run-1",
			childRun.ParentRunID)
	}
	if childRun.ParentStepID != "parent-step-a" {
		t.Fatalf("ParentStepID = %q, want parent-step-a",
			childRun.ParentStepID)
	}
}

func TestOrchestratorChildCompletionNotifiesParent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Register a single-step child workflow
	childDef := dag.WorkflowDef{
		Name: "notify-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "echo-task", Type: dag.StepTypeNormal},
		},
	}
	childDefData := mustMarshal(t, childDef)
	mustPut(t, defKV, "notify-child", childDefData)

	// Subscribe to parent's history for child.completed
	parentSub, err := js.SubscribeSync("history.parent-run-2",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe parent history: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Spawn a child workflow
	spawnPayload := mustMarshal(t, map[string]string{
		"child_run_id":   "child-run-2",
		"child_workflow": "notify-child",
		"parent_step_id": "parent-step-b",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-run-2", spawnPayload)
	data, err := spawnEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))

	// Wait for child step to be queued, then simulate completion.
	waitForStepStatus(t, orch.store, "child-run-2", "s1",
		dag.StepStatusQueued, 5*time.Second)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted,
		"child-run-2", "s1", []byte(`"child-result"`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Look for workflow.child.completed on parent's history
	deadline := time.Now().Add(5 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		m, err := parentSub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		m.Ack()
		evt, _ := protocol.UnmarshalEvent(m.Data)
		if evt.Type == protocol.EventWorkflowChildCompleted {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected workflow.child.completed on parent history")
	}
}

func TestOrchestratorRejectsExcessiveNesting(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")
	store := NewSnapshotStore(jsNew)

	// Register child workflow def
	childDef := dag.WorkflowDef{
		Name: "deep-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t", Type: dag.StepTypeNormal},
		},
	}
	childDefData := mustMarshal(t, childDef)
	mustPut(t, defKV, "deep-child", childDefData)

	// Create a chain: run-0 -> run-1 -> run-2 (depth 3)
	for i := 0; i < 3; i++ {
		run := dag.WorkflowRun{
			RunID: fmt.Sprintf("run-%d", i), WorkflowID: "deep-child",
			Status:    dag.RunStatusRunning,
			Steps:     map[string]dag.StepState{"s1": {Status: dag.StepStatusRunning}},
			CreatedAt: time.Now(),
		}
		if i > 0 {
			run.ParentRunID = fmt.Sprintf("run-%d", i-1)
			run.ParentStepID = "s1"
		}
		store.Save(context.Background(), run)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Try to spawn from run-2 (depth would be 4, exceeds 3)
	spawnPayload := mustMarshal(t, map[string]string{
		"child_run_id":   "run-3",
		"child_workflow": "deep-child",
		"parent_step_id": "s1",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "run-2", spawnPayload)
	data, err := spawnEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))

	// Poll briefly — run-3 should never be created
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := store.Load(context.Background(), "run-3"); err == nil {
			t.Fatalf("run-3 should not exist — nesting too deep")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForEventType polls a subscription for a specific event type.
func waitForEventType(
	sub *nats.Subscription,
	target protocol.EventType,
	timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		m.Ack()
		evt, _ := protocol.UnmarshalEvent(m.Data)
		if evt.Type == target {
			return true
		}
	}
	return false
}

func TestOrchestratorChildFailureNotifiesParent(t *testing.T) {
	// Methodology: spawn a child, fail it, verify parent gets
	// workflow.child.failed event.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	childDef := dag.WorkflowDef{
		Name: "fail-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "fail-task",
				Type: dag.StepTypeNormal},
		},
	}
	childDefData := mustMarshal(t, childDef)
	mustPut(t, defKV, "fail-child", childDefData)

	parentSub, err := js.SubscribeSync(
		"history.parent-fail",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	spawnPayload := mustMarshal(t, map[string]string{
		"child_run_id":   "child-fail-1",
		"child_workflow": "fail-child",
		"parent_step_id": "parent-step",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-fail",
		spawnPayload)
	data, err := spawnEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))
	waitForStepStatus(t, orch.store, "child-fail-1", "s1",
		dag.StepStatusQueued, 5*time.Second)

	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "child-fail-1", "s1",
		[]byte(`"child error"`))
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()))

	// Positive: parent notified.
	found := waitForEventType(parentSub,
		protocol.EventWorkflowChildFailed, 5*time.Second)
	if !found {
		t.Fatal("expected child.failed on parent")
	}
	// Positive: child run is Failed.
	store := NewSnapshotStore(jsNew)
	childRun, _ := store.Load(context.Background(), "child-fail-1")
	if childRun.Status != dag.RunStatusFailed {
		t.Fatalf("child = %v, want Failed",
			childRun.Status)
	}
}

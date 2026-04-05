// engine/subworkflow_test.go
// Integration tests for sub-workflow execution. Uses real embedded NATS server.
// Methodology: publish events to the history stream, let the orchestrator
// process them, then verify child runs are created, parent steps transition
// correctly, and cancellation cascades to non-detached children.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// registerWorkflowDef marshals and stores a WorkflowDef in the
// workflow_defs KV bucket for test setup.
func registerWorkflowDef(
	t *testing.T,
	js nats.JetStreamContext,
	def dag.WorkflowDef,
) {
	t.Helper()
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs): %v", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal workflow def: %v", err)
	}
	if _, err := defKV.Put(def.Name, data); err != nil {
		t.Fatalf("put workflow def: %v", err)
	}
}

// publishEvent publishes a protocol.Event to the history stream.
func publishEvent(
	t *testing.T,
	js nats.JetStreamContext,
	evt protocol.Event,
) {
	t.Helper()
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	_, err = js.Publish(
		evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()),
	)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
}

func TestSubWorkflow_ChildCompletesParentCompletes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Register child workflow with one normal step.
	childDef := dag.WorkflowDef{
		Name:    "child-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "child-step",
				Task: "child-task",
				Type: dag.StepTypeNormal,
			},
		},
	}
	registerWorkflowDef(t, js, childDef)

	// Register parent workflow with one sub-workflow step.
	parentDef := dag.WorkflowDef{
		Name:    "parent-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "spawn",
				Task: "child-wf",
				Type: dag.StepTypeSubWorkflow,
				Config: dag.MarshalConfig(&dag.SubWorkflowConfig{
					Workflow: "child-wf",
				}),
			},
		},
	}
	registerWorkflowDef(t, js, parentDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	store := NewSnapshotStore(jsNew)

	// Start the parent workflow.
	startPayload, _ := json.Marshal(parentDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "parent-run-1",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	// Wait for the child task to be enqueued.
	childSub, err := js.PullSubscribe(
		"task.child-task.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := childSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch child task: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 child task, got %d", len(msgs))
	}

	// Find the child run ID from the parent step state.
	var childRunID string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parentRun, err := store.Load("parent-run-1")
		if err == nil {
			state := parentRun.Steps["spawn"]
			if state.ChildRunID != "" {
				childRunID = state.ChildRunID
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if childRunID == "" {
		t.Fatal("child run ID not set on parent step")
	}

	// Parent step should be Running (not completed yet).
	parentRun, _ := store.Load("parent-run-1")
	if parentRun.Steps["spawn"].Status != dag.StepStatusRunning {
		t.Fatalf(
			"parent step status = %v, want Running",
			parentRun.Steps["spawn"].Status,
		)
	}

	// Complete the child step.
	childComplete := protocol.NewStepEvent(
		protocol.EventStepCompleted, childRunID,
		"child-step", []byte(`"child-output"`),
	)
	publishEvent(t, js, childComplete)

	// Wait for parent to complete.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parentRun, err = store.Load("parent-run-1")
		if err == nil &&
			parentRun.Status == dag.RunStatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: parent workflow completed.
	if parentRun.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"parent status = %v, want Completed",
			parentRun.Status,
		)
	}
	// Negative: parent step should also be completed.
	if parentRun.Steps["spawn"].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"parent step status = %v, want Completed",
			parentRun.Steps["spawn"].Status,
		)
	}
}

func TestSubWorkflow_ChildFailsParentFails(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	childDef := dag.WorkflowDef{
		Name:    "child-wf-fail",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "child-step",
				Task: "child-task-fail",
				Type: dag.StepTypeNormal,
			},
		},
	}
	registerWorkflowDef(t, js, childDef)

	parentDef := dag.WorkflowDef{
		Name:    "parent-wf-fail",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "spawn",
				Task: "child-wf-fail",
				Type: dag.StepTypeSubWorkflow,
				Config: dag.MarshalConfig(&dag.SubWorkflowConfig{
					Workflow: "child-wf-fail",
				}),
			},
		},
	}
	registerWorkflowDef(t, js, parentDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	store := NewSnapshotStore(jsNew)

	startPayload, _ := json.Marshal(parentDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "parent-run-fail",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	// Wait for child run to be created.
	var childRunID string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parentRun, err := store.Load("parent-run-fail")
		if err == nil {
			state := parentRun.Steps["spawn"]
			if state.ChildRunID != "" {
				childRunID = state.ChildRunID
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if childRunID == "" {
		t.Fatal("child run ID not set on parent step")
	}

	// Fail the child step.
	childFail := protocol.NewStepEvent(
		protocol.EventStepFailed, childRunID,
		"child-step", []byte(`"boom"`),
	)
	publishEvent(t, js, childFail)

	// Wait for parent to fail.
	deadline = time.Now().Add(5 * time.Second)
	var parentRun dag.WorkflowRun
	for time.Now().Before(deadline) {
		var err error
		parentRun, err = store.Load("parent-run-fail")
		if err == nil &&
			parentRun.Status == dag.RunStatusFailed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: parent workflow failed.
	if parentRun.Status != dag.RunStatusFailed {
		t.Fatalf(
			"parent status = %v, want Failed",
			parentRun.Status,
		)
	}
	// Negative: parent step should be failed.
	if parentRun.Steps["spawn"].Status != dag.StepStatusFailed {
		t.Fatalf(
			"parent step status = %v, want Failed",
			parentRun.Steps["spawn"].Status,
		)
	}
}

func TestSubWorkflow_DetachedCompletesImmediately(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	childDef := dag.WorkflowDef{
		Name:    "child-wf-detach",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "child-step",
				Task: "child-task-detach",
				Type: dag.StepTypeNormal,
			},
		},
	}
	registerWorkflowDef(t, js, childDef)

	parentDef := dag.WorkflowDef{
		Name:    "parent-wf-detach",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "spawn",
				Task: "child-wf-detach",
				Type: dag.StepTypeSubWorkflow,
				Config: dag.MarshalConfig(&dag.SubWorkflowConfig{
					Workflow: "child-wf-detach",
					Detach:   true,
				}),
			},
		},
	}
	registerWorkflowDef(t, js, parentDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	store := NewSnapshotStore(jsNew)

	startPayload, _ := json.Marshal(parentDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "parent-run-detach",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	// Wait for parent to complete (should happen immediately since
	// detached sub-workflow completes the parent step at spawn time).
	deadline := time.Now().Add(5 * time.Second)
	var parentRun dag.WorkflowRun
	for time.Now().Before(deadline) {
		var err error
		parentRun, err = store.Load("parent-run-detach")
		if err == nil &&
			parentRun.Status == dag.RunStatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: parent completed without waiting for child.
	if parentRun.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"parent status = %v, want Completed",
			parentRun.Status,
		)
	}
	// Negative: child run should still exist and be running.
	childRunID := parentRun.Steps["spawn"].ChildRunID
	if childRunID == "" {
		t.Fatal("child run ID not set on parent step")
	}
	childRun, err := store.Load(childRunID)
	if err != nil {
		t.Fatalf("load child run: %v", err)
	}
	// Detached child has no parent link.
	if childRun.ParentRunID != "" {
		t.Fatalf(
			"detached child ParentRunID = %q, want empty",
			childRun.ParentRunID,
		)
	}
}

func TestSubWorkflow_CancellationCascades(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	childDef := dag.WorkflowDef{
		Name:    "child-wf-cancel",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "child-step",
				Task: "child-task-cancel",
				Type: dag.StepTypeNormal,
			},
		},
	}
	registerWorkflowDef(t, js, childDef)

	parentDef := dag.WorkflowDef{
		Name:    "parent-wf-cancel",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "spawn",
				Task: "child-wf-cancel",
				Type: dag.StepTypeSubWorkflow,
				Config: dag.MarshalConfig(&dag.SubWorkflowConfig{
					Workflow: "child-wf-cancel",
				}),
			},
		},
	}
	registerWorkflowDef(t, js, parentDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	store := NewSnapshotStore(jsNew)

	startPayload, _ := json.Marshal(parentDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "parent-run-cancel",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	// Wait for child run to be created.
	var childRunID string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parentRun, err := store.Load("parent-run-cancel")
		if err == nil {
			state := parentRun.Steps["spawn"]
			if state.ChildRunID != "" {
				childRunID = state.ChildRunID
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if childRunID == "" {
		t.Fatal("child run ID not set on parent step")
	}

	// Cancel the parent.
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "parent-run-cancel", nil,
	)
	publishEvent(t, js, cancelEvt)

	// Wait for both parent and child to be cancelled.
	deadline = time.Now().Add(5 * time.Second)
	var parentRun dag.WorkflowRun
	var childRun dag.WorkflowRun
	for time.Now().Before(deadline) {
		var err1, err2 error
		parentRun, err1 = store.Load("parent-run-cancel")
		childRun, err2 = store.Load(childRunID)
		if err1 == nil && err2 == nil &&
			parentRun.Status == dag.RunStatusCancelled &&
			childRun.Status == dag.RunStatusCancelled {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: parent is cancelled.
	if parentRun.Status != dag.RunStatusCancelled {
		t.Fatalf(
			"parent status = %v, want Cancelled",
			parentRun.Status,
		)
	}
	// Negative: child is also cancelled (cascade).
	if childRun.Status != dag.RunStatusCancelled {
		t.Fatalf(
			"child status = %v, want Cancelled",
			childRun.Status,
		)
	}
}

func TestSubWorkflow_DetachedChildSurvivesCancel(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	childDef := dag.WorkflowDef{
		Name:    "child-wf-detach-cancel",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "child-step",
				Task: "child-task-detach-cancel",
				Type: dag.StepTypeNormal,
			},
		},
	}
	registerWorkflowDef(t, js, childDef)

	parentDef := dag.WorkflowDef{
		Name:    "parent-wf-detach-cancel",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "spawn",
				Task: "child-wf-detach-cancel",
				Type: dag.StepTypeSubWorkflow,
				Config: dag.MarshalConfig(
					&dag.SubWorkflowConfig{
						Workflow: "child-wf-detach-cancel",
						Detach:   true,
					},
				),
			},
		},
	}
	registerWorkflowDef(t, js, parentDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	store := NewSnapshotStore(jsNew)

	startPayload, _ := json.Marshal(parentDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"parent-run-detach-cancel",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	// Wait for parent to complete (detached completes immediately).
	var parentRun dag.WorkflowRun
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		parentRun, err = store.Load(
			"parent-run-detach-cancel",
		)
		if err == nil &&
			parentRun.Status == dag.RunStatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if parentRun.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"parent status = %v, want Completed",
			parentRun.Status,
		)
	}

	childRunID := parentRun.Steps["spawn"].ChildRunID
	if childRunID == "" {
		t.Fatal("child run ID not set on parent step")
	}

	// Cancel the parent.
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled,
		"parent-run-detach-cancel", nil,
	)
	publishEvent(t, js, cancelEvt)

	// Give orchestrator time to process cancellation.
	time.Sleep(500 * time.Millisecond)

	// Reload parent — it was already Completed, so cancel is
	// a no-op (not Running). Load child to verify it survived.
	childRun, err := store.Load(childRunID)
	if err != nil {
		t.Fatalf("load child run: %v", err)
	}

	// Positive: child is NOT cancelled — still Running.
	if childRun.Status != dag.RunStatusRunning {
		t.Fatalf(
			"child status = %v, want Running",
			childRun.Status,
		)
	}
	// Negative: parent remained Completed (not cancelled).
	parentAfter, err := store.Load("parent-run-detach-cancel")
	if err != nil {
		t.Fatalf("load parent after cancel: %v", err)
	}
	if parentAfter.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"parent after cancel = %v, want Completed",
			parentAfter.Status,
		)
	}
}

func TestSubWorkflow_ChildReceivesResolvedInput(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	childDef := dag.WorkflowDef{
		Name:    "child-wf-input",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "child-step",
				Task: "child-task-input",
				Type: dag.StepTypeNormal,
			},
		},
	}
	registerWorkflowDef(t, js, childDef)

	parentDef := dag.WorkflowDef{
		Name:    "parent-wf-input",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "step-a",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID:        "spawn-b",
				Task:      "child-wf-input",
				Type:      dag.StepTypeSubWorkflow,
				DependsOn: []string{"step-a"},
				Config: dag.MarshalConfig(
					&dag.SubWorkflowConfig{
						Workflow: "child-wf-input",
					},
				),
			},
		},
	}
	registerWorkflowDef(t, js, parentDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	store := NewSnapshotStore(jsNew)

	startPayload, _ := json.Marshal(parentDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"parent-run-input",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	// Complete step A with output.
	stepAOutput := []byte(`{"msg":"hello"}`)
	completeA := protocol.NewStepEvent(
		protocol.EventStepCompleted,
		"parent-run-input", "step-a", stepAOutput,
	)
	publishEvent(t, js, completeA)

	// Wait for the child run to be created.
	var childRunID string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parentRun, err := store.Load("parent-run-input")
		if err == nil {
			state := parentRun.Steps["spawn-b"]
			if state.ChildRunID != "" {
				childRunID = state.ChildRunID
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if childRunID == "" {
		t.Fatal("child run ID not set on parent step")
	}

	childRun, err := store.Load(childRunID)
	if err != nil {
		t.Fatalf("load child run: %v", err)
	}

	// Positive: child run input matches step A's output.
	if string(childRun.Input) != string(stepAOutput) {
		t.Fatalf(
			"child Input = %s, want %s",
			string(childRun.Input), string(stepAOutput),
		)
	}
	// Negative: child should not have parent's original input.
	parentRun, err := store.Load("parent-run-input")
	if err != nil {
		t.Fatalf("load parent run: %v", err)
	}
	if string(childRun.Input) == string(parentRun.Input) &&
		len(parentRun.Input) > 0 {
		t.Fatal(
			"child input should differ from parent input",
		)
	}
}

func TestSubWorkflow_MaxNestingDepthRejected(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Create a chain of workflow defs that would exceed max depth.
	leafDef := dag.WorkflowDef{
		Name:    "leaf-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "leaf-step",
				Task: "leaf-task",
				Type: dag.StepTypeNormal,
			},
		},
	}
	registerWorkflowDef(t, js, leafDef)

	store := NewSnapshotStore(jsNew)
	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Manually create a run chain at depth=2 (maxNestingDepth=3).
	// Level 0: root-run (parentRunID="")
	// Level 1: mid-run (parentRunID="root-run")
	// Level 2: deep-run (parentRunID="mid-run")
	rootRun := dag.NewWorkflowRun(leafDef, "root-run")
	rootRun.Status = dag.RunStatusRunning
	if err := store.Save(rootRun); err != nil {
		t.Fatalf("save root run: %v", err)
	}

	midRun := dag.NewWorkflowRun(leafDef, "mid-run")
	midRun.ParentRunID = "root-run"
	midRun.Status = dag.RunStatusRunning
	if err := store.Save(midRun); err != nil {
		t.Fatalf("save mid run: %v", err)
	}

	deepRun := dag.NewWorkflowRun(leafDef, "deep-run")
	deepRun.ParentRunID = "mid-run"
	deepRun.Status = dag.RunStatusRunning
	if err := store.Save(deepRun); err != nil {
		t.Fatalf("save deep run: %v", err)
	}

	// Attempt to spawn from depth=2, which would create depth=3.
	spawnPayload, _ := json.Marshal(map[string]any{
		"child_run_id":   "rejected-child",
		"child_workflow": "leaf-wf",
		"parent_step_id": "spawn",
	})
	spawnEvt := protocol.NewStepEvent(
		protocol.EventWorkflowSpawn, "deep-run",
		"spawn", spawnPayload,
	)
	data, _ := spawnEvt.Marshal()

	// Publish and wait for the message to be processed.
	_, err = js.Publish(
		spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()),
	)
	if err != nil {
		t.Fatalf("publish spawn event: %v", err)
	}

	// Give orchestrator time to process.
	time.Sleep(1 * time.Second)

	// Positive: the rejected child should not exist.
	_, err = store.Load("rejected-child")
	if err == nil {
		t.Fatal("expected child run to not exist (nesting exceeded)")
	}
	// Negative: the deep run should still be running.
	deepRunAfter, err := store.Load("deep-run")
	if err != nil {
		t.Fatalf("load deep run: %v", err)
	}
	if deepRunAfter.Status != dag.RunStatusRunning {
		t.Fatalf(
			"deep run status = %v, want Running",
			deepRunAfter.Status,
		)
	}
}

// engine/planner_test.go
// Integration tests for dynamic DAG generation via StepTypePlanner.
// Methodology: register a workflow with a planner step, start a run,
// simulate the planner worker completing with a JSON fragment, then
// verify that generated steps materialize and execute to completion.
// Each test uses a fresh embedded NATS server for isolation.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestPlanner_MaterializeAndComplete(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	// Workflow: planner step that generates two normal steps.
	wfDef := dag.WorkflowDef{
		Name:    "planner-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "plan",
				Task: "generate-plan",
				Type: dag.StepTypePlanner,
				Config: dag.MarshalConfig(&dag.PlannerConfig{
					MaxSteps: 10,
				}),
			},
		},
	}
	registerWorkflowDef(t, js, wfDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()
	store := NewSnapshotStore(js)

	// Subscribe to the planner task queue.
	planSub, err := js.PullSubscribe(
		"task.generate-plan.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}

	// Start the workflow.
	startPayload, _ := json.Marshal(wfDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "plan-run-1",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	// Wait for planner task to be dispatched.
	msgs, err := planSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch planner task: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 planner task, got %d", len(msgs))
	}

	// Simulate planner worker output: two steps, b depends on a.
	planOutput, _ := json.Marshal(map[string]interface{}{
		"steps": []map[string]interface{}{
			{"id": "a", "task": "build", "type": "normal"},
			{
				"id":         "b",
				"task":       "test",
				"type":       "normal",
				"depends_on": []string{"a"},
			},
		},
	})

	// Publish step.completed for the planner with the fragment.
	completeEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "plan-run-1",
		"plan", planOutput,
	)
	publishEvent(t, js, completeEvt)

	// Wait for the generated "build" task to be dispatched.
	buildSub, err := js.PullSubscribe(
		"task.build.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe build: %v", err)
	}
	buildMsgs, err := buildSub.Fetch(
		1, nats.MaxWait(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Fetch build task: %v", err)
	}
	if len(buildMsgs) != 1 {
		t.Fatalf("expected 1 build task, got %d", len(buildMsgs))
	}

	// Verify dynamic steps exist in snapshot.
	run, err := store.Load("plan-run-1")
	if err != nil {
		t.Fatalf("Load snapshot: %v", err)
	}
	if len(run.DynamicSteps) != 2 {
		t.Fatalf(
			"expected 2 dynamic steps, got %d",
			len(run.DynamicSteps),
		)
	}

	// Complete the build step (plan.a).
	buildComplete := protocol.NewStepEvent(
		protocol.EventStepCompleted, "plan-run-1",
		"plan.a", []byte(`"build-output"`),
	)
	publishEvent(t, js, buildComplete)

	// Wait for "test" task to be dispatched.
	testSub, err := js.PullSubscribe(
		"task.test.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe test: %v", err)
	}
	testMsgs, err := testSub.Fetch(
		1, nats.MaxWait(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Fetch test task: %v", err)
	}
	if len(testMsgs) != 1 {
		t.Fatalf("expected 1 test task, got %d", len(testMsgs))
	}

	// Complete the test step (plan.b).
	testComplete := protocol.NewStepEvent(
		protocol.EventStepCompleted, "plan-run-1",
		"plan.b", []byte(`"test-output"`),
	)
	publishEvent(t, js, testComplete)

	// Wait for workflow to complete.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err = store.Load("plan-run-1")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: workflow completed.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"workflow status = %v, want Completed",
			run.Status,
		)
	}
	// Negative: no steps should be pending or failed.
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusPending ||
			state.Status == dag.StepStatusFailed {
			t.Errorf(
				"step %q has unexpected status %v",
				id, state.Status,
			)
		}
	}
}

func TestPlanner_MaxStepsExceeded(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "planner-max-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "plan",
				Task: "generate-plan",
				Type: dag.StepTypePlanner,
				Config: dag.MarshalConfig(&dag.PlannerConfig{
					MaxSteps: 1,
				}),
			},
		},
	}
	registerWorkflowDef(t, js, wfDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()
	store := NewSnapshotStore(js)

	planSub, err := js.PullSubscribe(
		"task.generate-plan.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}

	startPayload, _ := json.Marshal(wfDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "plan-max-run",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	msgs, err := planSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	// Output 2 steps but MaxSteps is 1.
	planOutput, _ := json.Marshal(map[string]interface{}{
		"steps": []map[string]interface{}{
			{"id": "a", "task": "t1", "type": "normal"},
			{"id": "b", "task": "t2", "type": "normal"},
		},
	})

	completeEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "plan-max-run",
		"plan", planOutput,
	)
	publishEvent(t, js, completeEvt)

	// Wait for workflow to fail.
	deadline := time.Now().Add(5 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		run, err = store.Load("plan-max-run")
		if err == nil && run.Status == dag.RunStatusFailed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: workflow failed.
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("workflow status = %v, want Failed", run.Status)
	}
	// Negative: no dynamic steps should have been added.
	if len(run.DynamicSteps) != 0 {
		t.Errorf(
			"expected 0 dynamic steps, got %d",
			len(run.DynamicSteps),
		)
	}
}

func TestPlanner_AllowedTasksViolation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "planner-allowed-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "plan",
				Task: "generate-plan",
				Type: dag.StepTypePlanner,
				Config: dag.MarshalConfig(&dag.PlannerConfig{
					MaxSteps:     10,
					AllowedTasks: []string{"build"},
				}),
			},
		},
	}
	registerWorkflowDef(t, js, wfDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()
	store := NewSnapshotStore(js)

	planSub, err := js.PullSubscribe(
		"task.generate-plan.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}

	startPayload, _ := json.Marshal(wfDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "plan-allowed-run",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	msgs, err := planSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	// Output a step with a disallowed task type.
	planOutput, _ := json.Marshal(map[string]interface{}{
		"steps": []map[string]interface{}{
			{"id": "a", "task": "forbidden", "type": "normal"},
		},
	})

	completeEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "plan-allowed-run",
		"plan", planOutput,
	)
	publishEvent(t, js, completeEvt)

	deadline := time.Now().Add(5 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		run, err = store.Load("plan-allowed-run")
		if err == nil && run.Status == dag.RunStatusFailed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: workflow failed due to disallowed task.
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("workflow status = %v, want Failed", run.Status)
	}
	// Negative: planner step should be Failed.
	planState := run.Steps["plan"]
	if planState.Status != dag.StepStatusFailed {
		t.Errorf(
			"plan step status = %v, want Failed",
			planState.Status,
		)
	}
}

func TestPlanner_CycleInFragment(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "planner-cycle-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "plan",
				Task: "generate-plan",
				Type: dag.StepTypePlanner,
				Config: dag.MarshalConfig(&dag.PlannerConfig{
					MaxSteps: 10,
				}),
			},
		},
	}
	registerWorkflowDef(t, js, wfDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()
	store := NewSnapshotStore(js)

	planSub, err := js.PullSubscribe(
		"task.generate-plan.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}

	startPayload, _ := json.Marshal(wfDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "plan-cycle-run",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	msgs, err := planSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	// Output a cycle: a -> b -> a.
	planOutput, _ := json.Marshal(map[string]interface{}{
		"steps": []map[string]interface{}{
			{
				"id": "a", "task": "t1", "type": "normal",
				"depends_on": []string{"b"},
			},
			{
				"id": "b", "task": "t2", "type": "normal",
				"depends_on": []string{"a"},
			},
		},
	})

	completeEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "plan-cycle-run",
		"plan", planOutput,
	)
	publishEvent(t, js, completeEvt)

	deadline := time.Now().Add(5 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		run, err = store.Load("plan-cycle-run")
		if err == nil && run.Status == dag.RunStatusFailed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: workflow failed due to cycle.
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("workflow status = %v, want Failed", run.Status)
	}
	// Negative: no dynamic steps materialized.
	if len(run.DynamicSteps) != 0 {
		t.Errorf(
			"expected 0 dynamic steps, got %d",
			len(run.DynamicSteps),
		)
	}
}

func TestPlanner_EmptyPlan(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "planner-empty-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "plan",
				Task: "generate-plan",
				Type: dag.StepTypePlanner,
				Config: dag.MarshalConfig(&dag.PlannerConfig{
					MaxSteps: 10,
				}),
			},
		},
	}
	registerWorkflowDef(t, js, wfDef)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()
	store := NewSnapshotStore(js)

	planSub, err := js.PullSubscribe(
		"task.generate-plan.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}

	startPayload, _ := json.Marshal(wfDef)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "plan-empty-run",
		startPayload,
	)
	publishEvent(t, js, startEvt)

	msgs, err := planSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	// Empty plan: no steps.
	planOutput, _ := json.Marshal(map[string]interface{}{
		"steps": []interface{}{},
	})

	completeEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "plan-empty-run",
		"plan", planOutput,
	)
	publishEvent(t, js, completeEvt)

	// Workflow should complete since the only step (plan) is done.
	deadline := time.Now().Add(5 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		run, err = store.Load("plan-empty-run")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: workflow completed.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"workflow status = %v, want Completed",
			run.Status,
		)
	}
	// Negative: no dynamic steps.
	if len(run.DynamicSteps) != 0 {
		t.Errorf(
			"expected 0 dynamic steps, got %d",
			len(run.DynamicSteps),
		)
	}
}

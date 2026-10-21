// engine/spawn_idempotent_test.go
// Redelivery safety for workflow.spawn (#803): handling the same spawn
// event twice must not reset a child run that has already advanced.
// Methodology: real embedded NATS, orchestrator constructed but not
// started so the test drives handleWorkflowSpawn directly and nothing
// races the manual snapshot edit. Positive space: the first delivery
// creates the child. Negative space: after the child's step is marked
// completed, a second delivery leaves that state untouched. Crash-gap
// test: a child saved but never enqueued gets its first step queued by the
// next delivery.
package engine

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func TestHandleWorkflowSpawn_RedeliveryKeepsAdvancedChild(t *testing.T) {
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
		Name: "redeliver-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t", Type: dag.StepTypeNormal},
		},
	}
	data := mustMarshal(t, childDef)
	mustPut(t, defKV, "redeliver-child", data)
	mustPut(t, defKV,
		dag.DefVersionKey(childDef.Name, dag.DefHash(childDef)), data)

	orch := NewOrchestrator(nc)
	payload := mustMarshal(t, map[string]string{
		"child_run_id":   "child-redeliver",
		"child_workflow": "redeliver-child",
		"parent_step_id": "ps",
	})
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-redeliver", payload)
	ctx := context.Background()
	if err := orch.handleWorkflowSpawn(ctx, evt); err != nil {
		t.Fatalf("first spawn: %v", err)
	}

	store := NewSnapshotStore(jsNew)
	run, err := store.Load(ctx, "child-redeliver")
	if err != nil {
		t.Fatalf("child should exist: %v", err)
	}
	state := run.Steps["s1"]
	state.Status = dag.StepStatusCompleted
	run.Steps["s1"] = state
	if err := store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("advance child: %v", err)
	}

	if err := orch.handleWorkflowSpawn(ctx, evt); err != nil {
		t.Fatalf("redelivered spawn: %v", err)
	}
	got, err := store.Load(ctx, "child-redeliver")
	if err != nil {
		t.Fatalf("load after redelivery: %v", err)
	}
	if got.Steps["s1"].Status != dag.StepStatusCompleted {
		t.Fatalf("redelivery reset child step to %v, want completed",
			got.Steps["s1"].Status)
	}
	if got.ParentRunID != "parent-redeliver" {
		t.Fatalf("ParentRunID = %q", got.ParentRunID)
	}
}

// TestHandleWorkflowSpawn_HealsSavedButNeverEnqueuedChild simulates a crash
// between the child's initial save and enqueueReady: the redelivered spawn
// must enqueue the first step instead of skipping the existing child.
func TestHandleWorkflowSpawn_HealsSavedButNeverEnqueuedChild(t *testing.T) {
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
		Name: "heal-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t", Type: dag.StepTypeNormal},
		},
	}
	data := mustMarshal(t, childDef)
	mustPut(t, defKV, "heal-child", data)
	mustPut(t, defKV,
		dag.DefVersionKey(childDef.Name, dag.DefHash(childDef)), data)

	orch := NewOrchestrator(nc)
	ctx := context.Background()
	run := dag.NewWorkflowRun(childDef, "child-heal")
	run.Status = dag.RunStatusRunning
	run.Revision = dag.NewSnapshotRevision(0)
	if err := orch.saveInitialSnapshot(ctx, run); err != nil {
		t.Fatalf("seed saved-not-enqueued child: %v", err)
	}
	store := NewSnapshotStore(jsNew)
	before, err := store.Load(ctx, "child-heal")
	if err != nil || before.Steps["s1"].Status != dag.StepStatusPending {
		t.Fatalf("seed state = %v, %v; want pending", before.Steps["s1"], err)
	}

	payload := mustMarshal(t, map[string]string{
		"child_run_id":   "child-heal",
		"child_workflow": "heal-child",
		"parent_step_id": "ps",
	})
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-heal", payload)
	if err := orch.handleWorkflowSpawn(ctx, evt); err != nil {
		t.Fatalf("redelivered spawn: %v", err)
	}
	after, err := store.Load(ctx, "child-heal")
	if err != nil {
		t.Fatalf("load after heal: %v", err)
	}
	if after.Steps["s1"].Status != dag.StepStatusQueued {
		t.Fatalf("first step = %v, want queued after heal",
			after.Steps["s1"].Status)
	}
}

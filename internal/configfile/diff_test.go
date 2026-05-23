package configfile

// Methodology: pure unit tests for the Diff function. No I/O.
// Each scenario exercises exactly one of add / update / remove /
// no-op so a regression in the diff segmentation surfaces with
// a precise failure.

import (
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

func makeTrigger(id, wf string, enabled bool) trigger.TriggerDef {
	return trigger.TriggerDef{
		ID:         id,
		WorkflowID: wf,
		Enabled:    enabled,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *",
		},
	}
}

func makeWorkflow(name string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name: name,
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo"},
		},
	}
}

func TestDiffEmpty(t *testing.T) {
	plan := Diff(CurrentState{}, DesiredState{})
	if !plan.Empty() {
		t.Fatalf("Plan should be empty, got %+v", plan)
	}
}

func TestDiffAddsNewTrigger(t *testing.T) {
	desired := DesiredState{
		Triggers: map[string]trigger.TriggerDef{
			"t1": makeTrigger("t1", "wf", true),
		},
	}
	plan := Diff(CurrentState{}, desired)
	if len(plan.TriggersAdd) != 1 || plan.TriggersAdd[0].ID != "t1" {
		t.Fatalf("TriggersAdd = %+v, want 1 trigger t1",
			plan.TriggersAdd)
	}
	if len(plan.TriggersRemove) != 0 {
		t.Fatalf("TriggersRemove = %+v, want 0",
			plan.TriggersRemove)
	}
}

func TestDiffRemovesTrigger(t *testing.T) {
	current := CurrentState{
		Triggers: map[string]trigger.TriggerDef{
			"t1": makeTrigger("t1", "wf", true),
		},
	}
	plan := Diff(current, DesiredState{})
	if len(plan.TriggersRemove) != 1 || plan.TriggersRemove[0] != "t1" {
		t.Fatalf("TriggersRemove = %+v, want [t1]",
			plan.TriggersRemove)
	}
}

func TestDiffUpdatesChangedTrigger(t *testing.T) {
	current := CurrentState{
		Triggers: map[string]trigger.TriggerDef{
			"t1": makeTrigger("t1", "wf", true),
		},
	}
	updated := makeTrigger("t1", "wf", false) // Enabled flipped
	desired := DesiredState{
		Triggers: map[string]trigger.TriggerDef{"t1": updated},
	}
	plan := Diff(current, desired)
	if len(plan.TriggersUpdate) != 1 ||
		plan.TriggersUpdate[0].Enabled {
		t.Fatalf("TriggersUpdate = %+v, want disabled t1",
			plan.TriggersUpdate)
	}
	if len(plan.TriggersAdd) != 0 || len(plan.TriggersRemove) != 0 {
		t.Fatalf("plan should only update, got %+v", plan)
	}
}

func TestDiffNoOpWhenEqual(t *testing.T) {
	tr := makeTrigger("t1", "wf", true)
	plan := Diff(
		CurrentState{
			Triggers: map[string]trigger.TriggerDef{"t1": tr},
		},
		DesiredState{
			Triggers: map[string]trigger.TriggerDef{"t1": tr},
		},
	)
	if !plan.Empty() {
		t.Fatalf("plan should be empty, got %+v", plan)
	}
}

func TestDiffIgnoresSourceWhenComparing(t *testing.T) {
	currentDef := makeTrigger("t1", "wf", true)
	currentDef.Source = ""
	desiredDef := makeTrigger("t1", "wf", true)
	desiredDef.Source = SourceFilePrefix + "dagnats.yaml"
	plan := Diff(
		CurrentState{
			Triggers: map[string]trigger.TriggerDef{"t1": currentDef},
		},
		DesiredState{
			Triggers: map[string]trigger.TriggerDef{"t1": desiredDef},
		},
	)
	if !plan.Empty() {
		t.Fatalf("Source diff should not trigger update, got %+v",
			plan)
	}
}

func TestDiffAddsAndRemovesWorkflows(t *testing.T) {
	current := CurrentState{
		Workflows: map[string]dag.WorkflowDef{
			"old": makeWorkflow("old"),
		},
	}
	desired := DesiredState{
		Workflows: map[string]dag.WorkflowDef{
			"new": makeWorkflow("new"),
		},
	}
	plan := Diff(current, desired)
	if len(plan.WorkflowsAdd) != 1 || plan.WorkflowsAdd[0].Name != "new" {
		t.Fatalf("WorkflowsAdd = %+v", plan.WorkflowsAdd)
	}
	if len(plan.WorkflowsRemove) != 1 || plan.WorkflowsRemove[0] != "old" {
		t.Fatalf("WorkflowsRemove = %+v", plan.WorkflowsRemove)
	}
}

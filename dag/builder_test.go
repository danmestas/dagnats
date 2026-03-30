// dag/builder_test.go

// Tests for the WorkflowBuilder: verifies Build() produces correct
// WorkflowDefs and that Validate integration catches structural errors.
// StepRef-specific tests live in stepref_test.go.
package dag

import "testing"

func TestBuilderProducesValidDef(t *testing.T) {
	wf := NewWorkflow("basic")
	a := wf.Task("a", "task-a")
	wf.Task("b", "task-b").After(a)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if def.Name != "basic" {
		t.Fatalf("Name = %q, want %q", def.Name, "basic")
	}
	if len(def.Steps) != 2 {
		t.Fatalf("Steps = %d, want 2", len(def.Steps))
	}
}

func TestBuilderRejectsInvalidDef(t *testing.T) {
	wf := NewWorkflow("bad")
	wf.Task("a", "task-a").After(StepRef{id: "nope", index: 0, builder: wf})

	_, err := wf.Build()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestBuilderName(t *testing.T) {
	wf := NewWorkflow("my-workflow")
	if wf.Name() != "my-workflow" {
		t.Fatalf("Name() = %q, want %q", wf.Name(), "my-workflow")
	}
}

func TestBuilderVersion(t *testing.T) {
	wf := NewWorkflow("v")
	wf.Version("2.0")
	wf.Task("a", "task-a")
	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if def.Version != "2.0" {
		t.Fatalf("Version = %q, want %q", def.Version, "2.0")
	}
}

func findStep(def WorkflowDef, id string) *StepDef {
	for i := range def.Steps {
		if def.Steps[i].ID == id {
			return &def.Steps[i]
		}
	}
	return nil
}

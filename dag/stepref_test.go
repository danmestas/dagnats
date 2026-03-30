// dag/stepref_test.go

// Tests for StepRef: compile-time-safe step references in the workflow builder.
// Methodology: verify that After() wires dependencies correctly, that modifier
// methods target the right step, and that misuse panics are caught.
package dag

import (
	"testing"
	"time"
)

func TestStepRefLinearChain(t *testing.T) {
	wf := NewWorkflow("ref-linear")
	a := wf.Task("a", "task-a")
	b := wf.Task("b", "task-b").After(a)
	_ = wf.Task("c", "task-c").After(b)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(def.Steps) != 3 {
		t.Fatalf("Steps count = %d, want 3", len(def.Steps))
	}
	stepB := findStep(def, "b")
	if stepB == nil {
		t.Fatal("step 'b' not found")
	}
	if len(stepB.DependsOn) != 1 || stepB.DependsOn[0] != "a" {
		t.Fatalf("step 'b' DependsOn = %v, want [a]", stepB.DependsOn)
	}
}

func TestStepRefFanOutFanIn(t *testing.T) {
	wf := NewWorkflow("ref-fan")
	root := wf.Task("root", "task-root")
	left := wf.Task("left", "task-left").After(root)
	right := wf.Task("right", "task-right").After(root)
	_ = wf.Task("join", "task-join").After(left, right)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(def.Steps) != 4 {
		t.Fatalf("Steps count = %d, want 4", len(def.Steps))
	}
	join := findStep(def, "join")
	if join == nil {
		t.Fatal("step 'join' not found")
	}
	if len(join.DependsOn) != 2 {
		t.Fatalf("join.DependsOn count = %d, want 2", len(join.DependsOn))
	}
}

func TestStepRefAgentLoop(t *testing.T) {
	wf := NewWorkflow("ref-loop")
	prep := wf.Task("prep", "task-prep")
	fix := wf.AgentLoop("fix", "task-fix").After(prep).
		WithMaxIterations(10).WithMaxDuration(5 * time.Minute)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	_ = fix
	step := findStep(def, "fix")
	if step == nil {
		t.Fatal("step 'fix' not found")
	}
	if step.Type != StepTypeAgentLoop {
		t.Fatalf("fix.Type = %v, want AgentLoop", step.Type)
	}
	if step.Loop.MaxIterations != 10 {
		t.Fatalf("MaxIterations = %d, want 10", step.Loop.MaxIterations)
	}
	if step.Loop.MaxDuration != 5*time.Minute {
		t.Fatalf("MaxDuration = %v, want 5m", step.Loop.MaxDuration)
	}
}

func TestStepRefWithTimeout(t *testing.T) {
	wf := NewWorkflow("ref-timeout")
	_ = wf.Task("a", "task-a").WithTimeout(30 * time.Second)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	step := findStep(def, "a")
	if step.Timeout != 30*time.Second {
		t.Fatalf("Timeout = %v, want 30s", step.Timeout)
	}
	if step.Loop != nil {
		t.Fatal("normal step should not have Loop config")
	}
}

func TestStepRefID(t *testing.T) {
	wf := NewWorkflow("ref-id")
	ref := wf.Task("my-step", "my-task")
	if ref.ID() != "my-step" {
		t.Fatalf("ID() = %q, want %q", ref.ID(), "my-step")
	}
	if wf.Name() != "ref-id" {
		t.Fatalf("Name() = %q, want %q", wf.Name(), "ref-id")
	}
}

func TestStepRefCrossBuilderPanics(t *testing.T) {
	wf1 := NewWorkflow("wf1")
	wf2 := NewWorkflow("wf2")
	a := wf1.Task("a", "task-a")
	b := wf2.Task("b", "task-b")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for cross-builder After, got nil")
		}
	}()
	_ = b.After(a)
}

func TestStepRefZeroValuePanics(t *testing.T) {
	var ref StepRef

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for zero-value After, got nil")
		}
	}()
	_ = ref.After()
}

func TestStepRefWithMaxIterationsOnNormalPanics(t *testing.T) {
	wf := NewWorkflow("bad")
	ref := wf.Task("a", "task-a")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for WithMaxIterations on Task")
		}
	}()
	_ = ref.WithMaxIterations(10)
}

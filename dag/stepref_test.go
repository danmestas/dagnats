// dag/stepref_test.go

// Tests for StepRef: compile-time-safe step references in the builder.
// Methodology: verify After() wires dependencies, modifier methods
// target the right step, misuse panics are caught, and all modifiers
// compose correctly.
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
		t.Fatalf("Steps = %d, want 3", len(def.Steps))
	}
	stepB := findStep(def, "b")
	if len(stepB.DependsOn) != 1 || stepB.DependsOn[0] != "a" {
		t.Fatalf("b.DependsOn = %v, want [a]", stepB.DependsOn)
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
	join := findStep(def, "join")
	if len(join.DependsOn) != 2 {
		t.Fatalf("join.DependsOn = %d, want 2", len(join.DependsOn))
	}
}

func TestStepRefAgentLoop(t *testing.T) {
	wf := NewWorkflow("ref-loop")
	prep := wf.Task("prep", "task-prep")
	_ = wf.AgentLoop("fix", "task-fix").After(prep).
		WithMaxIterations(10).WithMaxDuration(5 * time.Minute)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	fix := findStep(def, "fix")
	if fix.Type != StepTypeAgentLoop {
		t.Fatalf("fix.Type = %v, want AgentLoop", fix.Type)
	}
	if fix.Loop.MaxIterations != 10 {
		t.Fatalf("MaxIterations = %d, want 10", fix.Loop.MaxIterations)
	}
	if fix.Loop.MaxDuration != 5*time.Minute {
		t.Fatalf("MaxDuration = %v, want 5m", fix.Loop.MaxDuration)
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

func TestStepRefWithRetries(t *testing.T) {
	wf := NewWorkflow("ref-retries")
	_ = wf.Task("a", "task-a").WithRetries(3)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	step := findStep(def, "a")
	if step.Retries != 3 {
		t.Fatalf("Retries = %d, want 3", step.Retries)
	}
}

func TestStepRefWithLoopDelay(t *testing.T) {
	wf := NewWorkflow("ref-delay")
	_ = wf.AgentLoop("a", "task-a").
		WithMaxIterations(5).
		WithLoopDelay(100 * time.Millisecond)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	step := findStep(def, "a")
	if step.Loop.LoopDelay != 100*time.Millisecond {
		t.Fatalf("LoopDelay = %v, want 100ms", step.Loop.LoopDelay)
	}
}

func TestStepRefSkipIfWithChaining(t *testing.T) {
	wf := NewWorkflow("ref-skipif")
	a := wf.Task("a", "task-a")
	_ = wf.Task("b", "task-b").
		After(a).
		WithTimeout(10 * time.Second).
		SkipIf(SkipIfOutput(a, "count", "<", float64(10))).
		WithRetries(2)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	step := findStep(def, "b")
	if step.SkipIf == nil {
		t.Fatal("SkipIf should not be nil")
	}
	if step.SkipIf.StepID != "a" {
		t.Fatalf("SkipIf.StepID = %q, want %q", step.SkipIf.StepID, "a")
	}
	if step.Timeout != 10*time.Second {
		t.Fatalf("Timeout = %v, want 10s", step.Timeout)
	}
	if step.Retries != 2 {
		t.Fatalf("Retries = %d, want 2", step.Retries)
	}
}

func TestStepRefID(t *testing.T) {
	wf := NewWorkflow("ref-id")
	ref := wf.Task("my-step", "my-task")
	if ref.ID() != "my-step" {
		t.Fatalf("ID() = %q, want %q", ref.ID(), "my-step")
	}
}

func TestStepRefCrossBuilderPanics(t *testing.T) {
	wf1 := NewWorkflow("wf1")
	wf2 := NewWorkflow("wf2")
	a := wf1.Task("a", "task-a")
	b := wf2.Task("b", "task-b")

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for cross-builder After")
		}
	}()
	_ = b.After(a)
}

func TestStepRefZeroValuePanics(t *testing.T) {
	var ref StepRef
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero-value After")
		}
	}()
	_ = ref.After()
}

func TestStepRefWithMaxIterationsOnNormalPanics(t *testing.T) {
	wf := NewWorkflow("bad")
	ref := wf.Task("a", "task-a")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for WithMaxIterations on Task")
		}
	}()
	_ = ref.WithMaxIterations(10)
}

func TestStepRefWithLoopDelayOnNormalPanics(t *testing.T) {
	wf := NewWorkflow("bad")
	ref := wf.Task("a", "task-a")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for WithLoopDelay on Task")
		}
	}()
	_ = ref.WithLoopDelay(time.Second)
}

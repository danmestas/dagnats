// dag/condition_test.go

// Tests for ParentCond evaluation: numeric, string, and bool comparisons,
// missing fields, nil output, and all six operators.
// Methodology: construct step states with known output, evaluate conditions,
// assert both positive and negative results.
package dag

import "testing"

func makeSteps(stepID string, output string) map[string]StepState {
	return map[string]StepState{
		stepID: {
			Status: StepStatusCompleted,
			Output: []byte(output),
		},
	}
}

func TestParentCondNumericOperators(t *testing.T) {
	steps := makeSteps("s1", `{"count": 5}`)

	tests := []struct {
		op   string
		val  interface{}
		want bool
	}{
		{"==", float64(5), true},
		{"==", float64(6), false},
		{"!=", float64(6), true},
		{"!=", float64(5), false},
		{"<", float64(10), true},
		{"<", float64(3), false},
		{">", float64(3), true},
		{">", float64(10), false},
		{"<=", float64(5), true},
		{"<=", float64(4), false},
		{">=", float64(5), true},
		{">=", float64(6), false},
	}
	for _, tt := range tests {
		cond := &ParentCond{StepID: "s1", Field: "count", Op: tt.op, Value: tt.val}
		got := cond.Evaluate(steps)
		if got != tt.want {
			t.Errorf("count %s %v = %v, want %v", tt.op, tt.val, got, tt.want)
		}
	}
}

func TestParentCondStringComparison(t *testing.T) {
	steps := makeSteps("s1", `{"name": "beta"}`)

	cond := &ParentCond{StepID: "s1", Field: "name", Op: "==", Value: "beta"}
	if !cond.Evaluate(steps) {
		t.Fatal("expected name == beta to be true")
	}

	cond = &ParentCond{StepID: "s1", Field: "name", Op: "<", Value: "gamma"}
	if !cond.Evaluate(steps) {
		t.Fatal("expected name < gamma to be true")
	}
}

func TestParentCondBoolComparison(t *testing.T) {
	steps := makeSteps("s1", `{"done": true}`)

	cond := &ParentCond{StepID: "s1", Field: "done", Op: "==", Value: true}
	if !cond.Evaluate(steps) {
		t.Fatal("expected done == true to be true")
	}

	cond = &ParentCond{StepID: "s1", Field: "done", Op: "!=", Value: false}
	if !cond.Evaluate(steps) {
		t.Fatal("expected done != false to be true")
	}
}

func TestParentCondMissingField(t *testing.T) {
	steps := makeSteps("s1", `{"count": 5}`)

	cond := &ParentCond{StepID: "s1", Field: "missing", Op: "==", Value: float64(5)}
	if cond.Evaluate(steps) {
		t.Fatal("expected missing field to return false")
	}
}

func TestParentCondNilOutput(t *testing.T) {
	steps := map[string]StepState{
		"s1": {Status: StepStatusCompleted, Output: nil},
	}

	cond := &ParentCond{StepID: "s1", Field: "count", Op: "==", Value: float64(5)}
	if cond.Evaluate(steps) {
		t.Fatal("expected nil output to return false")
	}
}

func TestParentCondMissingStep(t *testing.T) {
	steps := map[string]StepState{}

	cond := &ParentCond{StepID: "s1", Field: "count", Op: "==", Value: float64(5)}
	if cond.Evaluate(steps) {
		t.Fatal("expected missing step to return false")
	}
}

func TestParentCondNilCondition(t *testing.T) {
	steps := makeSteps("s1", `{"count": 5}`)
	var cond *ParentCond
	if cond.Evaluate(steps) {
		t.Fatal("expected nil condition to return false")
	}
}

func TestParentCondTypeMismatch(t *testing.T) {
	steps := makeSteps("s1", `{"count": 5}`)

	// Comparing number to string should return false.
	cond := &ParentCond{StepID: "s1", Field: "count", Op: "==", Value: "five"}
	if cond.Evaluate(steps) {
		t.Fatal("expected type mismatch to return false")
	}
}

func TestResolveSkippedBasic(t *testing.T) {
	wf := NewWorkflow("skip-test")
	a := wf.Task("a", "task-a")
	wf.Task("b", "task-b").After(a).SkipIf(
		SkipIfOutput(a, "count", "<", float64(10)),
	)
	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	steps := map[string]StepState{
		"a": {Status: StepStatusCompleted, Output: []byte(`{"count": 5}`)},
		"b": {Status: StepStatusPending},
	}
	completed := map[string]bool{"a": true}
	queued := map[string]bool{}

	skipped := ResolveSkipped(def, completed, queued, steps)
	if len(skipped) != 1 {
		t.Fatalf("expected 1 skipped step, got %d", len(skipped))
	}
	if skipped[0].ID != "b" {
		t.Fatalf("expected step 'b' skipped, got %q", skipped[0].ID)
	}
}

func TestResolveSkippedConditionFalse(t *testing.T) {
	wf := NewWorkflow("no-skip")
	a := wf.Task("a", "task-a")
	wf.Task("b", "task-b").After(a).SkipIf(
		SkipIfOutput(a, "count", "<", float64(10)),
	)
	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	steps := map[string]StepState{
		"a": {Status: StepStatusCompleted, Output: []byte(`{"count": 50}`)},
		"b": {Status: StepStatusPending},
	}
	completed := map[string]bool{"a": true}
	queued := map[string]bool{}

	skipped := ResolveSkipped(def, completed, queued, steps)
	if len(skipped) != 0 {
		t.Fatalf("expected 0 skipped steps, got %d", len(skipped))
	}
}

func TestValidateSkipIfInvalidOp(t *testing.T) {
	wf := NewWorkflow("bad-op")
	a := wf.Task("a", "task-a")
	wf.Task("b", "task-b").After(a).SkipIf(
		&ParentCond{StepID: "a", Field: "x", Op: "~=", Value: 1},
	)
	_, err := wf.Build()
	if err == nil {
		t.Fatal("expected validation error for invalid op, got nil")
	}
}

func TestValidateSkipIfNonParent(t *testing.T) {
	wf := NewWorkflow("bad-ref")
	wf.Task("a", "task-a")
	wf.Task("b", "task-b").SkipIf(
		&ParentCond{StepID: "a", Field: "x", Op: "==", Value: 1},
	)
	_, err := wf.Build()
	if err == nil {
		t.Fatal("expected validation error for non-parent SkipIf ref")
	}
}

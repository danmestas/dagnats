// dag/condition_test.go

// Tests for ParentCond evaluation: numeric, string, and bool
// comparisons, missing fields, nil output, and all six operators.
// Methodology: construct step states with known output, evaluate
// conditions, assert both positive and negative results.
package dag

import "testing"

func makeSteps(
	stepID string, output string,
) map[string]StepState {
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
		cond := &ParentCond{
			StepID: "s1", Field: "count",
			Op: tt.op, Value: tt.val,
		}
		if got := cond.Evaluate(steps); got != tt.want {
			t.Errorf("%s %v = %v, want %v",
				tt.op, tt.val, got, tt.want)
		}
	}
}

func TestParentCondStringComparison(t *testing.T) {
	steps := makeSteps("s1", `{"name": "beta"}`)
	cond := &ParentCond{
		StepID: "s1", Field: "name", Op: "==", Value: "beta",
	}
	if !cond.Evaluate(steps) {
		t.Fatal("expected name == beta to be true")
	}
	cond.Op = "<"
	cond.Value = "gamma"
	if !cond.Evaluate(steps) {
		t.Fatal("expected name < gamma to be true")
	}
}

func TestParentCondBoolComparison(t *testing.T) {
	steps := makeSteps("s1", `{"done": true}`)
	cond := &ParentCond{
		StepID: "s1", Field: "done", Op: "==", Value: true,
	}
	if !cond.Evaluate(steps) {
		t.Fatal("expected done == true")
	}
	cond.Op = "!="
	cond.Value = false
	if !cond.Evaluate(steps) {
		t.Fatal("expected done != false")
	}
}

func TestParentCondMissingField(t *testing.T) {
	steps := makeSteps("s1", `{"count": 5}`)
	cond := &ParentCond{
		StepID: "s1", Field: "missing", Op: "==", Value: float64(5),
	}
	if cond.Evaluate(steps) {
		t.Fatal("expected missing field to return false")
	}
}

func TestParentCondNilOutput(t *testing.T) {
	steps := map[string]StepState{
		"s1": {Status: StepStatusCompleted, Output: nil},
	}
	cond := &ParentCond{
		StepID: "s1", Field: "count", Op: "==", Value: float64(5),
	}
	if cond.Evaluate(steps) {
		t.Fatal("expected nil output to return false")
	}
}

func TestParentCondMissingStep(t *testing.T) {
	cond := &ParentCond{
		StepID: "s1", Field: "count", Op: "==", Value: float64(5),
	}
	if cond.Evaluate(map[string]StepState{}) {
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
	cond := &ParentCond{
		StepID: "s1", Field: "count", Op: "==", Value: "five",
	}
	if cond.Evaluate(steps) {
		t.Fatal("expected type mismatch to return false")
	}
}

func TestSkipIfOutputPanicsOnEmptyField(t *testing.T) {
	wf := NewWorkflow("test")
	a := wf.Task("a", "task-a")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty field")
		}
	}()
	SkipIfOutput(a, "", "==", 1)
}

func TestSkipIfOutputPanicsOnInvalidOp(t *testing.T) {
	wf := NewWorkflow("test")
	a := wf.Task("a", "task-a")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid op")
		}
	}()
	SkipIfOutput(a, "x", "~=", 1)
}

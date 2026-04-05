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
		val  any
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

func TestCompareValuesStringAllOps(t *testing.T) {
	tests := []struct {
		a, b string
		op   string
		want bool
	}{
		{"alpha", "alpha", "==", true},
		{"alpha", "beta", "==", false},
		{"alpha", "beta", "!=", true},
		{"alpha", "alpha", "!=", false},
		{"alpha", "beta", "<", true},
		{"beta", "alpha", "<", false},
		{"beta", "alpha", ">", true},
		{"alpha", "beta", ">", false},
		{"alpha", "alpha", "<=", true},
		{"alpha", "beta", "<=", true},
		{"beta", "alpha", "<=", false},
		{"beta", "beta", ">=", true},
		{"beta", "alpha", ">=", true},
		{"alpha", "beta", ">=", false},
	}
	for _, tt := range tests {
		got := compareValues(tt.a, tt.op, tt.b)
		if got != tt.want {
			t.Errorf("%q %s %q = %v, want %v",
				tt.a, tt.op, tt.b, got, tt.want)
		}
	}
}

func TestCompareValuesInvalidOp(t *testing.T) {
	// Positive: unknown op returns false for strings
	if compareValues("a", "~=", "b") {
		t.Fatal("unknown op should return false for strings")
	}
	// Negative: unknown op returns false for numbers
	if compareValues(float64(1), "~=", float64(2)) {
		t.Fatal("unknown op should return false for numbers")
	}
}

func TestCompareValuesBoolNotComparable(t *testing.T) {
	// Positive: bool == works
	if !compareValues(true, "==", true) {
		t.Fatal("true == true should be true")
	}
	// Negative: bool < not supported, returns false
	if compareValues(true, "<", false) {
		t.Fatal("bool < should return false")
	}
}

func TestToFloat64IntTypes(t *testing.T) {
	// Positive: int converts
	v, ok := toFloat64(int(42))
	if !ok || v != 42.0 {
		t.Fatalf("int: got %v, %v", v, ok)
	}
	// Positive: int64 converts
	v, ok = toFloat64(int64(99))
	if !ok || v != 99.0 {
		t.Fatalf("int64: got %v, %v", v, ok)
	}
	// Negative: string does not convert
	_, ok = toFloat64("nope")
	if ok {
		t.Fatal("string should not convert to float64")
	}
}

func TestCompareValuesMixedTypes(t *testing.T) {
	// Positive: mismatched types (num vs bool) returns false
	if compareValues(float64(1), "==", true) {
		t.Fatal("num vs bool should return false")
	}
	// Negative: mismatched types (string vs num) returns false
	if compareValues("hello", "==", float64(5)) {
		t.Fatal("string vs num should return false")
	}
}

func TestSkipIfOutputConstructor(t *testing.T) {
	wf := NewWorkflow("skip-ctor")
	parent := wf.Task("a", "task-a")
	cond := SkipIfOutput(parent, "ready", "==", true)

	// Positive: StepID matches parent
	if cond.StepID != "a" {
		t.Fatalf("StepID = %q, want %q", cond.StepID, "a")
	}
	// Positive: fields set correctly
	if cond.Field != "ready" || cond.Op != "==" {
		t.Fatalf("Field=%q Op=%q, want ready ==",
			cond.Field, cond.Op)
	}
}

func TestParentCondInvalidJSON(t *testing.T) {
	steps := map[string]StepState{
		"s1": {
			Status: StepStatusCompleted,
			Output: []byte(`{not json`),
		},
	}
	cond := &ParentCond{
		StepID: "s1", Field: "x", Op: "==", Value: float64(1),
	}
	// Positive: invalid JSON output returns false
	if cond.Evaluate(steps) {
		t.Fatal("invalid JSON should return false")
	}
	// Negative: valid JSON works
	steps["s1"] = StepState{
		Status: StepStatusCompleted,
		Output: []byte(`{"x":1}`),
	}
	if !cond.Evaluate(steps) {
		t.Fatal("valid JSON should evaluate correctly")
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

// Tests for DAG resolution: given a set of completed steps, determine which
// steps are ready to execute. Also tests input resolution for single-dep,
// fan-in, and no-dep cases.
// Methodology: build DAGs of varying shapes, mark subsets as completed, and
// verify exactly which steps become ready. Check both inclusion and exclusion.
package dag

import (
	"encoding/json"
	"sort"
	"testing"
)

func TestResolveReadyFirstSteps(t *testing.T) {
	def := WorkflowDef{Name: "test", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "t-a", Type: StepTypeNormal},
		{ID: "b", Task: "t-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
	}}
	ready := ResolveReady(def, map[string]bool{}, map[string]bool{})
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready step, got %d", len(ready))
	}
	if ready[0].ID != "a" {
		t.Fatalf("expected step 'a', got %q", ready[0].ID)
	}
}

func TestResolveReadyAfterCompletion(t *testing.T) {
	def := WorkflowDef{Name: "test", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "t-a", Type: StepTypeNormal},
		{ID: "b", Task: "t-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
		{ID: "c", Task: "t-c", DependsOn: []string{"a"}, Type: StepTypeNormal},
	}}
	completed := map[string]bool{"a": true}
	ready := ResolveReady(def, completed, map[string]bool{})
	ids := readyIDs(ready)
	sort.Strings(ids)
	if len(ids) != 2 || ids[0] != "b" || ids[1] != "c" {
		t.Fatalf("expected [b, c], got %v", ids)
	}
	for _, s := range ready {
		if completed[s.ID] {
			t.Fatalf("completed step %q appeared in ready list", s.ID)
		}
	}
}

func TestResolveReadyFanIn(t *testing.T) {
	def := WorkflowDef{Name: "test", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "t-a", Type: StepTypeNormal},
		{ID: "b", Task: "t-b", Type: StepTypeNormal},
		{ID: "c", Task: "t-c", DependsOn: []string{"a", "b"}, Type: StepTypeNormal},
	}}
	ready := ResolveReady(def, map[string]bool{"a": true}, map[string]bool{})
	for _, s := range ready {
		if s.ID == "c" {
			t.Fatal("step 'c' should not be ready — 'b' not completed")
		}
	}
	ready = ResolveReady(def, map[string]bool{"a": true, "b": true}, map[string]bool{})
	if len(ready) != 1 || ready[0].ID != "c" {
		t.Fatalf("expected [c], got %v", readyIDs(ready))
	}
}

func TestResolveReadySkipsQueued(t *testing.T) {
	def := WorkflowDef{Name: "test", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "t-a", Type: StepTypeNormal},
	}}
	ready := ResolveReady(def, map[string]bool{}, map[string]bool{"a": true})
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready steps (already queued), got %d", len(ready))
	}
}

func TestResolveReadyAllCompleted(t *testing.T) {
	def := WorkflowDef{Name: "test", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "t-a", Type: StepTypeNormal},
		{ID: "b", Task: "t-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
	}}
	ready := ResolveReady(def, map[string]bool{"a": true, "b": true}, map[string]bool{})
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready steps (all completed), got %d", len(ready))
	}
}

func TestResolveInputSingleDep(t *testing.T) {
	step := StepDef{ID: "b", DependsOn: []string{"a"}}
	steps := map[string]StepState{"a": {Status: StepStatusCompleted, Output: []byte(`"a-out"`)}}
	input, err := ResolveInput(step, steps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(input) != `"a-out"` {
		t.Fatalf("input = %q, want %q", string(input), `"a-out"`)
	}
}

func TestResolveInputFanIn(t *testing.T) {
	step := StepDef{ID: "c", DependsOn: []string{"a", "b"}}
	steps := map[string]StepState{
		"a": {Status: StepStatusCompleted, Output: []byte(`"a-out"`)},
		"b": {Status: StepStatusCompleted, Output: []byte(`"b-out"`)},
	}
	input, err := ResolveInput(step, steps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var result map[string]json.RawMessage
	err = json.Unmarshal(input, &result)
	if err != nil {
		t.Fatalf("fan-in input is not valid JSON map: %v", err)
	}
	if string(result["a"]) != `"a-out"` {
		t.Fatalf("result[a] = %q, want %q", string(result["a"]), `"a-out"`)
	}
	if string(result["b"]) != `"b-out"` {
		t.Fatalf("result[b] = %q, want %q", string(result["b"]), `"b-out"`)
	}
}

func TestResolveInputNoDeps(t *testing.T) {
	step := StepDef{ID: "a", DependsOn: nil}
	input, err := ResolveInput(step, map[string]StepState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if input != nil {
		t.Fatalf("input = %q, want nil for step with no deps", string(input))
	}
}

func readyIDs(steps []StepDef) []string {
	ids := make([]string, len(steps))
	for i, s := range steps {
		ids[i] = s.ID
	}
	return ids
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
		"a": {
			Status: StepStatusCompleted,
			Output: []byte(`{"count": 5}`),
		},
		"b": {Status: StepStatusPending},
	}
	completed := map[string]bool{"a": true}
	skipped := ResolveSkipped(def, completed, map[string]bool{}, steps)
	if len(skipped) != 1 || skipped[0].ID != "b" {
		t.Fatalf("expected [b] skipped, got %v", readyIDs(skipped))
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
		"a": {
			Status: StepStatusCompleted,
			Output: []byte(`{"count": 50}`),
		},
		"b": {Status: StepStatusPending},
	}
	completed := map[string]bool{"a": true}
	skipped := ResolveSkipped(def, completed, map[string]bool{}, steps)
	if len(skipped) != 0 {
		t.Fatalf("expected 0 skipped, got %d", len(skipped))
	}
}

func TestResolveSkippedCascade(t *testing.T) {
	// A -> B(skip if A.x<10) -> C(skip if B.x<10)
	// When A outputs x=5, B should be skipped. Then C's deps are
	// satisfied (B is skipped), but B has no output, so C's SkipIf
	// evaluates false (missing field). C should become ready, not
	// skipped. This tests that the orchestrator's cascade loop works.
	wf := NewWorkflow("cascade")
	a := wf.Task("a", "task-a")
	b := wf.Task("b", "task-b").After(a).SkipIf(
		SkipIfOutput(a, "x", "<", float64(10)),
	)
	wf.Task("c", "task-c").After(b).SkipIf(
		SkipIfOutput(b, "x", "<", float64(10)),
	)
	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	steps := map[string]StepState{
		"a": {
			Status: StepStatusCompleted,
			Output: []byte(`{"x": 5}`),
		},
		"b": {Status: StepStatusPending},
		"c": {Status: StepStatusPending},
	}
	completed := map[string]bool{"a": true}
	queued := map[string]bool{}

	// First round: B gets skipped.
	skipped := ResolveSkipped(def, completed, queued, steps)
	if len(skipped) != 1 || skipped[0].ID != "b" {
		t.Fatalf("round 1: expected [b], got %v", readyIDs(skipped))
	}

	// Mark B as skipped, then round 2: C is NOT skipped (B has no
	// output so the condition evaluates false).
	steps["b"] = StepState{Status: StepStatusSkipped}
	completed["b"] = true
	skipped = ResolveSkipped(def, completed, queued, steps)
	if len(skipped) != 0 {
		t.Fatalf("round 2: expected 0 skipped, got %v",
			readyIDs(skipped))
	}

	// C should be ready now.
	ready := ResolveReady(def, completed, queued)
	if len(ready) != 1 || ready[0].ID != "c" {
		t.Fatalf("expected [c] ready, got %v", readyIDs(ready))
	}
}

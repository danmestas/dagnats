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

func TestIsCompleteAllDone(t *testing.T) {
	def := WorkflowDef{Name: "test", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "t-a", Type: StepTypeNormal},
		{ID: "b", Task: "t-b", DependsOn: []string{"a"}, Type: StepTypeNormal},
	}}
	// Positive: all steps completed
	if !IsComplete(def, map[string]bool{"a": true, "b": true}) {
		t.Fatal("expected IsComplete=true when all steps done")
	}
	// Negative: one step missing
	if IsComplete(def, map[string]bool{"a": true}) {
		t.Fatal("expected IsComplete=false when step b not done")
	}
}

func TestIsCompleteEmpty(t *testing.T) {
	def := WorkflowDef{Name: "empty", Version: "1", Steps: []StepDef{}}
	// Positive: no steps means trivially complete
	if !IsComplete(def, map[string]bool{}) {
		t.Fatal("expected IsComplete=true for empty workflow")
	}
	// Negative: non-empty map doesn't break empty def
	if !IsComplete(def, map[string]bool{"x": true}) {
		t.Fatal("expected IsComplete=true for empty def with extra keys")
	}
}

func TestAllDepsCompletedEmpty(t *testing.T) {
	// Positive: no deps means always satisfied
	if !allDepsCompleted(nil, map[string]bool{}) {
		t.Fatal("expected nil deps to be satisfied")
	}
	// Negative: non-empty deps with empty completed
	if allDepsCompleted([]string{"a"}, map[string]bool{}) {
		t.Fatal("expected unsatisfied dep to return false")
	}
}

func TestResolveSkippedNoSkipIf(t *testing.T) {
	def := WorkflowDef{Name: "test", Version: "1", Steps: []StepDef{
		{ID: "a", Task: "t-a", Type: StepTypeNormal},
		{ID: "b", Task: "t-b", DependsOn: []string{"a"},
			Type: StepTypeNormal},
	}}
	steps := map[string]StepState{
		"a": {Status: StepStatusCompleted, Output: []byte(`{}`)},
	}
	completed := map[string]bool{"a": true}
	// Positive: no steps returned when no SkipIf conditions
	skipped := ResolveSkipped(def, completed, map[string]bool{}, steps)
	if len(skipped) != 0 {
		t.Fatalf("expected 0 skipped, got %d", len(skipped))
	}
	// Negative: already-completed steps excluded
	skipped = ResolveSkipped(
		def, map[string]bool{"a": true, "b": true},
		map[string]bool{}, steps,
	)
	if len(skipped) != 0 {
		t.Fatalf("expected 0 skipped for completed steps, got %d",
			len(skipped))
	}
}

func TestResolveSkippedDepsNotMet(t *testing.T) {
	wf := NewWorkflow("skip-deps")
	a := wf.Task("a", "task-a")
	wf.Task("b", "task-b").After(a).SkipIf(
		SkipIfOutput(a, "x", "==", float64(1)),
	)
	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	steps := map[string]StepState{
		"a": {Status: StepStatusPending},
	}
	// Positive: deps not met so nothing skipped
	skipped := ResolveSkipped(
		def, map[string]bool{}, map[string]bool{}, steps,
	)
	if len(skipped) != 0 {
		t.Fatalf("expected 0 skipped when deps unmet, got %d",
			len(skipped))
	}
	// Negative: queued step excluded
	skipped = ResolveSkipped(
		def, map[string]bool{"a": true},
		map[string]bool{"b": true}, steps,
	)
	if len(skipped) != 0 {
		t.Fatalf("expected 0 skipped when queued, got %d",
			len(skipped))
	}
}

func readyIDs(steps []StepDef) []string {
	ids := make([]string, len(steps))
	for i, s := range steps {
		ids[i] = s.ID
	}
	return ids
}

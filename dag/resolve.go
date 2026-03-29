package dag

import "encoding/json"

// ResolveReady returns all steps whose dependencies are fully completed and
// that have not yet been queued or completed. Both completed and queued are
// checked to avoid double-dispatching a step that is already in flight.
func ResolveReady(def WorkflowDef, completed map[string]bool, queued map[string]bool) []StepDef {
	ready := make([]StepDef, 0, len(def.Steps))
	for _, step := range def.Steps {
		if completed[step.ID] || queued[step.ID] {
			continue
		}
		if allDepsCompleted(step.DependsOn, completed) {
			ready = append(ready, step)
		}
	}
	return ready
}

// ResolveInput builds the input payload for a step from its upstream outputs.
// No deps → nil (first step receives workflow-level input from the caller).
// Single dep → pass that step's output through unchanged.
// Fan-in → map of dep ID → raw output, so the task can address each upstream.
func ResolveInput(step StepDef, steps map[string]StepState) ([]byte, error) {
	if len(step.DependsOn) == 0 {
		return nil, nil
	}
	if len(step.DependsOn) == 1 {
		return steps[step.DependsOn[0]].Output, nil
	}
	collected := make(map[string]json.RawMessage, len(step.DependsOn))
	for _, dep := range step.DependsOn {
		collected[dep] = steps[dep].Output
	}
	data, err := json.Marshal(collected)
	return data, err
}

// IsComplete returns true when every step in the definition has been completed.
// Used by the engine to decide whether to transition the run to RunStatusCompleted.
func IsComplete(def WorkflowDef, completed map[string]bool) bool {
	for _, step := range def.Steps {
		if !completed[step.ID] {
			return false
		}
	}
	return true
}

// allDepsCompleted is an internal predicate: true iff every dep ID appears in
// the completed set. An empty deps slice is always satisfied (entry-point steps).
func allDepsCompleted(deps []string, completed map[string]bool) bool {
	for _, dep := range deps {
		if !completed[dep] {
			return false
		}
	}
	return true
}

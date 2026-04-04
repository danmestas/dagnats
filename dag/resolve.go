package dag

import "encoding/json"

// ResolveReady returns steps whose dependencies are fully satisfied (completed
// or skipped) and that have not yet been queued or completed. Both completed
// and queued are checked to avoid double-dispatching a step already in flight.
func ResolveReady(
	def WorkflowDef,
	completed map[string]bool,
	queued map[string]bool,
) []StepDef {
	ready := make([]StepDef, 0, len(def.Steps))
	for _, step := range def.Steps {
		if completed[step.ID] || queued[step.ID] {
			continue
		}
		// Auxiliary steps (OnFailure/Compensate targets) are never
		// resolved through normal dependency resolution. They are
		// dispatched directly by the engine when their trigger fires.
		if def.AuxSteps[step.ID] {
			continue
		}
		if allDepsCompleted(step.DependsOn, completed) {
			ready = append(ready, step)
		}
	}
	return ready
}

// ResolveSkipped returns steps whose dependencies are satisfied AND whose
// SkipIf condition evaluates to true. These should be marked Skipped by the
// orchestrator instead of being enqueued. Steps without SkipIf are never
// returned here.
func ResolveSkipped(
	def WorkflowDef,
	completed map[string]bool,
	queued map[string]bool,
	steps map[string]StepState,
) []StepDef {
	skipped := make([]StepDef, 0)
	for _, step := range def.Steps {
		if completed[step.ID] || queued[step.ID] {
			continue
		}
		if step.SkipIf == nil {
			continue
		}
		if allDepsCompleted(step.DependsOn, completed) {
			if step.SkipIf.Evaluate(steps) {
				skipped = append(skipped, step)
			}
		}
	}
	return skipped
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

// IsComplete returns true when every step in the definition has been
// completed or skipped. Auxiliary steps (OnFailure/Compensate targets)
// that were never triggered don't block completion — they are expected
// to remain Pending in the happy path.
func IsComplete(
	def WorkflowDef, completed map[string]bool,
) bool {
	for _, step := range def.Steps {
		if def.AuxSteps[step.ID] && !completed[step.ID] {
			continue
		}
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

// ResolveCompensateChain returns compensate steps for completed steps
// in reverse topological order. Each step (except the first) gets a
// DependsOn pointing to the previous — this lets the engine enforce
// sequential execution using existing resolution logic.
func ResolveCompensateChain(
	def WorkflowDef,
	completed map[string]bool,
	failedStepID string,
) []StepDef {
	// Collect completed steps with Compensate in definition order
	// (def.Steps is topo-sorted from Build).
	byID := make(map[string]StepDef, len(def.Steps))
	for _, s := range def.Steps {
		byID[s.ID] = s
	}
	var ordered []StepDef
	for _, step := range def.Steps {
		if step.Compensate == "" {
			continue
		}
		if !completed[step.ID] {
			continue
		}
		comp := byID[step.Compensate]
		ordered = append(ordered, comp)
	}
	if len(ordered) == 0 {
		return nil
	}

	// Reverse for compensation order (last completed first)
	for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
		ordered[i], ordered[j] = ordered[j], ordered[i]
	}

	// Wire DependsOn chain for sequential execution
	chain := make([]StepDef, len(ordered))
	for i, step := range ordered {
		chain[i] = step
		chain[i].DependsOn = nil
		if i > 0 {
			chain[i].DependsOn = []string{chain[i-1].ID}
		}
	}
	return chain
}

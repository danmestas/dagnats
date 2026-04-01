package dag

import "fmt"

// Validate checks a WorkflowDef for structural correctness before any run
// is created. Catching these errors at definition time defines them out of
// existence at runtime — the engine can safely assume every WorkflowDef it
// receives has already passed Validate.
func Validate(def WorkflowDef) error {
	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow %q has no steps", def.Name)
	}

	ids, err := validateStepIDs(def)
	if err != nil {
		return err
	}

	if err := validateStepReferences(def, ids); err != nil {
		return err
	}

	return detectCycle(def.Steps)
}

// validateStepIDs ensures every step has a unique ID.
// Returns the set of known IDs for downstream reference checks.
func validateStepIDs(def WorkflowDef) (map[string]bool, error) {
	if len(def.Steps) == 0 {
		panic("validateStepIDs: called with empty steps")
	}
	if def.Name == "" {
		panic("validateStepIDs: workflow name is empty")
	}

	ids := make(map[string]bool, len(def.Steps))
	for _, step := range def.Steps {
		if ids[step.ID] {
			return nil, fmt.Errorf("duplicate step ID %q", step.ID)
		}
		ids[step.ID] = true
	}
	return ids, nil
}

// validateStepReferences checks each step's dependencies, loop config,
// SkipIf conditions, OnFailure, and Compensate references against known IDs.
func validateStepReferences(
	def WorkflowDef,
	ids map[string]bool,
) error {
	if ids == nil {
		panic("validateStepReferences: ids map is nil")
	}
	if len(def.Steps) == 0 {
		panic("validateStepReferences: called with empty steps")
	}

	for _, step := range def.Steps {
		if err := validateSingleStep(step, ids); err != nil {
			return err
		}
	}
	return nil
}

// validateSingleStep validates one step's fields against the known ID set.
func validateSingleStep(step StepDef, ids map[string]bool) error {
	if ids == nil {
		panic("validateSingleStep: ids map is nil")
	}
	if step.ID == "" {
		panic("validateSingleStep: step ID is empty")
	}

	if step.Retries < 0 {
		return fmt.Errorf(
			"step %q has negative Retries (%d)", step.ID, step.Retries,
		)
	}
	for _, dep := range step.DependsOn {
		if !ids[dep] {
			return fmt.Errorf(
				"step %q depends on %q which does not exist",
				step.ID, dep,
			)
		}
	}
	if err := validateLoopConfig(step); err != nil {
		return err
	}
	if err := validateSkipIf(step); err != nil {
		return err
	}
	if step.OnFailure != "" && !ids[step.OnFailure] {
		return fmt.Errorf(
			"step %q OnFailure references %q which does not exist",
			step.ID, step.OnFailure,
		)
	}
	if step.Compensate != "" && !ids[step.Compensate] {
		return fmt.Errorf(
			"step %q Compensate references %q which does not exist",
			step.ID, step.Compensate,
		)
	}
	return nil
}

// validateLoopConfig checks AgentLoop/Loop consistency for a single step.
func validateLoopConfig(step StepDef) error {
	if step.ID == "" {
		panic("validateLoopConfig: step ID is empty")
	}
	if step.Task == "" {
		panic("validateLoopConfig: step task is empty")
	}
	if step.Type == StepTypeAgentLoop && step.Loop == nil {
		return fmt.Errorf(
			"step %q is AgentLoop but Loop config is nil", step.ID,
		)
	}
	if step.Type == StepTypeAgentLoop &&
		step.Loop != nil && step.Loop.MaxIterations <= 0 {
		return fmt.Errorf(
			"step %q is AgentLoop but MaxIterations is %d (must be > 0)",
			step.ID, step.Loop.MaxIterations,
		)
	}
	if step.Type != StepTypeAgentLoop && step.Loop != nil {
		return fmt.Errorf(
			"step %q has Loop config but is not AgentLoop type", step.ID,
		)
	}
	return nil
}

// validateSkipIf checks SkipIf conditions reference valid operators and
// parent steps. SkipIf.StepID must be in DependsOn — you can only skip
// based on a parent step's output.
func validateSkipIf(step StepDef) error {
	if step.ID == "" {
		panic("validateSkipIf: step ID is empty")
	}
	if step.Task == "" {
		panic("validateSkipIf: step task is empty")
	}
	if step.SkipIf == nil {
		return nil
	}
	if !validOps[step.SkipIf.Op] {
		return fmt.Errorf(
			"step %q SkipIf has invalid operator %q",
			step.ID, step.SkipIf.Op,
		)
	}
	if step.SkipIf.StepID == "" {
		return fmt.Errorf(
			"step %q SkipIf has empty StepID", step.ID,
		)
	}
	found := false
	for _, dep := range step.DependsOn {
		if dep == step.SkipIf.StepID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf(
			"step %q SkipIf references %q which is not in DependsOn",
			step.ID, step.SkipIf.StepID,
		)
	}
	return nil
}

// detectCycle uses iterative Kahn's algorithm (no recursion per TigerStyle).
// A cycle is proven when Kahn's BFS cannot reach all nodes — any remaining
// nodes with non-zero in-degree are part of a cycle.
func detectCycle(steps []StepDef) error {
	inDegree := make(map[string]int, len(steps))
	dependents := make(map[string][]string, len(steps))
	for _, s := range steps {
		if _, ok := inDegree[s.ID]; !ok {
			inDegree[s.ID] = 0
		}
		for _, dep := range s.DependsOn {
			inDegree[s.ID]++
			dependents[dep] = append(dependents[dep], s.ID)
		}
	}
	queue := make([]string, 0, len(steps))
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for _, dep := range dependents[node] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}
	if visited != len(steps) {
		return fmt.Errorf(
			"workflow contains a cycle (%d of %d steps reachable)",
			visited, len(steps),
		)
	}
	return nil
}

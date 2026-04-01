package dag

import "fmt"

// Validate checks a WorkflowDef for structural correctness before any run is created.
// Catching these errors at definition time defines them out of existence at runtime —
// the engine can safely assume every WorkflowDef it receives has already passed Validate.
func Validate(def WorkflowDef) error {
	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow %q has no steps", def.Name)
	}
	ids := make(map[string]bool, len(def.Steps))
	for _, s := range def.Steps {
		if ids[s.ID] {
			return fmt.Errorf("duplicate step ID %q", s.ID)
		}
		ids[s.ID] = true
	}
	for _, s := range def.Steps {
		if s.Retries < 0 {
			return fmt.Errorf("step %q has negative Retries (%d)", s.ID, s.Retries)
		}
		for _, dep := range s.DependsOn {
			if !ids[dep] {
				return fmt.Errorf("step %q depends on %q which does not exist", s.ID, dep)
			}
		}
		if s.Type == StepTypeAgentLoop && s.Loop == nil {
			return fmt.Errorf("step %q is AgentLoop but Loop config is nil", s.ID)
		}
		if s.Type == StepTypeAgentLoop && s.Loop != nil && s.Loop.MaxIterations <= 0 {
			return fmt.Errorf(
				"step %q is AgentLoop but MaxIterations is %d (must be > 0)",
				s.ID, s.Loop.MaxIterations,
			)
		}
		if s.Type != StepTypeAgentLoop && s.Loop != nil {
			return fmt.Errorf("step %q has Loop config but is not AgentLoop type", s.ID)
		}
		if s.SkipIf != nil {
			if !validOps[s.SkipIf.Op] {
				return fmt.Errorf(
					"step %q SkipIf has invalid operator %q",
					s.ID, s.SkipIf.Op,
				)
			}
			if s.SkipIf.StepID == "" {
				return fmt.Errorf(
					"step %q SkipIf has empty StepID", s.ID,
				)
			}
			// SkipIf.StepID must be in DependsOn — you can only skip
			// based on a parent step's output.
			found := false
			for _, dep := range s.DependsOn {
				if dep == s.SkipIf.StepID {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf(
					"step %q SkipIf references %q which is not in DependsOn",
					s.ID, s.SkipIf.StepID,
				)
			}
		}
		if s.OnFailure != "" && !ids[s.OnFailure] {
			return fmt.Errorf(
				"step %q OnFailure references %q which does not exist",
				s.ID, s.OnFailure)
		}
		if s.Compensate != "" && !ids[s.Compensate] {
			return fmt.Errorf(
				"step %q Compensate references %q which does not exist",
				s.ID, s.Compensate)
		}
	}
	return detectCycle(def.Steps)
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

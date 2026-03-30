package dag

import "fmt"

// Validate checks a WorkflowDef for structural correctness before any
// run is created. Catching errors at definition time defines them out
// of existence at runtime.
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
		if err := validateStep(s, ids); err != nil {
			return err
		}
	}
	return detectCycle(def.Steps)
}

func validateStep(s StepDef, ids map[string]bool) error {
	if s.Retries < 0 {
		return fmt.Errorf(
			"step %q has negative Retries (%d)", s.ID, s.Retries,
		)
	}
	for _, dep := range s.DependsOn {
		if !ids[dep] {
			return fmt.Errorf(
				"step %q depends on %q which does not exist",
				s.ID, dep,
			)
		}
	}
	if s.Type == StepTypeAgentLoop && s.Loop == nil {
		return fmt.Errorf(
			"step %q is AgentLoop but Loop config is nil", s.ID,
		)
	}
	if s.Type == StepTypeAgentLoop && s.Loop != nil &&
		s.Loop.MaxIterations <= 0 {
		return fmt.Errorf(
			"step %q AgentLoop MaxIterations must be > 0 (got %d)",
			s.ID, s.Loop.MaxIterations,
		)
	}
	if s.Type != StepTypeAgentLoop && s.Loop != nil {
		return fmt.Errorf(
			"step %q has Loop config but is not AgentLoop", s.ID,
		)
	}
	if s.SkipIf != nil {
		return validateSkipIf(s)
	}
	return nil
}

func validateSkipIf(s StepDef) error {
	c := s.SkipIf
	if c.StepID == "" {
		return fmt.Errorf("step %q SkipIf has empty StepID", s.ID)
	}
	if c.Field == "" {
		return fmt.Errorf("step %q SkipIf has empty Field", s.ID)
	}
	if !validOps[c.Op] {
		return fmt.Errorf(
			"step %q SkipIf has invalid operator %q", s.ID, c.Op,
		)
	}
	found := false
	for _, dep := range s.DependsOn {
		if dep == c.StepID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf(
			"step %q SkipIf references %q which is not in DependsOn",
			s.ID, c.StepID,
		)
	}
	return nil
}

// detectCycle uses iterative Kahn's algorithm (no recursion per
// TigerStyle). A cycle is proven when Kahn's BFS cannot reach all
// nodes.
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

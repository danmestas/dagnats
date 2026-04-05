package dag

import "fmt"

// EffectiveSteps returns the combined static + dynamic steps for a
// running workflow. When no dynamic steps exist, the original slice
// is returned unchanged to avoid allocation.
func EffectiveSteps(
	def WorkflowDef, run WorkflowRun,
) []StepDef {
	if len(run.DynamicSteps) == 0 {
		return def.Steps
	}
	capacity := len(def.Steps) + len(run.DynamicSteps)
	all := make([]StepDef, 0, capacity)
	all = append(all, def.Steps...)
	all = append(all, run.DynamicSteps...)
	return all
}

// EffectiveDef returns a WorkflowDef augmented with dynamic steps
// from the run. The original def is not mutated — a shallow copy
// is returned with the combined step list and rebuilt AuxSteps.
func EffectiveDef(
	def WorkflowDef, run WorkflowRun,
) WorkflowDef {
	if len(def.Steps) == 0 {
		panic("EffectiveDef: def must have at least one step")
	}
	if run.RunID == "" {
		panic("EffectiveDef: run.RunID must not be empty")
	}
	if len(run.DynamicSteps) == 0 {
		return def
	}
	augmented := def
	augmented.Steps = EffectiveSteps(def, run)
	augmented.AuxSteps = buildAuxSteps(augmented.Steps)
	return augmented
}

// ValidateFragment checks a planner-generated DAG fragment against
// bounds. All IDs must be unique and not collide with existing steps.
// Tasks must be non-empty and in AllowedTasks if configured.
// Dependencies must reference only within-fragment steps.
func ValidateFragment(
	fragment []StepDef,
	cfg PlannerConfig,
	existingIDs map[string]bool,
) error {
	if existingIDs == nil {
		panic("ValidateFragment: existingIDs must not be nil")
	}
	if cfg.MaxSteps <= 0 {
		panic("ValidateFragment: MaxSteps must be positive")
	}
	if len(fragment) == 0 {
		return fmt.Errorf("planner fragment is empty")
	}
	if len(fragment) > cfg.MaxSteps {
		return fmt.Errorf(
			"planner fragment has %d steps (max %d)",
			len(fragment), cfg.MaxSteps,
		)
	}
	if err := validateFragmentIDs(
		fragment, existingIDs,
	); err != nil {
		return err
	}
	if err := validateFragmentTasks(
		fragment, cfg.AllowedTasks,
	); err != nil {
		return err
	}
	if err := validateFragmentDeps(fragment); err != nil {
		return err
	}
	if err := detectCycle(fragment); err != nil {
		return fmt.Errorf("planner fragment: %w", err)
	}
	if cfg.MaxDepth > 0 {
		depth := maxChainDepth(fragment)
		if depth > cfg.MaxDepth {
			return fmt.Errorf(
				"planner fragment depth is %d (max %d)",
				depth, cfg.MaxDepth,
			)
		}
	}
	return nil
}

// validateFragmentIDs checks for duplicate and colliding step IDs.
func validateFragmentIDs(
	fragment []StepDef, existingIDs map[string]bool,
) error {
	if existingIDs == nil {
		panic("validateFragmentIDs: existingIDs must not be nil")
	}
	if len(fragment) == 0 {
		panic("validateFragmentIDs: fragment must not be empty")
	}
	seen := make(map[string]bool, len(fragment))
	for _, step := range fragment {
		if step.ID == "" {
			return fmt.Errorf("planner fragment step has empty ID")
		}
		if seen[step.ID] {
			return fmt.Errorf(
				"planner fragment has duplicate ID %q", step.ID,
			)
		}
		if existingIDs[step.ID] {
			return fmt.Errorf(
				"planner fragment ID %q collides with existing step",
				step.ID,
			)
		}
		seen[step.ID] = true
	}
	return nil
}

// validateFragmentTasks checks that all tasks are non-empty and
// within the allowed set if configured.
func validateFragmentTasks(
	fragment []StepDef, allowedTasks []string,
) error {
	if len(fragment) == 0 {
		panic("validateFragmentTasks: fragment must not be empty")
	}
	allowed := make(map[string]bool, len(allowedTasks))
	for _, task := range allowedTasks {
		allowed[task] = true
	}
	for _, step := range fragment {
		if step.Task == "" {
			return fmt.Errorf(
				"planner fragment step %q has empty task",
				step.ID,
			)
		}
		if len(allowedTasks) > 0 && !allowed[step.Task] {
			return fmt.Errorf(
				"planner fragment step %q uses disallowed "+
					"task %q",
				step.ID, step.Task,
			)
		}
	}
	return nil
}

// validateFragmentDeps checks that all dependencies reference steps
// within the fragment — no references to external steps allowed.
func validateFragmentDeps(fragment []StepDef) error {
	if len(fragment) == 0 {
		panic("validateFragmentDeps: fragment must not be empty")
	}
	ids := make(map[string]bool, len(fragment))
	for _, step := range fragment {
		ids[step.ID] = true
	}
	for _, step := range fragment {
		for _, dep := range step.DependsOn {
			if !ids[dep] {
				return fmt.Errorf(
					"planner fragment step %q depends on "+
						"%q which is not in the fragment",
					step.ID, dep,
				)
			}
		}
	}
	return nil
}

// maxChainDepth computes the longest dependency chain in a set of
// steps using iterative topological BFS (Kahn's). Returns 1 for a
// single step with no dependencies. Panics on empty input.
func maxChainDepth(steps []StepDef) int {
	if len(steps) == 0 {
		panic("maxChainDepth: steps must not be empty")
	}
	if len(steps) > 10000 {
		panic("maxChainDepth: steps exceeds upper bound")
	}
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
	depth := make(map[string]int, len(steps))
	queue := make([]string, 0, len(steps))
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
			depth[id] = 1
		}
	}
	maxDepth := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if depth[node] > maxDepth {
			maxDepth = depth[node]
		}
		for _, child := range dependents[node] {
			inDegree[child]--
			childDepth := depth[node] + 1
			if childDepth > depth[child] {
				depth[child] = childDepth
			}
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	return maxDepth
}

// NamespaceFragment prefixes all step IDs and DependsOn references
// with the planner step ID to prevent collisions. Also forces all
// steps to StepTypeNormal — planners cannot spawn nested planners.
func NamespaceFragment(
	plannerID string, fragment []StepDef,
) []StepDef {
	if plannerID == "" {
		panic("NamespaceFragment: plannerID must not be empty")
	}
	if len(fragment) == 0 {
		panic("NamespaceFragment: fragment must not be empty")
	}
	result := make([]StepDef, len(fragment))
	for i, step := range fragment {
		result[i] = step
		result[i].ID = plannerID + "." + step.ID
		result[i].Type = StepTypeNormal
		if len(step.DependsOn) > 0 {
			deps := make([]string, len(step.DependsOn))
			for j, dep := range step.DependsOn {
				deps[j] = plannerID + "." + dep
			}
			result[i].DependsOn = deps
		}
	}
	return result
}

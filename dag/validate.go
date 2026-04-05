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

	if err := validateConcurrency(def); err != nil {
		return err
	}

	if err := validateIdempotencyKey(def.IdempotencyKey); err != nil {
		return err
	}

	if err := validateSticky(def); err != nil {
		return err
	}

	if err := validatePriority(def); err != nil {
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
// Also checks for Map step nesting (Map steps cannot depend on other Map steps).
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

	// Build a map of step types for nesting checks.
	stepTypes := make(map[string]StepType, len(def.Steps))
	for _, step := range def.Steps {
		stepTypes[step.ID] = step.Type
	}

	for _, step := range def.Steps {
		if err := validateSingleStep(step, ids); err != nil {
			return err
		}
		if err := validateWaitForEventStep(step, ids); err != nil {
			return err
		}
		if step.Type == StepTypeMap {
			if err := validateMapNesting(step, stepTypes); err != nil {
				return err
			}
		}
	}
	return validateAuxTargets(def, ids)
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
	if err := validateMapConfig(step); err != nil {
		return err
	}
	if err := validateSkipIf(step); err != nil {
		return err
	}
	if err := validateSubWorkflowConfig(step); err != nil {
		return err
	}
	if err := validateSleepStep(step); err != nil {
		return err
	}
	if err := validateApprovalStep(step); err != nil {
		return err
	}
	if err := validatePlannerConfig(step); err != nil {
		return err
	}
	if err := validateRateLimit(step); err != nil {
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
	if step.MaxTaskConcurrency < 0 || step.MaxTaskConcurrency > 1000 {
		return fmt.Errorf(
			"step %q MaxTaskConcurrency is %d (must be 0..1000)",
			step.ID, step.MaxTaskConcurrency,
		)
	}
	return nil
}

// stepRequiresTask returns true for step types that must have a non-empty Task field.
// Future step types like Sleep and WaitForEvent won't require a task.
func stepRequiresTask(t StepType) bool {
	switch t {
	case StepTypeNormal, StepTypeAgentLoop, StepTypeSubWorkflow,
		StepTypeAgent, StepTypeMap, StepTypePlanner:
		return true
	case StepTypeSleep, StepTypeWaitForEvent, StepTypeApproval:
		return false
	default:
		return false
	}
}

// validateLoopConfig checks AgentLoop/Config consistency for a single step.
func validateLoopConfig(step StepDef) error {
	if step.ID == "" {
		panic("validateLoopConfig: step ID is empty")
	}
	if step.Task == "" && stepRequiresTask(step.Type) {
		panic("validateLoopConfig: step task is empty")
	}
	// Non-task step types (e.g., sleep, wait-for-event) skip.
	if step.Task == "" {
		return nil
	}
	if step.Type == StepTypeAgentLoop {
		cfg, err := ParseAgentLoopConfig(step)
		if err != nil {
			return fmt.Errorf(
				"step %q is AgentLoop but Config is invalid: %w",
				step.ID, err,
			)
		}
		if cfg.MaxIterations <= 0 {
			return fmt.Errorf(
				"step %q is AgentLoop but MaxIterations is "+
					"%d (must be > 0)",
				step.ID, cfg.MaxIterations,
			)
		}
	}
	return nil
}

// validateMapConfig checks Map step configuration and dependencies.
// Map steps must have exactly one dependency, and MaxItems must be
// within bounds.
func validateMapConfig(step StepDef) error {
	if step.ID == "" {
		panic("validateMapConfig: step ID is empty")
	}
	if step.Task == "" && stepRequiresTask(step.Type) {
		panic("validateMapConfig: step task is empty")
	}
	// Non-task step types (e.g., sleep, wait-for-event) skip.
	if step.Task == "" {
		return nil
	}
	if step.Type != StepTypeMap {
		return nil
	}
	cfg, err := ParseMapConfig(step)
	if err != nil {
		panic("Map step must have Map config")
	}
	if len(step.DependsOn) != 1 {
		return fmt.Errorf(
			"step %q is Map but has %d dependencies "+
				"(must have exactly one)",
			step.ID, len(step.DependsOn),
		)
	}
	if cfg.MaxItems <= 0 {
		return fmt.Errorf(
			"step %q is Map but MaxItems is %d (must be > 0)",
			step.ID, cfg.MaxItems,
		)
	}
	if cfg.MaxItems > 10000 {
		return fmt.Errorf(
			"step %q is Map but MaxItems is %d "+
				"(must be <= 10000)",
			step.ID, cfg.MaxItems,
		)
	}
	return nil
}

// validateMapNesting checks that Map steps do not depend on other Map steps.
// Nested map steps would create quadratic fanout which is not supported.
func validateMapNesting(step StepDef, stepTypes map[string]StepType) error {
	if step.ID == "" {
		panic("validateMapNesting: step ID is empty")
	}
	if step.Type != StepTypeMap {
		panic("validateMapNesting: called on non-Map step")
	}
	if stepTypes == nil {
		panic("validateMapNesting: stepTypes map is nil")
	}
	for _, dep := range step.DependsOn {
		if stepTypes[dep] == StepTypeMap {
			return fmt.Errorf(
				"step %q is Map and depends on %q which is also Map (nesting not supported)",
				step.ID, dep,
			)
		}
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
	if step.Task == "" && stepRequiresTask(step.Type) {
		panic("validateSkipIf: step task is empty")
	}
	// Non-task step types (e.g., sleep, wait-for-event) skip task validation.
	if step.Task == "" {
		return nil
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

// validateAuxTargets checks OnFailure and Compensate target constraints.
// Targets must not have DependsOn (they receive error context, not
// upstream output) and must not self-reference.
func validateAuxTargets(
	def WorkflowDef, ids map[string]bool,
) error {
	byID := make(map[string]StepDef, len(def.Steps))
	for _, s := range def.Steps {
		byID[s.ID] = s
	}
	for _, step := range def.Steps {
		if step.OnFailure != "" {
			if step.OnFailure == step.ID {
				return fmt.Errorf(
					"step %q OnFailure references itself",
					step.ID,
				)
			}
			target := byID[step.OnFailure]
			if len(target.DependsOn) > 0 {
				return fmt.Errorf(
					"step %q OnFailure target %q must not "+
						"have DependsOn",
					step.ID, step.OnFailure,
				)
			}
		}
		if step.Compensate != "" {
			if step.Compensate == step.ID {
				return fmt.Errorf(
					"step %q Compensate references itself",
					step.ID,
				)
			}
			target := byID[step.Compensate]
			if len(target.DependsOn) > 0 {
				return fmt.Errorf(
					"step %q Compensate target %q must not "+
						"have DependsOn",
					step.ID, step.Compensate,
				)
			}
		}
	}
	return nil
}

// validateConcurrency checks workflow-level concurrency limits.
// MaxSteps must be in range [0, 1000] if set.
func validateConcurrency(def WorkflowDef) error {
	if def.Name == "" {
		panic("validateConcurrency: workflow name is empty")
	}
	if len(def.Steps) == 0 {
		panic("validateConcurrency: called with empty steps")
	}
	if def.Concurrency == nil {
		return nil
	}
	if def.Concurrency.MaxSteps < 0 ||
		def.Concurrency.MaxSteps > 1000 {
		return fmt.Errorf(
			"workflow %q Concurrency.MaxSteps is %d "+
				"(must be 0..1000)",
			def.Name, def.Concurrency.MaxSteps,
		)
	}
	return nil
}

// validateSticky checks sticky strategy constraints.
func validateSticky(def WorkflowDef) error {
	if def.Sticky == StickyNone {
		return nil
	}
	if def.Sticky != StickySoft && def.Sticky != StickyHard {
		return fmt.Errorf(
			"workflow %q: invalid sticky strategy %q",
			def.Name, def.Sticky,
		)
	}
	if def.Sticky == StickyHard && def.Timeout == 0 {
		return fmt.Errorf(
			"workflow %q: hard sticky requires a timeout "+
				"to prevent permanent blocking",
			def.Name,
		)
	}
	for _, step := range def.Steps {
		if step.WorkerGroup != "" {
			return fmt.Errorf(
				"workflow %q: sticky is incompatible with "+
					"per-step WorkerGroup (step %q)",
				def.Name, step.ID,
			)
		}
	}
	return nil
}

// validateIdempotencyKey checks dot-path syntax: no empty segments,
// no leading/trailing dots.
func validateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if key[0] == '.' || key[len(key)-1] == '.' {
		return fmt.Errorf(
			"idempotency_key %q: must not start or end with dot",
			key,
		)
	}
	for i := range len(key) - 1 {
		if key[i] == '.' && key[i+1] == '.' {
			return fmt.Errorf(
				"idempotency_key %q: empty segment", key,
			)
		}
	}
	return nil
}

// validatePriority checks PriorityConfig constraints.
func validatePriority(def WorkflowDef) error {
	if def.Priority == nil {
		return nil
	}
	if def.Priority.Key == "" {
		return fmt.Errorf("priority: key must not be empty")
	}
	if len(def.Priority.Rules) == 0 {
		return fmt.Errorf("priority: rules must not be empty")
	}
	if len(def.Priority.Rules) > 20 {
		return fmt.Errorf("priority: max 20 rules")
	}
	for _, offset := range def.Priority.Rules {
		if offset < -600 || offset > 600 {
			return fmt.Errorf(
				"priority: offset %d out of [-600, 600]",
				offset,
			)
		}
	}
	if def.Priority.DefaultOffset < -600 ||
		def.Priority.DefaultOffset > 600 {
		return fmt.Errorf(
			"priority: default_offset out of [-600, 600]",
		)
	}
	return nil
}

package dag

import "fmt"

// PlannerConfig holds configuration for planner steps that generate
// DAG fragments at runtime. MaxSteps bounds the number of steps the
// planner can emit; MaxDepth bounds the longest dependency chain.
// AllowedTasks restricts which task types the fragment may reference.
type PlannerConfig struct {
	MaxSteps     int      `json:"max_steps"`
	MaxDepth     int      `json:"max_depth,omitempty"`
	AllowedTasks []string `json:"allowed_tasks,omitempty"`
}

// validatePlannerConfig checks Planner step configuration.
// Non-Planner steps are skipped. Planner steps must have a valid
// Config with MaxSteps in [1, 100] and MaxDepth in [0, 10].
func validatePlannerConfig(step StepDef) error {
	if step.ID == "" {
		panic("validatePlannerConfig: step.ID must not be empty")
	}
	if step.Type != StepTypePlanner {
		return nil
	}
	cfg, err := ParsePlannerConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: invalid planner config: %w",
			step.ID, err,
		)
	}
	if cfg.MaxSteps < 1 || cfg.MaxSteps > 100 {
		return fmt.Errorf(
			"step %q: PlannerConfig.MaxSteps is %d "+
				"(must be 1..100)",
			step.ID, cfg.MaxSteps,
		)
	}
	if cfg.MaxDepth < 0 || cfg.MaxDepth > 10 {
		return fmt.Errorf(
			"step %q: PlannerConfig.MaxDepth is %d "+
				"(must be 0..10)",
			step.ID, cfg.MaxDepth,
		)
	}
	return nil
}

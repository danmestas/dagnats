package dag

import "fmt"

// SubWorkflowConfig holds configuration for sub-workflow steps.
// Workflow names the child workflow definition to spawn.
// Detach controls whether the parent waits for the child to complete:
// when true, the parent step completes immediately after spawn.
type SubWorkflowConfig struct {
	Workflow string `json:"workflow"`
	Detach   bool   `json:"detach,omitempty"`
}

// validateSubWorkflowConfig checks SubWorkflow step configuration.
// Non-SubWorkflow steps are skipped. SubWorkflow steps must have a
// valid Config with a non-empty Workflow name.
func validateSubWorkflowConfig(step StepDef) error {
	if step.ID == "" {
		panic("validateSubWorkflowConfig: step.ID must not be empty")
	}
	if step.Type != StepTypeSubWorkflow {
		return nil
	}
	cfg, err := ParseSubWorkflowConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: invalid sub-workflow config: %w",
			step.ID, err,
		)
	}
	if cfg.Workflow == "" {
		return fmt.Errorf(
			"step %q: sub-workflow config must have "+
				"non-empty Workflow",
			step.ID,
		)
	}
	return nil
}

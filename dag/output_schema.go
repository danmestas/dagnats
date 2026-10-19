package dag

import (
	"encoding/json"
	"fmt"
)

// validateOutputSchema rejects a workflow whose OutputSchema cannot be
// parsed, so a malformed schema fails at register time rather than
// failing every run at completion.
func validateOutputSchema(def WorkflowDef) error {
	if def.Name == "" {
		panic("validateOutputSchema: workflow name is empty")
	}
	if len(def.Steps) == 0 {
		panic("validateOutputSchema: workflow has no steps")
	}
	if len(def.OutputSchema) == 0 {
		return nil
	}
	var s schemaNode
	if err := json.Unmarshal(def.OutputSchema, &s); err != nil {
		return fmt.Errorf(
			"workflow %q output_schema is invalid: %w", def.Name, err,
		)
	}
	return nil
}

// ValidateRunOutput checks the run's final output against the
// workflow's OutputSchema. The final output is the output of every
// terminal step (one no other step depends on) that completed with a
// non-empty output; each must match. Returns the offending step ID with
// the violation (path + reason). A def without a schema, or terminal
// steps that produced no output (skipped, or void), pass.
func ValidateRunOutput(
	def WorkflowDef, steps map[string]StepState,
) (string, error) {
	if steps == nil {
		panic("ValidateRunOutput: steps must not be nil")
	}
	if len(def.OutputSchema) == 0 {
		return "", nil
	}
	hasDependents := make(map[string]bool, len(def.Steps))
	for _, step := range def.Steps {
		for _, dep := range step.DependsOn {
			hasDependents[dep] = true
		}
	}
	for _, step := range def.Steps {
		state := steps[step.ID]
		if hasDependents[step.ID] ||
			state.Status != StepStatusCompleted ||
			len(state.Output) == 0 {
			continue
		}
		if err := ValidateSchema(def.OutputSchema, state.Output); err != nil {
			return step.ID, err
		}
	}
	return "", nil
}

// cli/run_output.go
// Command for printing the final output of a completed workflow run.
// Shows output from terminal steps (steps with no dependents).
package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/danmestas/dagnats/dag"
)

// runOutputResult is the JSON response for run output.
type runOutputResult struct {
	RunID   string            `json:"run_id"`
	Status  string            `json:"status"`
	Outputs map[string]string `json:"outputs,omitempty"`
}

// runOutputCmd prints the output of a completed run's terminal steps.
func runOutputCmd(args []string) {
	if args == nil {
		panic("runOutputCmd: args must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	hasLast := HasLastFlag(args)
	args = StripLastFlag(args)

	var rawID string
	if len(args) == 1 {
		rawID = args[0]
	} else if !hasLast {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run output"+
				" <run-id> [--last] [--json]")
		os.Exit(1)
	}

	svc, nc := connectService()
	defer nc.Close()

	runID, err := ResolveRunID(svc, rawID, hasLast)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve run: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		os.Exit(1)
	}

	def, err := svc.GetWorkflow(run.WorkflowID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get workflow: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		result := buildRunOutputResult(run, def)
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Print(FormatRunOutput(run, def))
}

// buildRunOutputResult constructs a runOutputResult from a run
// and its workflow definition, including only terminal step outputs.
func buildRunOutputResult(
	run dag.WorkflowRun, def dag.WorkflowDef,
) runOutputResult {
	if run.RunID == "" {
		panic("buildRunOutputResult: RunID must not be empty")
	}
	if run.Steps == nil {
		panic("buildRunOutputResult: Steps must not be nil")
	}

	result := runOutputResult{
		RunID:  run.RunID,
		Status: run.Status.String(),
	}

	if run.Status != dag.RunStatusCompleted {
		return result
	}

	terminals := findTerminalSteps(def)
	outputs := make(map[string]string, len(terminals))
	for _, stepID := range terminals {
		state, ok := run.Steps[stepID]
		if !ok || state.Status != dag.StepStatusCompleted {
			continue
		}
		if len(state.Output) > 0 {
			outputs[stepID] = string(state.Output)
		}
	}

	if len(outputs) > 0 {
		result.Outputs = outputs
	}
	return result
}

// FormatRunOutput returns the output of terminal steps in a run.
// Terminal steps are steps that no other step depends on.
func FormatRunOutput(
	run dag.WorkflowRun, def dag.WorkflowDef,
) string {
	if run.Steps == nil {
		panic("FormatRunOutput: Steps must not be nil")
	}
	if run.RunID == "" {
		panic("FormatRunOutput: RunID must not be empty")
	}

	if run.Status != dag.RunStatusCompleted {
		return fmt.Sprintf(
			"Run %s is not completed (status: %s)\n",
			run.RunID, run.Status.String())
	}

	terminals := findTerminalSteps(def)
	if len(terminals) == 0 {
		panic("FormatRunOutput: no terminal steps found")
	}

	var b strings.Builder
	for _, stepID := range terminals {
		state, ok := run.Steps[stepID]
		if !ok {
			continue
		}
		if len(terminals) > 1 {
			fmt.Fprintf(&b, "--- %s ---\n", stepID)
		}
		if len(state.Output) > 0 {
			b.Write(state.Output)
			if state.Output[len(state.Output)-1] != '\n' {
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}

// findTerminalSteps returns step IDs that no other step depends on.
func findTerminalSteps(def dag.WorkflowDef) []string {
	if len(def.Steps) == 0 {
		panic("findTerminalSteps: def must have steps")
	}

	const maxSteps = 1000
	if len(def.Steps) > maxSteps {
		panic("findTerminalSteps: steps exceeds max bound")
	}

	hasDependents := make(map[string]bool, len(def.Steps))
	for _, step := range def.Steps {
		for _, dep := range step.DependsOn {
			hasDependents[dep] = true
		}
	}

	terminals := make([]string, 0, len(def.Steps))
	for _, step := range def.Steps {
		if !hasDependents[step.ID] {
			terminals = append(terminals, step.ID)
		}
	}
	return terminals
}

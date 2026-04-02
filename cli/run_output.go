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

// runOutputCmd prints the output of a completed run's terminal steps.
func runOutputCmd(args []string) {
	if args == nil {
		panic("runOutputCmd: args must not be nil")
	}
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run output <run-id>")
		os.Exit(1)
	}
	runID := args[0]
	if runID == "" {
		panic("runOutputCmd: runID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

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

	fmt.Print(FormatRunOutput(run, def))
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

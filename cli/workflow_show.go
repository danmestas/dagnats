// cli/workflow_show.go
// Displays detailed info for a single registered workflow, including
// its step table with dependency edges. Requires a live NATS connection.
package cli

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/danmestas/dagnats/dag"
)

// runWorkflowShowCmd fetches and displays a single workflow definition.
func runWorkflowShowCmd(args []string) {
	if args == nil {
		panic("runWorkflowShowCmd: args must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	if jsonOutput {
		args = StripJSONFlag(args)
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats workflow show <name> [--json]")
		os.Exit(1)
	}
	workflowName := args[0]
	if workflowName == "" {
		panic(
			"runWorkflowShowCmd: workflowName must not be empty",
		)
	}

	svc, nc := connectService()
	defer nc.Close()

	def, err := svc.GetWorkflow(workflowName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get workflow: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, def); err != nil {
			fmt.Fprintf(
				os.Stderr, "format json: %v\n", err,
			)
			os.Exit(1)
		}
		return
	}

	printWorkflowShowHuman(def)
}

// printWorkflowShowHuman renders the human-readable workflow header.
func printWorkflowShowHuman(def dag.WorkflowDef) {
	if def.Name == "" {
		panic("printWorkflowShowHuman: def.Name must not be empty")
	}
	if len(def.Steps) == 0 {
		panic("printWorkflowShowHuman: def must have steps")
	}

	timeout := "none"
	if def.Timeout > 0 {
		timeout = def.Timeout.String()
	}

	fmt.Printf("Name:        %s\n", def.Name)
	fmt.Printf("Version:     %s\n", def.Version)
	fmt.Printf("Steps:       %d\n", len(def.Steps))
	fmt.Printf("Timeout:     %s\n", timeout)
	fmt.Println()

	printStepTable(def)
}

// printStepTable renders the step dependency table using tabwriter.
func printStepTable(def dag.WorkflowDef) {
	if def.Name == "" {
		panic("printStepTable: def.Name must not be empty")
	}
	if len(def.Steps) == 0 {
		panic("printStepTable: def must have at least one step")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  ID\tTASK\tDEPENDS ON")

	const maxSteps = 10000
	for i, step := range def.Steps {
		if i >= maxSteps {
			break
		}
		deps := "-"
		if len(step.DependsOn) > 0 {
			deps = strings.Join(step.DependsOn, ", ")
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\n",
			step.ID, step.Task, deps)
	}

	w.Flush()
}

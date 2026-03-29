package cli

import (
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
)

// runRunCmd dispatches run subcommands. Stubs are placeholders until HTTP
// client integration is added in a later task.
func runRunCmd(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: dagnats run <start|status|history|retry>")
		return
	}
	switch args[0] {
	case "start":
		fmt.Println("(run start not yet implemented)")
	case "status":
		fmt.Println("(run status not yet implemented)")
	case "history":
		fmt.Println("(run history not yet implemented)")
	case "retry":
		fmt.Println("(run retry not yet implemented)")
	default:
		fmt.Printf("unknown run subcommand: %s\n", args[0])
	}
}

// FormatRunStatus renders a WorkflowRun as a human-readable string. Steps are
// rendered individually to avoid exposing raw Go map syntax in terminal output.
func FormatRunStatus(run dag.WorkflowRun) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Run:      %s\n", run.RunID)
	fmt.Fprintf(&b, "Workflow: %s\n", run.WorkflowID)
	fmt.Fprintf(&b, "Status:   %s\n", run.Status.String())
	fmt.Fprintf(&b, "Created:  %s\n", run.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(&b, "\nSteps:\n")
	for id, state := range run.Steps {
		fmt.Fprintf(&b, "  %-20s %s (attempts: %d)\n", id, state.Status.String(), state.Attempts)
	}
	return b.String()
}

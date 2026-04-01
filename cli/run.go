// cli/run.go
// Commands for managing workflow runs: start, status, cancel, signal.
package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/danmestas/dagnats/dag"
)

// runRunCmd dispatches run subcommands.
func runRunCmd(args []string) {
	if len(args) == 0 {
		fmt.Println(
			"Usage: dagnats run <start|status|cancel|signal|list|events>",
		)
		return
	}
	switch args[0] {
	case "start":
		runStartCmd(args[1:])
	case "status":
		runStatusCmd(args[1:])
	case "cancel":
		runCancelCmd(args[1:])
	case "signal":
		runSignalCmd(args[1:])
	case "list":
		runListCmd(args[1:])
	case "events":
		runEventsCmd(args[1:])
	default:
		fmt.Printf("unknown run subcommand: %s\n", args[0])
	}
}

// runStartCmd starts a new workflow run with optional input.
func runStartCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: dagnats run start <workflow> [input]")
		os.Exit(1)
	}
	workflowName := args[0]
	if workflowName == "" {
		panic("runStartCmd: workflowName must not be empty")
	}

	var input []byte
	if len(args) > 1 {
		input = []byte(args[1])
	}

	svc, nc := connectService()
	defer nc.Close()

	runID, err := svc.StartRun(context.Background(), workflowName, input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start run: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Started: %s\n", runID)
}

// runStatusCmd retrieves and prints the status of a workflow run.
func runStatusCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Usage: dagnats run status <run-id>")
		os.Exit(1)
	}
	runID := args[0]
	if runID == "" {
		panic("runStatusCmd: runID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	run, err := svc.GetRun(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(FormatRunStatus(run))
}

// runCancelCmd publishes a workflow.cancelled event to cancel a running workflow.
func runCancelCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Usage: dagnats run cancel <run-id>")
		os.Exit(1)
	}
	runID := args[0]
	if runID == "" {
		panic("runCancelCmd: runID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.CancelRun(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cancel run: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Cancelled: %s\n", runID)
}

// runSignalCmd sends a signal to a running workflow.
func runSignalCmd(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run signal <run-id> <name> <payload>")
		os.Exit(1)
	}

	runID := args[0]
	name := args[1]
	payload := args[2]

	if runID == "" {
		panic("runSignalCmd: runID must not be empty")
	}
	if name == "" {
		panic("runSignalCmd: name must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.SendSignal(context.Background(), runID, name, []byte(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "send signal: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Signal sent: %s\n", name)
}

// runListCmd lists workflow runs with optional filtering.
func runListCmd(args []string) {
	var workflowFilter, statusFilter string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--workflow=") {
			workflowFilter = strings.TrimPrefix(arg, "--workflow=")
		}
		if strings.HasPrefix(arg, "--status=") {
			statusFilter = strings.TrimPrefix(arg, "--status=")
		}
	}

	svc, nc := connectService()
	defer nc.Close()

	runs, err := svc.ListRuns(context.Background(), workflowFilter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list runs: %v\n", err)
		os.Exit(1)
	}

	// Client-side status filter
	if statusFilter != "" {
		filtered := runs[:0]
		for _, r := range runs {
			if strings.EqualFold(r.Status.String(), statusFilter) {
				filtered = append(filtered, r)
			}
		}
		runs = filtered
	}

	if len(runs) == 0 {
		fmt.Println("No runs found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "RUN_ID\tWORKFLOW\tSTATUS\tCREATED\tSTEPS")

	for _, run := range runs {
		created := run.CreatedAt.Format("2006-01-02 15:04:05")
		stepCount := len(run.Steps)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n",
			run.RunID, run.WorkflowID, run.Status.String(), created, stepCount)
	}

	w.Flush()
}

// runEventsCmd retrieves and prints the event history for a run.
func runEventsCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run events <run-id> [--full]")
		os.Exit(1)
	}

	runID := args[0]
	if runID == "" {
		panic("runEventsCmd: runID must not be empty")
	}

	fullData := false
	for _, arg := range args[1:] {
		if arg == "--full" {
			fullData = true
		}
	}

	svc, nc := connectService()
	defer nc.Close()

	events, err := svc.ListRunEvents(
		context.Background(), runID, fullData,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list run events: %v\n", err)
		os.Exit(1)
	}

	if len(events) == 0 {
		fmt.Println("No events found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIMESTAMP\tTYPE\tSTEP\tDATA")

	for _, evt := range events {
		timestamp := evt.Timestamp.Format("2006-01-02 15:04:05")
		step := evt.StepID
		if step == "" {
			step = "-"
		}
		data := evt.Data
		if data == "" {
			data = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", timestamp, evt.Type, step, data)
	}

	w.Flush()
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

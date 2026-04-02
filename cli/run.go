// cli/run.go
// Commands for managing workflow runs: start, status, cancel, signal.
package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
)

// runRunCmd dispatches run subcommands.
func runRunCmd(args []string) {
	if HasHelpFlag(args) {
		printRunUsage()
		return
	}
	if len(args) == 0 {
		printRunUsage()
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
	case "inspect":
		runInspectCmd(args[1:])
	case "watch":
		runWatchCmd(args[1:])
	case "output":
		runOutputCmd(args[1:])
	default:
		fmt.Printf("unknown run subcommand: %s\n", args[0])
	}
}

// printRunUsage prints the run subcommand help text.
func printRunUsage() {
	fmt.Println("Usage: dagnats run <command>")
	fmt.Println("Commands:")
	fmt.Println("  start    start a workflow run")
	fmt.Println("  status   show run status")
	fmt.Println("  inspect  unified debug view for a run")
	fmt.Println("  cancel   cancel a running workflow")
	fmt.Println("  signal   send a signal to a run")
	fmt.Println("  list     list workflow runs")
	fmt.Println("  events   show run event history")
	fmt.Println("  watch    watch a run until completion")
	fmt.Println("  output   print final output of a completed run")
}

// runStartResult is the JSON response for run start.
type runStartResult struct {
	RunID string `json:"run_id"`
}

// runStartCmd starts a new workflow run with optional input.
func runStartCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run start"+
				" <workflow> [input] [--watch] [--json]")
		os.Exit(1)
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	workflowName := args[0]
	if workflowName == "" {
		panic("runStartCmd: workflowName must not be empty")
	}

	var input []byte
	var watch bool
	for _, arg := range args[1:] {
		if arg == "--watch" {
			watch = true
		} else if input == nil {
			input = []byte(arg)
		}
	}

	svc, nc := connectService()
	defer nc.Close()

	runID, err := svc.StartRun(
		context.Background(), workflowName, input,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start run: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		result := runStartResult{RunID: runID}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Started: %s\n", runID)

	if watch {
		watchRun(svc, runID)
	}
}

// runStatusCmd retrieves and prints the status of a workflow run.
func runStatusCmd(args []string) {
	if args == nil {
		panic("runStatusCmd: args must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run status <run-id> [--json]")
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

	if jsonOutput {
		if err := FormatJSON(os.Stdout, run); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Print(FormatRunStatus(run))
}

// runCancelResult is the JSON response for run cancel.
type runCancelResult struct {
	RunID     string `json:"run_id"`
	Cancelled bool   `json:"cancelled"`
}

// runCancelCmd publishes a workflow.cancelled event.
func runCancelCmd(args []string) {
	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run cancel <run-id> [--json]")
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

	if jsonOutput {
		result := runCancelResult{
			RunID: runID, Cancelled: true,
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Cancelled: %s\n", runID)
}

// runSignalResult is the JSON response for run signal.
type runSignalResult struct {
	RunID  string `json:"run_id"`
	Signal string `json:"signal"`
	Sent   bool   `json:"sent"`
}

// runSignalCmd sends a signal to a running workflow.
func runSignalCmd(args []string) {
	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) != 3 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run signal"+
				" <run-id> <name> <payload> [--json]")
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

	err := svc.SendSignal(
		context.Background(), runID, name, []byte(payload),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "send signal: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		result := runSignalResult{
			RunID: runID, Signal: name, Sent: true,
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Signal sent: %s\n", name)
}

// runListCmd lists workflow runs with optional filtering.
func runListCmd(args []string) {
	if args == nil {
		panic("runListCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runListCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	var workflowFilter, statusFilter string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--workflow=") {
			workflowFilter = strings.TrimPrefix(
				arg, "--workflow=",
			)
		}
		if strings.HasPrefix(arg, "--status=") {
			statusFilter = strings.TrimPrefix(
				arg, "--status=",
			)
		}
	}

	svc, nc := connectService()
	defer nc.Close()

	runs, err := svc.ListRuns(
		context.Background(), workflowFilter,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list runs: %v\n", err)
		os.Exit(1)
	}

	// Client-side status filter
	if statusFilter != "" {
		filtered := runs[:0]
		for _, r := range runs {
			if strings.EqualFold(
				r.Status.String(), statusFilter,
			) {
				filtered = append(filtered, r)
			}
		}
		runs = filtered
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, runs); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if len(runs) == 0 {
		fmt.Println("No runs found.")
		return
	}

	printRunTable(runs)
}

// printRunTable writes a formatted table of workflow runs to stdout.
func printRunTable(runs []dag.WorkflowRun) {
	if len(runs) == 0 {
		panic("printRunTable: runs must not be empty")
	}
	if len(runs) > 10000 {
		panic("printRunTable: runs exceeds max bound")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "RUN_ID\tWORKFLOW\tSTATUS\tCREATED\tSTEPS")

	for _, run := range runs {
		created := run.CreatedAt.Format("2006-01-02 15:04:05")
		stepCount := len(run.Steps)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n",
			run.RunID, run.WorkflowID,
			ColorStatus(run.Status.String()),
			created, stepCount)
	}

	w.Flush()
}

// runEventsCmd retrieves and prints the event history for a run.
func runEventsCmd(args []string) {
	if args == nil {
		panic("runEventsCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runEventsCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run events <run-id>"+
				" [--full] [--type=TYPE] [--step=STEP]"+
				" [--json]")
		os.Exit(1)
	}

	runID := args[0]
	if runID == "" {
		panic("runEventsCmd: runID must not be empty")
	}

	fullData, typeFilter, stepFilter := parseEventFlags(args[1:])

	svc, nc := connectService()
	defer nc.Close()

	events, err := svc.ListRunEvents(
		context.Background(), runID, fullData,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list run events: %v\n", err)
		os.Exit(1)
	}

	events = filterRunEvents(events, typeFilter, stepFilter)

	if len(events) == 0 {
		fmt.Println("No events found.")
		return
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, events); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	printEventsTable(events)
}

// parseEventFlags extracts --full, --type, and --step from flag args.
func parseEventFlags(
	args []string,
) (fullData bool, typeFilter, stepFilter string) {
	if len(args) > 100 {
		panic("parseEventFlags: args exceeds max bound")
	}
	for _, arg := range args {
		if arg == "--full" {
			fullData = true
		}
		if strings.HasPrefix(arg, "--type=") {
			typeFilter = strings.TrimPrefix(arg, "--type=")
		}
		if strings.HasPrefix(arg, "--step=") {
			stepFilter = strings.TrimPrefix(arg, "--step=")
		}
	}
	return fullData, typeFilter, stepFilter
}

// printEventsTable writes a formatted table of run events to stdout.
func printEventsTable(events []api.RunEvent) {
	if len(events) == 0 {
		panic("printEventsTable: events must not be empty")
	}
	if len(events) > 10000 {
		panic("printEventsTable: events exceeds max bound")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIMESTAMP\tTYPE\tSTEP\tDATA")

	for _, evt := range events {
		ts := evt.Timestamp.Format("2006-01-02 15:04:05")
		step := evt.StepID
		if step == "" {
			step = "-"
		}
		data := evt.Data
		if data == "" {
			data = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			ts, evt.Type, step, data)
	}

	w.Flush()
}

// filterRunEvents applies optional type and step filters to a slice
// of run events, returning only those that match all non-empty filters.
func filterRunEvents(
	events []api.RunEvent, typeFilter, stepFilter string,
) []api.RunEvent {
	if len(events) > 10000 {
		panic("filterRunEvents: events exceeds 10000 bound")
	}
	if typeFilter == "" && stepFilter == "" {
		return events
	}

	filtered := make([]api.RunEvent, 0, len(events))
	for _, evt := range events {
		if typeFilter != "" && evt.Type != typeFilter {
			continue
		}
		if stepFilter != "" && evt.StepID != stepFilter {
			continue
		}
		filtered = append(filtered, evt)
	}
	return filtered
}

// FormatRunStatus renders a WorkflowRun as a human-readable string.
// Steps are rendered individually to avoid exposing raw Go map syntax.
func FormatRunStatus(run dag.WorkflowRun) string {
	if run.Steps == nil {
		panic("FormatRunStatus: Steps must not be nil")
	}
	if run.RunID == "" {
		panic("FormatRunStatus: RunID must not be empty")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Run:      %s\n", run.RunID)
	fmt.Fprintf(&b, "Workflow: %s\n", run.WorkflowID)
	fmt.Fprintf(&b, "Status:   %s\n", ColorStatus(run.Status.String()))
	fmt.Fprintf(&b, "Created:  %s\n",
		run.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(&b, "\nSteps:\n")
	for id, state := range run.Steps {
		fmt.Fprintf(&b, "  %s\n", formatStepLine(id, state))
	}
	return b.String()
}

// formatStepLine renders a single step as a human-readable line,
// including error and iteration details when present.
func formatStepLine(id string, state dag.StepState) string {
	if id == "" {
		panic("formatStepLine: id must not be empty")
	}
	if state.Attempts < 0 {
		panic("formatStepLine: attempts must not be negative")
	}

	line := fmt.Sprintf("%-20s %s (attempts: %d)",
		id, ColorStatus(state.Status.String()), state.Attempts)

	if state.Iterations > 0 {
		line += fmt.Sprintf(" (iterations: %d)", state.Iterations)
	}
	if state.Status == dag.StepStatusFailed && state.Error != "" {
		line += fmt.Sprintf(" error: %s", state.Error)
	}
	return line
}

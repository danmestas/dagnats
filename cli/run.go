// cli/run.go
// Commands for managing workflow runs: start, status, cancel, signal.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
)

// printRunOutputForStart fetches a completed run and prints
// its terminal step output. Used by --output flag on run start.
func printRunOutputForStart(
	svc *api.Service, runID string,
) {
	if svc == nil {
		panic("printRunOutputForStart: svc must not be nil")
	}
	if runID == "" {
		panic(
			"printRunOutputForStart: runID must not be empty",
		)
	}
	ctx := context.Background()
	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		return
	}
	def, err := svc.GetWorkflow(run.WorkflowID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get workflow: %v\n", err)
		return
	}
	fmt.Printf("\nOutput:\n")
	fmt.Print(FormatRunOutput(run, def))
}

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
	case "cancel-all":
		runCancelAllCmd(args[1:])
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
	case "retry":
		runRetryCmd(args[1:])
	default:
		fmt.Printf("unknown run subcommand: %s\n", args[0])
	}
}

// printRunUsage prints the run subcommand help text.
func printRunUsage() {
	fmt.Println("Usage: dagnats run <command> [--json]")
	fmt.Println("Commands:")
	fmt.Println("  start       start a workflow run")
	fmt.Println("  status      show run status")
	fmt.Println("  inspect     unified debug view for a run")
	fmt.Println("  cancel      cancel a running workflow")
	fmt.Println("  cancel-all  cancel multiple runs by workflow")
	fmt.Println("  signal      send a signal to a run")
	fmt.Println("  list     list workflow runs")
	fmt.Println("  events   show run event history")
	fmt.Println("  watch    watch a run until completion")
	fmt.Println("  output   print final output of a completed run")
	fmt.Println("  retry    re-run a workflow from a previous run")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --at=TIME      schedule run at RFC3339 time")
	fmt.Println("  --scheduled    list scheduled runs")
	fmt.Println("  --last         use the most recent run")
	fmt.Println("  --json         output as JSON")
	fmt.Println()
	fmt.Println("Run IDs accept 8+ character prefixes.")
}

// HasLastFlag returns true when args contains "--last".
func HasLastFlag(args []string) bool {
	if args == nil {
		panic("HasLastFlag: args must not be nil")
	}

	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("HasLastFlag: args exceeds max bound")
	}

	for _, arg := range args {
		if arg == "--last" {
			return true
		}
	}
	return false
}

// StripLastFlag returns a copy of args with "--last" removed.
func StripLastFlag(args []string) []string {
	if args == nil {
		panic("StripLastFlag: args must not be nil")
	}

	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("StripLastFlag: args exceeds max bound")
	}

	result := make([]string, 0, len(args))
	for _, arg := range args {
		if arg != "--last" {
			result = append(result, arg)
		}
	}
	return result
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
				" <workflow> [input]"+
				" [--watch] [--output] [--json]")
		os.Exit(1)
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	workflowName := args[0]
	if workflowName == "" {
		panic("runStartCmd: workflowName must not be empty")
	}

	var input []byte
	var watch, showOutput bool
	var runAtStr string
	for _, arg := range args[1:] {
		switch {
		case arg == "--watch":
			watch = true
		case arg == "--output":
			showOutput = true
		case strings.HasPrefix(arg, "--at="):
			runAtStr = strings.TrimPrefix(arg, "--at=")
		default:
			if input == nil {
				input = []byte(arg)
			}
		}
	}
	// --output implies --watch
	if showOutput {
		watch = true
	}

	svc, nc := connectService()
	defer nc.Close()

	if runAtStr != "" {
		runAt, err := time.Parse(time.RFC3339, runAtStr)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"invalid --at time (use RFC3339): %v\n", err)
			os.Exit(1)
		}
		runID, err := svc.ScheduleRun(
			context.Background(), workflowName, input, runAt,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"schedule run: %v\n", err)
			os.Exit(1)
		}
		if jsonOutput {
			json.NewEncoder(os.Stdout).Encode(
				map[string]string{
					"run_id": runID,
					"status": "scheduled",
					"run_at": runAt.Format(time.RFC3339),
				},
			)
		} else {
			fmt.Printf("Scheduled %s (run at %s)\n",
				runID[:8], runAt.Format(time.RFC3339))
		}
		return
	}

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
		handleWatch(svc, runID, showOutput)
	}
}

// handleWatch watches a run and optionally prints output.
func handleWatch(
	svc *api.Service, runID string, showOutput bool,
) {
	if svc == nil {
		panic("handleWatch: svc must not be nil")
	}
	if runID == "" {
		panic("handleWatch: runID must not be empty")
	}

	status := watchRunWithStatus(svc, runID)
	if !showOutput {
		return
	}
	if status == dag.RunStatusCompleted {
		printRunOutputForStart(svc, runID)
	} else {
		fmt.Fprintf(os.Stderr,
			"\nNo output: run %s\n", status.String())
	}
}

// runStatusCmd retrieves and prints the status of a workflow run.
func runStatusCmd(args []string) {
	if args == nil {
		panic("runStatusCmd: args must not be nil")
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
			"Usage: dagnats run status"+
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

	run, err := svc.GetRun(context.Background(), runID)
	if err != nil {
		// Try scheduled runs.
		sr, serr := svc.GetScheduledRun(runID)
		if serr == nil {
			if jsonOutput {
				FormatJSON(os.Stdout, sr)
				return
			}
			fmt.Printf("Run:    %s\n", sr.RunID[:8])
			fmt.Printf("Status: %s\n", sr.Status)
			fmt.Printf("Run At: %s\n",
				sr.RunAt.Format(time.RFC3339))
			return
		}
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

	def, defErr := svc.GetWorkflow(run.WorkflowID)
	if defErr != nil {
		fmt.Print(FormatRunStatus(run))
		return
	}
	fmt.Print(FormatRunStatusWithDef(run, &def))
}

// runCancelResult is the JSON response for run cancel.
type runCancelResult struct {
	RunID     string `json:"run_id"`
	Cancelled bool   `json:"cancelled"`
}

// runCancelCmd publishes a workflow.cancelled event.
func runCancelCmd(args []string) {
	if args == nil {
		panic("runCancelCmd: args must not be nil")
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
			"Usage: dagnats run cancel"+
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

	err = svc.CancelRun(context.Background(), runID)
	if err != nil {
		// Try cancelling a scheduled run.
		serr := svc.CancelScheduledRun(runID)
		if serr == nil {
			if jsonOutput {
				result := runCancelResult{
					RunID: runID, Cancelled: true,
				}
				FormatJSON(os.Stdout, result)
				return
			}
			fmt.Printf("Cancelled scheduled: %s\n", runID)
			return
		}
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
	if args == nil {
		panic("runSignalCmd: args must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	hasLast := HasLastFlag(args)
	args = StripLastFlag(args)

	// With --last: need 2 args (name, payload).
	// Without --last: need 3 args (run-id, name, payload).
	if hasLast && len(args) != 2 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run signal"+
				" --last <name> <payload> [--json]")
		os.Exit(1)
	}
	if !hasLast && len(args) != 3 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run signal"+
				" <run-id> <name> <payload>"+
				" [--last] [--json]")
		os.Exit(1)
	}

	svc, nc := connectService()
	defer nc.Close()

	var rawID, name, payload string
	if hasLast {
		name = args[0]
		payload = args[1]
	} else {
		rawID = args[0]
		name = args[1]
		payload = args[2]
	}

	if name == "" {
		panic("runSignalCmd: name must not be empty")
	}

	runID, err := ResolveRunID(svc, rawID, hasLast)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve run: %v\n", err)
		os.Exit(1)
	}

	err = svc.SendSignal(
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
	var scheduled bool
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
		if arg == "--scheduled" {
			scheduled = true
		}
	}

	svc, nc := connectService()
	defer nc.Close()

	if scheduled {
		sched, err := svc.ListScheduledRuns()
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"list scheduled: %v\n", err)
			os.Exit(1)
		}
		if jsonOutput {
			if err := FormatJSON(os.Stdout, sched); err != nil {
				fmt.Fprintf(os.Stderr,
					"format json: %v\n", err)
				os.Exit(1)
			}
			return
		}
		if len(sched) == 0 {
			fmt.Println("No scheduled runs.")
			return
		}
		tw := tabwriter.NewWriter(
			os.Stdout, 0, 4, 2, ' ', 0,
		)
		fmt.Fprintln(tw,
			"RUN ID\tWORKFLOW\tRUN AT\tSTATUS")
		for _, sr := range sched {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				sr.RunID[:8],
				sr.WorkflowID,
				sr.RunAt.Format(time.RFC3339),
				sr.Status,
			)
		}
		tw.Flush()
		return
	}

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
	hasLast := HasLastFlag(args)
	args = StripLastFlag(args)

	var rawID string
	if len(args) >= 1 && !strings.HasPrefix(args[0], "--") {
		rawID = args[0]
		args = args[1:]
	} else if !hasLast {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run events <run-id>"+
				" [--last] [--full] [--type=TYPE]"+
				" [--step=STEP] [--json]")
		os.Exit(1)
	}

	fullData, typeFilter, stepFilter := parseEventFlags(args)

	svc, nc := connectService()
	defer nc.Close()

	runID, err := ResolveRunID(svc, rawID, hasLast)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve run: %v\n", err)
		os.Exit(1)
	}

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
	fmt.Fprintln(w, "TIMESTAMP\tTYPE\tSTEP\tTRACE\tDATA")

	for _, evt := range events {
		ts := evt.Timestamp.Format("2006-01-02 15:04:05")
		step := evt.StepID
		if step == "" {
			step = "-"
		}
		trace := extractTraceID(evt.TraceParent)
		if trace == "" {
			trace = "-"
		} else if len(trace) > 16 {
			trace = trace[:16]
		}
		data := evt.Data
		if data == "" {
			data = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			ts, evt.Type, step, trace, data)
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
// Delegates to FormatRunStatusWithDef with nil def (no retry info).
func FormatRunStatus(run dag.WorkflowRun) string {
	return FormatRunStatusWithDef(run, nil)
}

// FormatRunStatusWithDef renders a WorkflowRun with optional retry
// visibility. When def is non-nil, steps with retry policies show
// "attempts: 3/5" instead of "attempts: 3".
func FormatRunStatusWithDef(
	run dag.WorkflowRun, def *dag.WorkflowDef,
) string {
	if run.Steps == nil {
		panic("FormatRunStatusWithDef: Steps must not be nil")
	}
	if run.RunID == "" {
		panic("FormatRunStatusWithDef: RunID must not be empty")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Run:      %s\n", run.RunID)
	fmt.Fprintf(&b, "Workflow: %s\n", run.WorkflowID)
	fmt.Fprintf(&b, "Status:   %s\n",
		ColorStatus(run.Status.String()))
	fmt.Fprintf(&b, "Created:  %s\n",
		run.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(&b, "\nSteps:\n")

	retryMax := buildRetryMaxMap(def)
	for id, state := range run.Steps {
		maxAttempts := retryMax[id]
		fmt.Fprintf(&b, "  %s\n",
			formatStepLine(id, state, maxAttempts))
	}
	return b.String()
}

// buildRetryMaxMap extracts the total max attempts (retries + 1)
// for each step from the workflow definition. Returns empty map
// when def is nil.
func buildRetryMaxMap(def *dag.WorkflowDef) map[string]int {
	if def == nil {
		return map[string]int{}
	}
	if len(def.Steps) > 10000 {
		panic("buildRetryMaxMap: steps exceeds max bound")
	}

	result := make(map[string]int, len(def.Steps))
	for _, stepDef := range def.Steps {
		policy := dag.ResolveRetryPolicy(*def, stepDef)
		if policy != nil && policy.MaxAttempts > 0 {
			result[stepDef.ID] = policy.MaxAttempts + 1
		}
	}
	return result
}

// formatStepLine renders a single step as a human-readable line,
// including error and iteration details when present.
// When maxAttempts > 0, shows "attempts: 3/5" format.
func formatStepLine(
	id string, state dag.StepState, maxAttempts int,
) string {
	if id == "" {
		panic("formatStepLine: id must not be empty")
	}
	if state.Attempts < 0 {
		panic("formatStepLine: attempts must not be negative")
	}

	var attemptStr string
	if maxAttempts > 0 {
		attemptStr = fmt.Sprintf("attempts: %d/%d",
			state.Attempts, maxAttempts)
	} else {
		attemptStr = fmt.Sprintf("attempts: %d",
			state.Attempts)
	}

	line := fmt.Sprintf("%-20s %s (%s)",
		id, ColorStatus(state.Status.String()), attemptStr)

	if state.Iterations > 0 {
		line += fmt.Sprintf(" (iterations: %d)", state.Iterations)
	}
	if state.Status == dag.StepStatusFailed && state.Error != "" {
		line += fmt.Sprintf(" error: %s", state.Error)
	}
	return line
}

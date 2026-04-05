// cli/inspect.go
// Unified debug view: status + failed step errors + DLQ entries for a run.
// Replaces the 3-command workflow of `run status` + `run events --type` + `dlq list`.
package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// inspectResult combines all three data sources for JSON output.
type inspectResult struct {
	Run         dag.WorkflowRun  `json:"run"`
	Failures    []api.RunEvent   `json:"failures,omitempty"`
	DeadLetters []api.DeadLetter `json:"dead_letters,omitempty"`
}

// runInspectCmd prints a unified debug view for a single run.
func runInspectCmd(args []string) {
	if args == nil {
		panic("runInspectCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runInspectCmd: args exceeds max bound")
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
			"Usage: dagnats run inspect"+
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

	if jsonOutput {
		printInspectJSON(svc, ctx, run)
		return
	}

	def, defErr := svc.GetWorkflow(run.WorkflowID)
	if defErr != nil {
		fmt.Print(FormatRunStatus(run))
	} else {
		fmt.Print(FormatRunStatusWithDef(run, &def))
	}
	printFailureEvents(svc, ctx, runID)
	printRunDeadLetters(svc, ctx, runID)
}

// printInspectJSON collects all inspect data and outputs as JSON.
func printInspectJSON(
	svc *api.Service, ctx context.Context, run dag.WorkflowRun,
) {
	if svc == nil {
		panic("printInspectJSON: svc must not be nil")
	}
	if run.RunID == "" {
		panic("printInspectJSON: run.RunID must not be empty")
	}

	result := inspectResult{Run: run}

	events, err := svc.ListRunEvents(ctx, run.RunID, true)
	if err == nil {
		result.Failures = collectFailures(events)
	}

	const dlqLimit = 50
	letters, err := svc.ListDeadLetters(ctx, dlqLimit)
	if err == nil {
		result.DeadLetters = matchRunDeadLetters(
			letters, run.RunID,
		)
	}

	if err := FormatJSON(os.Stdout, result); err != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", err)
		os.Exit(1)
	}
}

// collectFailures filters events to only step.failed and
// workflow.failed types.
func collectFailures(events []api.RunEvent) []api.RunEvent {
	if len(events) > 10000 {
		panic("collectFailures: events exceeds max bound")
	}

	var failures []api.RunEvent
	for _, evt := range events {
		if evt.Type == "step.failed" ||
			evt.Type == "workflow.failed" {
			failures = append(failures, evt)
		}
	}
	return failures
}

// matchRunDeadLetters returns only dead letters matching the run ID.
func matchRunDeadLetters(
	letters []api.DeadLetter, runID string,
) []api.DeadLetter {
	if len(letters) > 10000 {
		panic("matchRunDeadLetters: letters exceeds max bound")
	}
	if runID == "" {
		panic("matchRunDeadLetters: runID must not be empty")
	}

	var matched []api.DeadLetter
	for _, l := range letters {
		if l.RunID == runID {
			matched = append(matched, l)
		}
	}
	return matched
}

// printFailureEvents prints step.failed and workflow.failed events.
func printFailureEvents(
	svc *api.Service, ctx context.Context, runID string,
) {
	if svc == nil {
		panic("printFailureEvents: svc must not be nil")
	}
	if runID == "" {
		panic("printFailureEvents: runID must not be empty")
	}

	events, err := svc.ListRunEvents(ctx, runID, true)
	if err != nil {
		return
	}

	failures := filterRunEvents(events, "", "")
	filtered := failures[:0]
	for _, evt := range failures {
		if evt.Type == "step.failed" ||
			evt.Type == "workflow.failed" {
			filtered = append(filtered, evt)
		}
	}

	if len(filtered) == 0 {
		return
	}

	fmt.Println("\nFailures:")
	for _, evt := range filtered {
		step := evt.StepID
		if step == "" {
			step = "-"
		}
		ts := evt.Timestamp.Format("15:04:05")
		fmt.Printf("  %s  %-24s %s\n",
			ts, ColorRed(evt.Type), step)
		traceID := extractTraceID(evt.TraceParent)
		if traceID != "" {
			fmt.Printf("          trace: %s\n", traceID)
		}
		if evt.Data != "" && evt.Data != "-" {
			fmt.Printf("          %s\n", evt.Data)
		}
	}
}

// printRunDeadLetters prints DLQ entries matching the given run.
func printRunDeadLetters(
	svc *api.Service, ctx context.Context, runID string,
) {
	if svc == nil {
		panic("printRunDeadLetters: svc must not be nil")
	}
	if runID == "" {
		panic("printRunDeadLetters: runID must not be empty")
	}

	const dlqLimit = 50
	letters, err := svc.ListDeadLetters(ctx, dlqLimit)
	if err != nil {
		return
	}

	matched := letters[:0]
	for _, l := range letters {
		if l.RunID == runID {
			matched = append(matched, l)
		}
	}

	if len(matched) == 0 {
		return
	}

	fmt.Println("\nDead Letters:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  SEQ\tTASK\tSTEP\tERROR")
	for _, dl := range matched {
		fmt.Fprintf(w, "  %d\t%s\t%s\t%s\n",
			dl.Sequence, dl.Task, dl.StepID, dl.Error)
	}
	w.Flush()
}

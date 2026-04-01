// cli/inspect.go
// Unified debug view: status + failed step errors + DLQ entries for a run.
// Replaces the 3-command workflow of `run status` + `run events --type` + `dlq list`.
package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/danmestas/dagnats/api"
)

// runInspectCmd prints a unified debug view for a single run.
func runInspectCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run inspect <run-id>")
		os.Exit(1)
	}
	runID := args[0]
	if runID == "" {
		panic("runInspectCmd: runID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	ctx := context.Background()

	// Section 1: run status with step errors
	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(FormatRunStatus(run))

	// Section 2: failure events
	printFailureEvents(svc, ctx, runID)

	// Section 3: dead-letter entries for this run
	printRunDeadLetters(svc, ctx, runID)
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
		fmt.Printf("  %s  %-24s %s\n", ts, ColorRed(evt.Type), step)
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

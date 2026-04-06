// cli/inspect.go
// Unified debug view: status + failed step errors + DLQ entries for a run.
// Replaces the 3-command workflow of `run status` + `run events --type`
// + `dlq list`. Cross-references failures and DLQ inline under steps.
package cli

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// inspectResult combines all three data sources for JSON output.
type inspectResult struct {
	Run         dag.WorkflowRun  `json:"run"`
	Failures    []api.RunEvent   `json:"failures,omitempty"`
	DeadLetters []api.DeadLetter `json:"dead_letters,omitempty"`
}

// stepDebugContext collects failure events and DLQ entries for a
// single step, enabling inline cross-referenced output.
type stepDebugContext struct {
	Failures    []api.RunEvent
	DeadLetters []api.DeadLetter
	TraceID     string
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

	failures := fetchFailures(svc, ctx, runID)
	deadLetters := fetchDeadLetters(svc, ctx, runID)

	def, defErr := svc.GetWorkflow(run.WorkflowID)
	var defPtr *dag.WorkflowDef
	if defErr == nil {
		defPtr = &def
	}

	printRunHeader(run)
	contexts := collectStepContexts(failures, deadLetters)
	retryMax := buildRetryMaxMap(defPtr)
	printStepsWithContext(run, retryMax, contexts)
}

// printRunHeader prints the run metadata lines (ID, workflow,
// status, created).
func printRunHeader(run dag.WorkflowRun) {
	if run.RunID == "" {
		panic("printRunHeader: RunID must not be empty")
	}
	if run.WorkflowID == "" {
		panic("printRunHeader: WorkflowID must not be empty")
	}

	fmt.Printf("Run:      %s\n", run.RunID)
	fmt.Printf("Workflow: %s\n", run.WorkflowID)
	fmt.Printf("Status:   %s\n",
		ColorStatus(run.Status.String()))
	fmt.Printf("Created:  %s\n",
		run.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
}

// fetchFailures retrieves step.failed events for a run. Returns
// nil on error (non-fatal for display).
func fetchFailures(
	svc *api.Service, ctx context.Context, runID string,
) []api.RunEvent {
	if svc == nil {
		panic("fetchFailures: svc must not be nil")
	}
	if runID == "" {
		panic("fetchFailures: runID must not be empty")
	}

	events, err := svc.ListRunEvents(ctx, runID, true)
	if err != nil {
		return nil
	}
	return collectFailures(events)
}

// fetchDeadLetters retrieves DLQ entries matching a run. Returns
// nil on error (non-fatal for display).
func fetchDeadLetters(
	svc *api.Service, ctx context.Context, runID string,
) []api.DeadLetter {
	if svc == nil {
		panic("fetchDeadLetters: svc must not be nil")
	}
	if runID == "" {
		panic("fetchDeadLetters: runID must not be empty")
	}

	const dlqLimit = 50
	letters, err := svc.ListDeadLetters(ctx, dlqLimit)
	if err != nil {
		return nil
	}
	return matchRunDeadLetters(letters, runID)
}

// collectStepContexts groups failures and DLQ entries by step ID.
func collectStepContexts(
	failures []api.RunEvent,
	deadLetters []api.DeadLetter,
) map[string]stepDebugContext {
	if len(failures) > 10000 {
		panic("collectStepContexts: failures exceeds max")
	}
	if len(deadLetters) > 10000 {
		panic("collectStepContexts: deadLetters exceeds max")
	}

	result := make(map[string]stepDebugContext)
	for _, f := range failures {
		ctx := result[f.StepID]
		ctx.Failures = append(ctx.Failures, f)
		traceID := extractTraceID(f.TraceParent)
		if traceID != "" {
			ctx.TraceID = traceID
		}
		result[f.StepID] = ctx
	}
	for _, dl := range deadLetters {
		ctx := result[dl.StepID]
		ctx.DeadLetters = append(ctx.DeadLetters, dl)
		result[dl.StepID] = ctx
	}
	return result
}

// printStepsWithContext prints sorted steps with inline failure
// events, trace hints, and DLQ entries under failed steps.
func printStepsWithContext(
	run dag.WorkflowRun,
	retryMax map[string]int,
	contexts map[string]stepDebugContext,
) {
	if run.Steps == nil {
		panic("printStepsWithContext: Steps must not be nil")
	}
	if retryMax == nil {
		panic("printStepsWithContext: retryMax must not be nil")
	}

	stepIDs := sortedStepIDs(run.Steps)
	fmt.Printf("\nSteps:\n")
	for _, id := range stepIDs {
		state := run.Steps[id]
		maxAttempts := retryMax[id]
		fmt.Printf("  %s\n",
			formatStepLine(id, state, maxAttempts))
		printStepDebugLines(contexts[id])
	}
}

// sortedStepIDs returns step IDs sorted alphabetically.
func sortedStepIDs(
	steps map[string]dag.StepState,
) []string {
	if len(steps) > 10000 {
		panic("sortedStepIDs: steps exceeds max bound")
	}
	if steps == nil {
		panic("sortedStepIDs: steps must not be nil")
	}

	ids := make([]string, 0, len(steps))
	for id := range steps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// printStepDebugLines prints indented failure events, trace hints,
// and DLQ entries for a single step.
func printStepDebugLines(ctx stepDebugContext) {
	const maxFailures = 10000
	if len(ctx.Failures) > maxFailures {
		panic("printStepDebugLines: failures exceeds max")
	}
	if len(ctx.DeadLetters) > maxFailures {
		panic("printStepDebugLines: deadLetters exceeds max")
	}

	for _, f := range ctx.Failures {
		ts := f.Timestamp.Format("15:04:05")
		data := f.Data
		if data == "" || data == "-" {
			data = ""
		}
		if data != "" {
			fmt.Printf("    %s  %s  %s\n", ts, f.Type, data)
		} else {
			fmt.Printf("    %s  %s\n", ts, f.Type)
		}
	}
	if ctx.TraceID != "" {
		fmt.Printf("    trace: %s\n", ctx.TraceID)
		fmt.Printf("    view:  dagnats trace %s\n", ctx.TraceID)
	}
	for _, dl := range ctx.DeadLetters {
		fmt.Printf("    DLQ #%d: %s\n", dl.Sequence, dl.Error)
		fmt.Printf("    replay: dagnats dlq replay %d\n",
			dl.Sequence)
	}
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

// matchRunDeadLetters returns only dead letters matching the run.
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

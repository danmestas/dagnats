// cli/inspect.go
// Unified debug view: status + failures + DLQ + optional trace tree.
// One command for the full debug picture of a run.
package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/observe/spanread"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// inspectResult combines all data sources for JSON output.
type inspectResult struct {
	Run         dag.WorkflowRun      `json:"run"`
	Failures    []api.RunEvent       `json:"failures,omitempty"`
	DeadLetters []api.DeadLetterView `json:"dead_letters,omitempty"`
	Spans       []inspectSpan        `json:"spans,omitempty"`
}

// inspectSpan is a simplified span for JSON output.
type inspectSpan struct {
	TraceID    string `json:"trace_id"`
	SpanID     string `json:"span_id"`
	ParentID   string `json:"parent_span_id,omitempty"`
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
	Status     string `json:"status"`
}

// stepDebugContext collects failure events and DLQ entries for a
// single step, enabling inline cross-referenced output.
type stepDebugContext struct {
	Failures    []api.RunEvent
	DeadLetters []api.DeadLetterView
	TraceID     string
}

// inspectData holds everything needed to render inspect output.
// Gathered once, rendered by either human-readable or JSON path.
type inspectData struct {
	Run         dag.WorkflowRun
	Def         *dag.WorkflowDef
	Failures    []api.RunEvent
	DeadLetters []api.DeadLetterView
	Spans       []*tracepb.Span
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
	showTrace := hasFlag(args, "--trace")
	args = stripFlag(args, "--trace")

	var rawID string
	if len(args) == 1 {
		rawID = args[0]
	} else if !hasLast {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run inspect"+
				" <run-id> [--last] [--trace] [--json]")
		os.Exit(1)
	}

	svc, nc := connectService()
	defer nc.Close()

	runID, err := ResolveRunID(svc, rawID, hasLast)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve run: %v\n", err)
		os.Exit(1)
	}

	data := gatherInspectData(svc, nc, runID, showTrace)

	if jsonOutput {
		renderInspectJSON(data)
	} else {
		renderInspectHuman(data)
	}
}

// gatherInspectData collects all data sources for a run into a
// single struct. Both renderers consume the same data — no
// divergence possible.
func gatherInspectData(
	svc *api.Service, nc *nats.Conn,
	runID string, showTrace bool,
) inspectData {
	if svc == nil {
		panic("gatherInspectData: svc must not be nil")
	}
	if runID == "" {
		panic("gatherInspectData: runID must not be empty")
	}

	ctx := context.Background()

	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		os.Exit(1)
	}

	d := inspectData{
		Run:         run,
		Failures:    fetchFailures(svc, ctx, runID),
		DeadLetters: fetchDeadLetters(svc, ctx, runID),
	}

	def, defErr := svc.GetWorkflow(run.WorkflowID)
	if defErr == nil {
		d.Def = &def
	}

	if showTrace {
		// Empty non-nil slice signals "trace requested but
		// nothing found" to renderers.
		d.Spans = []*tracepb.Span{}
		js, jsErr := jetstream.New(nc)
		if jsErr == nil {
			traceCtx, cancel := context.WithTimeout(
				ctx, 5*time.Second,
			)
			spans, spansErr := spanread.CollectRunSpans(
				traceCtx, js, runID, spanread.MaxSpans,
			)
			cancel()
			if spansErr == nil && len(spans) > 0 {
				d.Spans = spans
			}
		}
	}

	return d
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
) []api.DeadLetterView {
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
	deadLetters []api.DeadLetterView,
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

// renderInspectHuman prints the human-readable inspect output.
func renderInspectHuman(d inspectData) {
	if d.Run.RunID == "" {
		panic("renderInspectHuman: RunID must not be empty")
	}
	if d.Run.Steps == nil {
		panic("renderInspectHuman: Steps must not be nil")
	}

	printRunHeader(d.Run)
	contexts := collectStepContexts(d.Failures, d.DeadLetters)
	retryMax := buildRetryMaxMap(d.Def)
	printStepsWithContext(d.Run, retryMax, contexts)

	if len(d.Spans) > 0 {
		fmt.Println()
		printSpanTrees(d.Spans)
	} else if d.Spans != nil {
		fmt.Println("\nTrace: no spans found")
	}
}

// renderInspectJSON outputs all inspect data as JSON.
func renderInspectJSON(d inspectData) {
	if d.Run.RunID == "" {
		panic("renderInspectJSON: RunID must not be empty")
	}

	result := inspectResult{
		Run:         d.Run,
		Failures:    d.Failures,
		DeadLetters: d.DeadLetters,
		Spans:       convertSpans(d.Spans),
	}

	if err := FormatJSON(os.Stdout, result); err != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", err)
		os.Exit(1)
	}
}

// convertSpans maps protobuf spans to the JSON-friendly format.
// Returns nil when input is nil (preserves omitempty behavior).
func convertSpans(
	spans []*tracepb.Span,
) []inspectSpan {
	if spans == nil {
		return nil
	}
	const maxSpans = 10000
	if len(spans) > maxSpans {
		panic("convertSpans: spans exceeds max bound")
	}

	result := make([]inspectSpan, 0, len(spans))
	for _, sp := range spans {
		result = append(result, inspectSpan{
			TraceID:    spanread.HexTraceID(sp),
			SpanID:     spanread.HexSpanID(sp),
			ParentID:   spanread.HexParentID(sp),
			Name:       sp.Name,
			DurationMs: spanread.DurationMs(sp),
			Status:     spanread.StatusLabel(sp),
		})
	}
	return result
}

// hasFlag returns true if args contains the given flag.
func hasFlag(args []string, flag string) bool {
	if args == nil {
		panic("hasFlag: args must not be nil")
	}
	if flag == "" {
		panic("hasFlag: flag must not be empty")
	}
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

// stripFlag returns a copy of args with the given flag removed.
func stripFlag(args []string, flag string) []string {
	if args == nil {
		panic("stripFlag: args must not be nil")
	}
	if flag == "" {
		panic("stripFlag: flag must not be empty")
	}
	result := make([]string, 0, len(args))
	for _, a := range args {
		if a != flag {
			result = append(result, a)
		}
	}
	return result
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
	letters []api.DeadLetterView, runID string,
) []api.DeadLetterView {
	if len(letters) > 10000 {
		panic("matchRunDeadLetters: letters exceeds max bound")
	}
	if runID == "" {
		panic("matchRunDeadLetters: runID must not be empty")
	}

	var matched []api.DeadLetterView
	for _, l := range letters {
		if l.RunID == runID {
			matched = append(matched, l)
		}
	}
	return matched
}

// traces_page.go
// Cross-run Traces page. There is one trace per run (W3C traceparent on
// the run's first history event), so the trace LIST is the runs the
// console already reads, projected to one row per run keyed on the run id
// — no per-run span scan and no blocking per-run trace-id fetch on the
// list path. The per-trace DETAIL view reuses the run's span tree
// (GetRunTrace -> the shared trace-tree component) and reads the header
// trace id from a single first-event lookup. The web counterpart of the
// run-detail Trace tab, surfaced as a standalone cross-run section.
package console

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// tracesListMax bounds the projected row count so a runaway run list
// can't unbound the page render.
const tracesListMax = 500

// TraceListRow is one row on /console/traces: one run that may carry a
// trace. The run id is the join key the detail page resolves; Workflow
// stands in for the mockup's "root operation" (backed — it is the run's
// WorkflowID). StepCount is the run's step count (honest: the run model's
// steps, not a post-hoc span count). Started is the run's CreatedAt.
type TraceListRow struct {
	RunID      string
	RunIDShort string
	WorkflowID string
	TraceID    string
	Status     string
	StepCount  int
	Started    string
	Duration   string
}

// TracesListView powers /console/traces. StatusFilter / TraceIDFilter
// echo the active query so the filter controls stay populated across
// reloads. TraceIDLookup is the trace id the operator arrived with (e.g.
// from a Logs trace-id link); the page surfaces it as context — the list
// rows themselves are keyed on run id, so this is a label, not a filter.
type TracesListView struct {
	Header        PageHeader
	Rows          []TraceListRow
	StatusFilter  string
	TraceIDLookup string
}

// TraceDetailView powers /console/traces/<runID>. RunID is the route key
// and the join key to the run; TraceID is the W3C trace id read from the
// run's first history event (empty when the run carries no trace
// context). Rows is the flattened span tree the shared trace-tree
// component renders; Note surfaces a degraded read.
type TraceDetailView struct {
	RunID      string
	RunIDShort string
	WorkflowID string
	Status     string
	TraceID    string
	Rows       []TraceRow
	Note       string
}

// servePageTracesList renders /console/traces. The list is the runs the
// console already reads, projected to one row per run. status filters by
// run status (reusing the run-status vocabulary); trace_id is carried
// through as a lookup label (true trace-id -> run resolution needs a
// derived index — see the package note).
func servePageTracesList(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageTracesList: w is nil")
	}
	if r == nil {
		panic("servePageTracesList: r is nil")
	}
	ds, ok := requirePort[RunStore](w, cfg, "traces-list")
	if !ok {
		return
	}
	q := r.URL.Query()
	statusFilter := strings.TrimSpace(q.Get("status"))
	traceLookup := strings.TrimSpace(q.Get("trace_id"))
	runs, err := ds.ListRuns(r.Context(), "")
	if err != nil {
		cfg.Logger.Error("console: traces list", "err", err)
		http.Error(w, "list traces failed", http.StatusInternalServerError)
		return
	}
	view := TracesListView{
		Header: PageHeader{
			Title: "Traces",
			Subtitle: "One trace per run (W3C traceparent). " +
				"Click a row for the span tree.",
		},
		Rows:          traceRowsFromRuns(runs, statusFilter),
		StatusFilter:  statusFilter,
		TraceIDLookup: traceLookup,
	}
	renderPage(w, r, ts, cfg, "traces-list", pageData{
		Title:   "Traces",
		Section: "traces",
		Page:    view,
	})
}

// traceRowsFromRuns projects runs into trace rows, optionally filtered by
// run status. Bounded by tracesListMax. Pure so the projection is unit-
// testable without a handler.
func traceRowsFromRuns(
	runs []dag.WorkflowRun, statusFilter string,
) []TraceListRow {
	rows := make([]TraceListRow, 0, len(runs))
	for i := 0; i < len(runs) && i < tracesListMax; i++ {
		run := runs[i]
		statusStr := run.Status.String()
		if statusFilter != "" && statusStr != statusFilter {
			continue
		}
		rows = append(rows, TraceListRow{
			RunID:      run.RunID,
			RunIDShort: shortRunID(run.RunID),
			WorkflowID: run.WorkflowID,
			TraceID:    parseW3CTraceID(run.TraceParent),
			Status:     statusStr,
			StepCount:  len(run.Steps),
			Started:    run.CreatedAt.UTC().Format(time.RFC3339),
			Duration:   formatRunDuration(run),
		})
	}
	return rows
}

// formatRunDuration returns the run's completed-minus-created span rounded
// to the millisecond, or "" when the run has not completed. An in-flight
// run (CompletedAt nil) reports no duration rather than a fabricated "0s".
func formatRunDuration(run dag.WorkflowRun) string {
	if run.CompletedAt == nil {
		return ""
	}
	return run.CompletedAt.Sub(run.CreatedAt).Round(time.Millisecond).String()
}

// dispatchTraces splits /console/traces/<runID> into the detail view.
// A bare or multi-segment path 404s (mirror dispatchRuns).
func dispatchTraces(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchTraces: w is nil")
	}
	if r == nil {
		panic("dispatchTraces: r is nil")
	}
	rest := strings.TrimPrefix(r.URL.Path, "/console/traces/")
	if rest == "" || strings.Contains(rest, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	servePageTraceDetail(w, r, ts, cfg, rest)
}

// servePageTraceDetail renders /console/traces/<runID>. It reuses the
// same span read the run-detail Trace tab uses (GetRunTrace) and reads
// the header trace id from the run's first history event. A trace read
// error degrades to an empty tree plus a Note rather than a 500, so the
// page never lies about telemetry it couldn't load.
func servePageTraceDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, runID string,
) {
	if w == nil {
		panic("servePageTraceDetail: w is nil")
	}
	if runID == "" {
		panic("servePageTraceDetail: runID is empty")
	}
	ds, ok := requirePort[RunStore](w, cfg, "trace-detail")
	if !ok {
		return
	}
	view := buildTraceDetailView(r.Context(), ds, runID, cfg)
	renderPage(w, r, ts, cfg, "trace-detail", pageData{
		Title:   "Trace",
		Section: "traces",
		Page:    view,
	})
}

// buildTraceDetailView assembles the detail view from the run snapshot
// (workflow + status for the header), the flattened span tree, and the
// first-event trace id. Each read degrades independently to an honest
// empty/dash rather than failing the whole page.
func buildTraceDetailView(
	ctx context.Context, ds RunStore, runID string, cfg Config,
) TraceDetailView {
	if ctx == nil {
		panic("buildTraceDetailView: ctx is nil")
	}
	if runID == "" {
		panic("buildTraceDetailView: runID is empty")
	}
	view := TraceDetailView{RunID: runID, RunIDShort: shortRunID(runID)}
	if run, err := ds.GetRun(ctx, runID); err == nil {
		view.WorkflowID = run.WorkflowID
		view.Status = run.Status.String()
	}
	view.TraceID = traceIDForRun(ctx, ds, runID, cfg)
	rows, err := ds.GetRunTrace(ctx, runID)
	if err != nil {
		cfg.Logger.Warn("console: trace detail", "run", runID, "err", err)
		view.Rows = nil
		view.Note = "Trace read failed; showing no spans."
		return view
	}
	view.Rows = rows
	return view
}

// traceIDForRun reads the run's first history event and parses the W3C
// trace id from its traceparent. Returns "" on any read miss — the
// header then shows the run id only, never a fabricated trace id. This is
// the single cheap per-detail lookup the Ousterhout review blessed; it is
// NOT done on the list path.
func traceIDForRun(
	ctx context.Context, ds RunStore, runID string, cfg Config,
) string {
	if ctx == nil {
		panic("traceIDForRun: ctx is nil")
	}
	if runID == "" {
		panic("traceIDForRun: runID is empty")
	}
	events, err := ds.ListRunEvents(ctx, runID, false)
	if err != nil {
		cfg.Logger.Warn("console: trace id lookup", "run", runID, "err", err)
		return ""
	}
	for i := 0; i < len(events); i++ {
		if id := parseW3CTraceID(events[i].TraceParent); id != "" {
			return id
		}
	}
	return ""
}

// parseW3CTraceID extracts the 32-hex trace id from a W3C traceparent
// ("00-{traceID}-{spanID}-{flags}"). Returns "" when empty or malformed.
// Mirrors the api package's parser; kept console-side so the read path
// needs no new engine surface.
func parseW3CTraceID(traceparent string) string {
	if traceparent == "" {
		return ""
	}
	if len(traceparent) > 256 {
		return ""
	}
	parts := strings.SplitN(traceparent, "-", 5)
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}

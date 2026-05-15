package console

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/starfederation/datastar-go/datastar"
)

// streams.go owns the two SSE endpoints PR 3 introduces:
//
//   GET /console/sse/runs         — list-page live update.
//   GET /console/sse/runs/<id>    — per-run-detail live update.
//
// Both handlers run for the lifetime of the client connection,
// pumping updates from a DataSource watcher into Datastar
// PatchElements events. The handlers are bound to r.Context().Done(),
// so disconnecting the client triggers prompt goroutine cleanup —
// the same proof shape the heartbeat handler established in PR 1.
//
// Test the handlers with a captured response writer + a producer
// goroutine creating runs / publishing events after the handler is
// installed; see streams_test.go for the working pattern.

// serveSSERuns streams updates from the workflow_runs KV bucket as
// Datastar PatchElements events targeting #runs-tbody. Filter signals
// from the page (workflow / status / range) gate which updates the
// operator sees. New runs prepend to the table head; status mutations
// replace the existing row by id.
func serveSSERuns(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSSERuns: w is nil")
	}
	if r == nil {
		panic("serveSSERuns: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ds, ok := requireData(w, cfg, "sse-runs")
	if !ok {
		return
	}
	filter := readRunsFilter(r)
	ch, err := ds.WatchRuns(r.Context())
	if err != nil {
		cfg.Logger.Error("console: sse runs watch", "err", err)
		http.Error(w, "watch failed", http.StatusServiceUnavailable)
		return
	}
	sse := datastar.NewSSE(w, r)
	pumpRunUpdates(r.Context(), sse, ts.base, ch, filter, cfg)
}

// runsFilter is the subset of fragmentEnvelope this SSE endpoint
// actually consults. Kept tiny so the per-update gate is O(1).
type runsFilter struct {
	Workflow string
	Status   string
}

// readRunsFilter pulls the signal envelope off r (best-effort). When
// no signals arrive the filter is open (matches everything). The
// "range" signal is intentionally absent — a live SSE stream is by
// definition recent, so a range filter on the list page is irrelevant
// for incoming events. The page-render side still honours it.
func readRunsFilter(r *http.Request) runsFilter {
	if r == nil {
		panic("readRunsFilter: r is nil")
	}
	env, ok := readSignalsBestEffort(r)
	if !ok {
		return runsFilter{}
	}
	return runsFilter{
		Workflow: env.RunsWorkflow,
		Status:   env.RunsStatus,
	}
}

// pumpRunUpdates is the inner loop that translates RunUpdate values
// into Datastar PatchElements events. Bounded loop, exits when ctx
// done or the channel closes. The outer handler owns the SSE writer
// and the watcher; here we just translate.
func pumpRunUpdates(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template,
	ch <-chan RunUpdate, filter runsFilter, cfg Config,
) {
	if sse == nil {
		panic("pumpRunUpdates: sse is nil")
	}
	if tmpl == nil {
		panic("pumpRunUpdates: tmpl is nil")
	}
	const maxIters = 1_000_000_000
	for i := 0; i < maxIters; i++ {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-ch:
			if !ok {
				return
			}
			if !runMatchesFilter(update.Run, filter) {
				continue
			}
			if err := emitRunPatch(sse, tmpl, update); err != nil {
				cfg.Logger.Warn("console: sse runs emit",
					"run_id", update.Run.RunID, "err", err)
				return
			}
		}
	}
}

// runMatchesFilter applies the operator's filter signals to one run.
// Empty filter values pass; any mismatch rejects. The dropped event
// is silently skipped — the operator sees no flicker for runs that
// don't match.
func runMatchesFilter(run dag.WorkflowRun, f runsFilter) bool {
	if f.Workflow != "" && run.WorkflowID != f.Workflow {
		return false
	}
	if f.Status != "" && f.Status != "any" &&
		run.Status.String() != f.Status {
		return false
	}
	return true
}

// emitRunPatch renders the single-row template and patches it into
// the tbody. To handle the three browser-side cases uniformly
// (page didn't include this id, page included this id, page included
// stale row for this id), we emit TWO patches: a remove targeting
// the row's id selector (no-op if absent), followed by a prepend into
// the tbody. The result is "this row, on top, fresh content" in every
// case.
//
// The single-write alternative — outer-mode replace by id — fails
// silently for rows the page never rendered, which is the common case
// for "live new run while operator was watching the list".
func emitRunPatch(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, update RunUpdate,
) error {
	if sse == nil {
		panic("emitRunPatch: sse is nil")
	}
	if tmpl == nil {
		panic("emitRunPatch: tmpl is nil")
	}
	row := runRowFromRun(update.Run)
	html, err := renderFragment(tmpl, "run-row", rowPatch{
		Row:    row,
		Fresh:  true, // always highlight; the row is freshly placed.
		PutSeq: update.Seq,
	})
	if err != nil {
		return fmt.Errorf("render run-row: %w", err)
	}
	// First: remove the row if it exists.
	rmOpts := []datastar.PatchElementOption{
		datastar.WithSelector("#run-row-" + row.RunID),
		datastar.WithMode(datastar.ElementPatchModeRemove),
	}
	// Datastar's remove with a missing selector logs a warning but
	// doesn't error. Ignore the warning; the second patch is what
	// matters.
	_ = sse.PatchElements("", rmOpts...)
	// Then: prepend the fresh row to the tbody.
	prependOpts := []datastar.PatchElementOption{
		datastar.WithSelector("#runs-tbody"),
		datastar.WithMode(datastar.ElementPatchModePrepend),
		datastar.WithPatchElementsEventID(
			strconv.FormatUint(update.Seq, 10)),
	}
	if err := sse.PatchElements(html, prependOpts...); err != nil {
		return fmt.Errorf("patch run row: %w", err)
	}
	return nil
}

// rowPatch is the template binding for fragments/run_row.html. The
// Fresh flag toggles the entry-animation class so a brand-new row
// gets the highlight; updates skip it so the row's previous animation
// state doesn't loop.
type rowPatch struct {
	Row    RunRow
	Fresh  bool
	PutSeq uint64
}

// serveSSERunDetail streams events for one run. Two emission targets:
//   - #run-detail-events tbody — append rows as new history arrives.
//   - #run-detail-steps — replace the per-step card when a step.* event
//     names the step.
//
// Run-terminal events also patch the header status badge so the
// operator sees the run wrap up without refreshing.
func serveSSERunDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSSERunDetail: w is nil")
	}
	if r == nil {
		panic("serveSSERunDetail: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/console/sse/runs/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	ds, ok := requireData(w, cfg, "sse-run-detail")
	if !ok {
		return
	}
	from := parseLastEventID(r.Header.Get("Last-Event-ID"))
	ch, err := ds.WatchRunHistory(r.Context(), id, from)
	if err != nil {
		cfg.Logger.Error("console: sse run-detail watch",
			"run_id", id, "err", err)
		http.Error(w, "watch failed", http.StatusServiceUnavailable)
		return
	}
	sse := datastar.NewSSE(w, r)
	def, _ := ds.GetWorkflow(getRunWorkflowID(r.Context(), ds, id))
	pumpHistory(r.Context(), sse, ts.base, ch, def, cfg)
}

// parseLastEventID interprets the Last-Event-ID header. The header
// holds the JetStream stream sequence the client last received; we
// resume from that sequence + 1 (the watcher applies the +1). An
// unparseable header is treated as 0 — replay-from-the-start, which
// is correct for first connections that don't send the header.
func parseLastEventID(h string) uint64 {
	if h == "" {
		return 0
	}
	v, err := strconv.ParseUint(h, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// getRunWorkflowID is a best-effort lookup that the per-step patches
// rely on — the workflow definition's step set determines which cards
// the run-detail page has rendered. If the run isn't yet readable
// (race with the publisher), we return empty and step patches no-op
// gracefully.
func getRunWorkflowID(
	ctx context.Context, ds DataSource, runID string,
) string {
	if ctx == nil {
		panic("getRunWorkflowID: ctx is nil")
	}
	if ds == nil {
		panic("getRunWorkflowID: ds is nil")
	}
	run, err := ds.GetRun(ctx, runID)
	if err != nil {
		return ""
	}
	return run.WorkflowID
}

// pumpHistory translates history events into Datastar patches. Steps
// patches go to #step-card-<id>; event rows append to #run-events-body.
func pumpHistory(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, ch <-chan HistoryEvent,
	def dag.WorkflowDef, cfg Config,
) {
	if sse == nil {
		panic("pumpHistory: sse is nil")
	}
	if tmpl == nil {
		panic("pumpHistory: tmpl is nil")
	}
	const maxIters = 1_000_000_000
	for i := 0; i < maxIters; i++ {
		select {
		case <-ctx.Done():
			return
		case he, ok := <-ch:
			if !ok {
				return
			}
			if err := emitHistoryPatch(sse, tmpl, he, def, i); err != nil {
				cfg.Logger.Warn("console: sse run-detail emit",
					"err", err)
				return
			}
		}
	}
}

// emitHistoryPatch writes the event-row append and (when the event
// carries a step id) a step-card replace. Two patches per event is
// fine; Datastar processes them in order and the browser renders
// without flicker.
func emitHistoryPatch(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, he HistoryEvent,
	def dag.WorkflowDef, idx int,
) error {
	if err := emitEventRowPatch(sse, tmpl, he, idx); err != nil {
		return err
	}
	if he.Event.StepID == "" {
		return nil
	}
	return emitStepCardPatch(sse, tmpl, he, def)
}

// emitEventRowPatch appends one history row.
func emitEventRowPatch(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, he HistoryEvent, idx int,
) error {
	row := eventRowFromHistory(he, idx)
	html, err := renderFragment(tmpl, "run-event-row", row)
	if err != nil {
		return fmt.Errorf("render run-event-row: %w", err)
	}
	opts := []datastar.PatchElementOption{
		datastar.WithSelector("#run-events-body"),
		datastar.WithMode(datastar.ElementPatchModeAppend),
		datastar.WithPatchElementsEventID(
			strconv.FormatUint(he.Seq, 10)),
	}
	return sse.PatchElements(html, opts...)
}

// emitStepCardPatch replaces #step-card-<id> with a fresh card.
// Looks up the step in def so the card title / position match the
// page's static render. If the step isn't in def (engine emitted an
// event for a step we don't know about), we silently no-op.
func emitStepCardPatch(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, he HistoryEvent,
	def dag.WorkflowDef,
) error {
	stepID := he.Event.StepID
	if stepID == "" {
		return nil
	}
	if !defHasStep(def, stepID) {
		return nil
	}
	card := stepCardFromEvent(he, stepID)
	html, err := renderFragment(tmpl, "run-step-card", card)
	if err != nil {
		return fmt.Errorf("render run-step-card: %w", err)
	}
	opts := []datastar.PatchElementOption{
		datastar.WithPatchElementsEventID(
			strconv.FormatUint(he.Seq, 10)),
	}
	return sse.PatchElements(html, opts...)
}

// defHasStep tests for membership without an allocation. ≤70 lines
// rule satisfied; pulled out so emitStepCardPatch stays small.
func defHasStep(def dag.WorkflowDef, stepID string) bool {
	if stepID == "" {
		return false
	}
	for _, s := range def.Steps {
		if s.ID == stepID {
			return true
		}
	}
	return false
}

// stepCardFromEvent infers a card render from one event type. The
// engine emits step.dispatched / step.completed / step.failed; we
// translate to the matching status badge. For ambiguous events
// (step.skipped etc.) we render a "pending" card — the next page
// reload will reconcile to the correct snapshot.
func stepCardFromEvent(he HistoryEvent, stepID string) StepCard {
	status := stepStatusFromEvent(he.Event.Type)
	card := StepCard{
		ID:     stepID,
		Status: status,
		Icon:   statusIcon(status),
	}
	if status == "failed" {
		card.HasError = true
		card.ErrorMsg = he.Event.Data
	}
	return card
}

// stepStatusFromEvent picks a status string for a step.* event type.
// Default "pending" is the safe fallback for unknown / non-step events.
func stepStatusFromEvent(typ string) string {
	switch typ {
	case "step.dispatched", "step.started":
		return "running"
	case "step.completed":
		return "completed"
	case "step.failed":
		return "failed"
	case "step.skipped":
		return "skipped"
	}
	return "pending"
}

// eventRowFromHistory projects a HistoryEvent into the EventRow shape
// the run-event-row template binds. idx is used as a stable signal
// key and the visible counter on the timeline; in steady state it
// counts only the live-stream events appended after the initial render.
func eventRowFromHistory(he HistoryEvent, idx int) EventRow {
	const previewMax = 200
	preview := he.Event.Data
	if len(preview) > previewMax {
		preview = preview[:previewMax]
	}
	return EventRow{
		Index:       idx,
		Timestamp:   he.Event.Timestamp.UTC().Format("2006-01-02T15:04:05Z"),
		Type:        he.Event.Type,
		StepID:      he.Event.StepID,
		DataPreview: preview,
		DataFull:    he.Event.Data,
	}
}

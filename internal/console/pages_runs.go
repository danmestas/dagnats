package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/starfederation/datastar-go/datastar"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// RunRow is one row in the runs list / recent-runs panel.
type RunRow struct {
	RunID       string
	RunIDShort  string
	WorkflowID  string
	Status      string
	StatusIcon  string
	TriggerKind string
	StartedAt   string
	Duration    string
}

// servePageRunsList renders /console/runs.
func servePageRunsList(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageRunsList: w is nil")
	}
	if r == nil {
		panic("servePageRunsList: r is nil")
	}
	ds, ok := requireData(w, cfg, "runs-list")
	if !ok {
		return
	}
	view, err := buildRunsView(r.Context(), ds, r.URL.Query())
	if err != nil {
		cfg.Logger.Error("console: runs list", "err", err)
		http.Error(w, "list runs failed", http.StatusInternalServerError)
		return
	}
	if view.EmptyState != nil {
		view.EmptyState.ReadOnly = cfg.ReadOnly
	}
	renderPage(w, r, ts, cfg, "runs-list", pageData{
		Title:   "Runs",
		Section: "runs",
		Page:    view,
	})
}

// RunsListView powers /console/runs.
type RunsListView struct {
	Header   PageHeader
	Workflow string
	Status   string
	// IDFilter is the case-insensitive run-id substring the find-by-id
	// box narrows the table on. Empty means no id filter. The template
	// echoes it back into the input so the box stays populated after a
	// GET round-trip.
	IDFilter string
	Range    string
	// SinceUnix / UntilUnix scope the run list to an explicit time
	// window (UTC seconds since epoch). Used by the anomaly-marker
	// click on the metrics page. When non-zero they override Range.
	SinceUnix int64
	UntilUnix int64
	Page      int
	Size      int
	HasNext   bool
	HasPrev   bool
	NextPage  int
	PrevPage  int
	Total     int
	// FirstIndex / LastIndex are the 1-indexed bounds of the current
	// page, used to render "Showing N–M of K runs" in the lede.
	// When Total is 0 both fields are 0 and the template omits the
	// range. Audit H3 — replaced raw "Total" count with a windowed
	// lede so operators understand pagination is happening.
	FirstIndex int
	LastIndex  int
	Workflows  []string
	Rows       []RunRow
	// EmptyState is non-nil only when no filters are set and the run
	// log is empty — first-time-operator state.
	EmptyState *EmptyState
}

// buildRunsView assembles RunsListView from query params. Filters
// apply server-side; the pagination math is identical to workflows.
func buildRunsView(
	ctx context.Context, ds DataSource, q map[string][]string,
) (RunsListView, error) {
	if ds == nil {
		panic("buildRunsView: ds is nil")
	}
	if ctx == nil {
		panic("buildRunsView: ctx is nil")
	}
	wf := firstQueryValue(q, "workflow")
	status := firstQueryValue(q, "status")
	idSubstr := firstQueryValue(q, "id")
	rng := firstQueryValue(q, "range")
	since := parseUnixSecsParam(firstQueryValue(q, "since"))
	until := parseUnixSecsParam(firstQueryValue(q, "until"))
	page, size := parsePageAndSize(firstQueryValue(q, "page"),
		firstQueryValue(q, "size"))
	// Audit H3 — runs page defaults to 50 per page (vs the global
	// 20 default) so operators see one screenful of activity per
	// page. Explicit ?size= still wins, clamped by parsePageAndSize.
	if firstQueryValue(q, "size") == "" {
		size = 50
	}
	runs, err := ds.ListRuns(ctx, wf)
	if err != nil {
		return RunsListView{}, fmt.Errorf("list runs: %w", err)
	}
	runs = filterRunsByStatus(runs, status)
	runs = filterRunsByIDSubstring(runs, idSubstr)
	if since > 0 || until > 0 {
		runs = filterRunsByWindow(runs, since, until)
	} else {
		runs = filterRunsByRange(runs, rng, time.Now())
	}
	defs, _ := ds.ListWorkflows(ctx)
	win := computePageWindow(len(runs), page, size)
	view := RunsListView{
		Header:   buildRunsHeader(runs, time.Now()),
		Workflow: wf, Status: status, IDFilter: idSubstr, Range: rng,
		SinceUnix: since, UntilUnix: until,
		Page: win.Page, Size: win.Size, Total: win.Total,
		HasNext: win.HasNext, HasPrev: win.HasPrev,
		NextPage: win.NextPage, PrevPage: win.PrevPage,
		FirstIndex: win.FirstIndex, LastIndex: win.LastIndex,
		Workflows: workflowNamesFromDefs(defs),
		Rows:      toRunRows(runs[win.Start:win.End]),
	}
	if win.Total == 0 && wf == "" && status == "" && idSubstr == "" &&
		(rng == "" || rng == "all") && since == 0 && until == 0 {
		view.EmptyState = newRunsEmptyState()
	}
	return view, nil
}

// newRunsEmptyState builds the EmptyState for /console/runs when no
// runs have been recorded and no filters are active. Mirrors the
// runs page tutorial copy — point operators at the triggers page so
// the system can drive itself.
func newRunsEmptyState() *EmptyState {
	e, err := NewEmptyState(EmptyState{
		Icon:        "run",
		Title:       "No runs yet",
		Description: "Start a run from the CLI or configure a trigger to drive runs automatically.",
		PrimaryAction: &EmptyStateAction{
			Label: "Configure triggers",
			Href:  "/console/triggers",
		},
	})
	if err != nil {
		return nil
	}
	return &e
}

// buildRunsHeader assembles the three count tiles shown above the
// runs table: in-the-last-hour / currently-running / failed-in-window.
// The set is the already-filtered runs slice (matches the rows visible
// to the operator); the hour bucket is anchored to the supplied now so
// tests can pin the clock without flaking.
func buildRunsHeader(runs []dag.WorkflowRun, now time.Time) PageHeader {
	hourCutoff := now.Add(-time.Hour)
	recent := 0
	running := 0
	failed := 0
	for i := 0; i < len(runs) && i < runsMax; i++ {
		r := runs[i]
		if r.CreatedAt.After(hourCutoff) {
			recent++
		}
		switch r.Status {
		case dag.RunStatusRunning:
			running++
		case dag.RunStatusFailed:
			failed++
		}
	}
	tiles := []Tile{
		{Label: "in last 1h", Count: recent, Tone: ToneDefault,
			Tooltip: "Runs created within the past hour"},
		{Label: "running", Count: running, Tone: ToneWarning,
			Href: "/console/runs?status=running"},
		{Label: "failed", Count: failed, Tone: ToneDanger,
			Href: "/console/runs?status=failed"},
	}
	h, err := NewPageHeader(PageHeader{
		Title: "Runs",
		Tiles: tiles,
	})
	if err != nil {
		return PageHeader{Title: "Runs"}
	}
	return h
}

// parseUnixSecsParam parses a positive int64 from a URL query value;
// returns 0 on parse error or empty / negative input. Used by the
// runs-list since / until filter wired up from the anomaly-marker
// click handler on the metrics page.
func parseUnixSecsParam(raw string) int64 {
	if raw == "" {
		return 0
	}
	const maxLen = 20
	if len(raw) > maxLen {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

// filterRunsByWindow keeps runs whose CreatedAt falls between
// [sinceUnix, untilUnix] inclusive. Either bound may be zero, in
// which case it's treated as unbounded on that side. Paired with the
// anomaly-marker click handler on the metrics dashboard.
func filterRunsByWindow(
	runs []dag.WorkflowRun, sinceUnix, untilUnix int64,
) []dag.WorkflowRun {
	if sinceUnix == 0 && untilUnix == 0 {
		return runs
	}
	out := make([]dag.WorkflowRun, 0, len(runs))
	for _, r := range runs {
		ts := r.CreatedAt.Unix()
		if sinceUnix > 0 && ts < sinceUnix {
			continue
		}
		if untilUnix > 0 && ts > untilUnix {
			continue
		}
		out = append(out, r)
	}
	return out
}

// filterRunsByStatus narrows runs to those matching statusName.
// Empty / "any" passes through. Unknown statusName returns empty.
func filterRunsByStatus(
	runs []dag.WorkflowRun, statusName string,
) []dag.WorkflowRun {
	if statusName == "" || statusName == "any" {
		return runs
	}
	parsed, err := dag.ParseRunStatus(statusName)
	if err != nil {
		return []dag.WorkflowRun{}
	}
	out := make([]dag.WorkflowRun, 0, len(runs))
	for _, r := range runs {
		if r.Status == parsed {
			out = append(out, r)
		}
	}
	return out
}

// filterRunsByIDSubstring keeps runs whose RunID contains the given
// (case-insensitive) substring. This powers the find-by-id box, which
// narrows the table rather than navigating to a single detail page —
// an operator pasting a partial id sees every run that matches. Empty
// substr is a noop and returns the input unchanged.
func filterRunsByIDSubstring(
	runs []dag.WorkflowRun, substr string,
) []dag.WorkflowRun {
	if substr == "" {
		return runs
	}
	needle := strings.ToLower(substr)
	out := make([]dag.WorkflowRun, 0, len(runs))
	for _, r := range runs {
		if strings.Contains(strings.ToLower(r.RunID), needle) {
			out = append(out, r)
		}
	}
	return out
}

// filterRunsByRange keeps runs whose CreatedAt is within the chosen
// window. Anchored at `now` so tests can pin the clock.
func filterRunsByRange(
	runs []dag.WorkflowRun, rng string, now time.Time,
) []dag.WorkflowRun {
	if rng == "" || rng == "all" {
		return runs
	}
	var window time.Duration
	switch rng {
	case "1h":
		window = time.Hour
	case "24h":
		window = 24 * time.Hour
	case "7d":
		window = 7 * 24 * time.Hour
	default:
		return runs
	}
	cutoff := now.Add(-window)
	out := make([]dag.WorkflowRun, 0, len(runs))
	for _, r := range runs {
		if r.CreatedAt.After(cutoff) {
			out = append(out, r)
		}
	}
	return out
}

// workflowNamesFromDefs extracts the names for the filter dropdown.
func workflowNamesFromDefs(defs []dag.WorkflowDef) []string {
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, d.Name)
	}
	sort.Strings(out)
	return out
}

// toRunRows projects dag.WorkflowRun → RunRow for templates.
func toRunRows(runs []dag.WorkflowRun) []RunRow {
	out := make([]RunRow, 0, len(runs))
	for _, r := range runs {
		row := runRowFromRun(r)
		out = append(out, row)
	}
	return out
}

// runRowFromRun is the per-row projector pulled out so unit tests
// can verify duration / icon rendering directly.
func runRowFromRun(r dag.WorkflowRun) RunRow {
	statusStr := r.Status.String()
	row := RunRow{
		RunID:       r.RunID,
		RunIDShort:  shortRunID(r.RunID),
		WorkflowID:  r.WorkflowID,
		Status:      statusStr,
		StatusIcon:  statusIcon(statusStr),
		TriggerKind: triggerKindFromInput(r.Input),
		StartedAt:   r.CreatedAt.UTC().Format(time.RFC3339),
	}
	// Terminal runs carry CompletedAt in the snapshot (the engine stamps
	// it on every terminal path), so the list shows the real wall-clock
	// duration without per-run history. Older snapshots predate the field
	// and fall back to the honest "—" placeholder. In-flight runs render a
	// labelled elapsed time so it can't be mistaken for a final duration.
	if r.Status.IsTerminal() {
		row.Duration = "—"
		if r.CompletedAt != nil && !r.CompletedAt.IsZero() &&
			r.CompletedAt.After(r.CreatedAt) {
			row.Duration = formatDuration(r.CompletedAt.Sub(r.CreatedAt))
		}
	} else {
		row.Duration = formatDuration(time.Since(r.CreatedAt)) + " elapsed"
	}
	return row
}

// shortRunID truncates a run id to the leading 12 chars for compact
// table display. The full ID is still present in URLs and tooltips.
func shortRunID(id string) string {
	const shortLen = 12
	if len(id) <= shortLen {
		return id
	}
	return id[:shortLen]
}

// statusIcon picks a unicode icon for a status. Matches ADR-014's
// status-palette table so operators see the same multimodal cue
// across surfaces.
func statusIcon(status string) string {
	switch status {
	case "completed":
		return "✓" // ✓
	case "running":
		return "●" // ●
	case "failed":
		return "✗" // ✗
	case "skipped", "cancelled":
		return "⊘" // ⊘
	default:
		return "○" // ○
	}
}

// outcomeIcon picks a unicode glyph for an audit-log outcome. The
// outcome vocabulary (success / denied / error) is a superset of
// statusIcon's run-status vocabulary, so it lives as its own helper
// to keep statusIcon's switch lean. Multimodal cue per audit fix C4.
func outcomeIcon(outcome string) string {
	switch outcome {
	case "success":
		return "✓"
	case "denied":
		return "⊘"
	case "error":
		return "✗"
	default:
		return "○"
	}
}

// triggerKindFromInput tries to identify how a run was started by
// peeking at the wrapper json. Unwrapped inputs render as "manual".
// Errors are swallowed: a malformed wrapper is operator-noise, not
// a UI failure.
func triggerKindFromInput(input []byte) string {
	if len(input) == 0 {
		return "manual"
	}
	var env trigger.TriggerEnvelope
	if err := json.Unmarshal(input, &env); err != nil {
		return "manual"
	}
	if env.Trigger == "" {
		return "manual"
	}
	return env.Trigger
}

// formatDuration renders a duration in a compact operator-friendly
// form: ms below 1s, then s, then m s.
func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	mins := int(d.Minutes())
	secs := int(d.Seconds()) - mins*60
	return fmt.Sprintf("%dm%ds", mins, secs)
}

// RunDetailView powers /console/runs/<id>.
//
// Phase 2 (T03+T04+T05): the page is now a tabs container. Steps tab
// is the default-active panel and renders the step list partial; the
// Events / DAG / Input-Output panels lazy-load via fragment endpoints
// on first click. FailedStep* fields populate the top-of-page error
// banner when Status == "failed". StepRows feeds the step list partial.
type RunDetailView struct {
	RunID       string
	RunIDShort  string
	WorkflowID  string
	Status      string
	StatusIcon  string
	TriggerKind string
	StartedAt   string
	Duration    string
	Input       string
	Output      string
	HasOutput   bool
	ErrorMsg    string
	HasError    bool
	NotFound    bool
	StepRows    []stepRow
	Events      []EventRow
	// MaxEventSeq is the highest JetStream stream sequence rendered
	// into the static Events tbody. The SSE endpoint reads this from
	// ?from=<seq> so the live stream resumes after the prefix and
	// the operator doesn't see every event rendered twice.
	MaxEventSeq uint64
	// FailedStep* populate the run-error-banner when HasError is true
	// and Status == "failed". Operators see them at the top of the
	// page so debugging starts without a scroll. Empty / zero values
	// suppress the banner (paired with HasError gating in template).
	FailedStepID       string
	FailedStepError    string
	FailedStepAttempts int

	// ReadOnly mirrors cfg.ReadOnly so the template can disable the
	// Signal / Cancel controls with the standard read-only signifier.
	// CSRFToken is the per-actor token the action fetches send via the
	// X-CSRF-Token header. CanCancel is false for terminal runs so the
	// Cancel button (and the Signal form) are not rendered against a run
	// that can no longer act on them — single-sourced on IsTerminal().
	ReadOnly  bool
	CSRFToken string
	CanCancel bool
}

// EventRow is one line of the run event timeline.
type EventRow struct {
	Index       int
	Timestamp   string
	Type        string
	StepID      string
	DataPreview string
	DataFull    string
}

// dispatchRuns routes the catch-all /console/runs/ prefix as
// /<id> or /<id>/<action>. The single-segment case renders the
// run-detail page; the action segments dispatch to the trace page or
// the cancel / signal mutation handlers. Mirrors dispatchTriggers'
// SplitN dispatch so the run-detail handler keeps its "no slash in id"
// invariant — only the nested-action branches carry a second segment.
func dispatchRuns(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchRuns: w is nil")
	}
	if r == nil {
		panic("dispatchRuns: r is nil")
	}
	rest := strings.TrimPrefix(r.URL.Path, "/console/runs/")
	if rest == "" {
		serveNotFound(w, r, ts, cfg)
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 1 {
		servePageRunDetail(w, r, ts, cfg)
		return
	}
	runID, action := parts[0], parts[1]
	switch action {
	case "trace":
		servePageRunTrace(w, r, ts, cfg)
	case "cancel":
		handleRunCancel(w, r, cfg, runID)
	case "signal":
		handleRunSignal(w, r, cfg, runID)
	default:
		serveNotFound(w, r, ts, cfg)
	}
}

// RunTraceView powers the standalone /console/runs/<id>/trace page. It
// carries the run id (full + short) for the page header and back-link,
// the flattened span rows the shared trace-tree component renders, and
// an optional Note surfaced when the trace read degraded.
type RunTraceView struct {
	RunID      string
	RunIDShort string
	Rows       []TraceRow
	Note       string
}

// servePageRunTrace renders /console/runs/<id>/trace as a deep-linkable
// full page. It reuses GetRunTrace — the same read the lazy Trace tab
// uses — and degrades a read error to an empty tree plus a Note rather
// than a 500, so the page never lies about telemetry it couldn't load.
func servePageRunTrace(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageRunTrace: w is nil")
	}
	if r == nil {
		panic("servePageRunTrace: r is nil")
	}
	rest := strings.TrimPrefix(r.URL.Path, "/console/runs/")
	runID := strings.TrimSuffix(rest, "/trace")
	if runID == "" || strings.Contains(runID, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	ds, ok := requireData(w, cfg, "run-trace")
	if !ok {
		return
	}
	var note string
	rows, err := ds.GetRunTrace(r.Context(), runID)
	if err != nil {
		cfg.Logger.Warn("console: get run trace",
			"run", runID, "err", err)
		rows = nil
		note = "Trace read failed; showing no spans."
	}
	renderPage(w, r, ts, cfg, "run-trace", pageData{
		Title:   "Trace",
		Section: "runs",
		Page: RunTraceView{
			RunID:      runID,
			RunIDShort: shortRunID(runID),
			Rows:       rows,
			Note:       note,
		},
	})
}

// servePageRunDetail renders /console/runs/<id>.
func servePageRunDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageRunDetail: w is nil")
	}
	if r == nil {
		panic("servePageRunDetail: r is nil")
	}
	id := strings.TrimPrefix(r.URL.Path, "/console/runs/")
	if id == "" || strings.Contains(id, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	ds, ok := requireData(w, cfg, "run-detail")
	if !ok {
		return
	}
	view := buildRunDetail(r.Context(), ds, id)
	view.ReadOnly = cfg.ReadOnly
	view.CSRFToken = csrfTokenFor(r)
	renderPage(w, r, ts, cfg, "run-detail", pageData{
		Title:   "Run " + shortRunID(id),
		Section: "runs",
		Page:    view,
	})
}

// serveRunTabFragment routes /console/api/run/<id>/{events,dag,io}-tab
// to the matching lazy-load handler. The route prefix is shared so a
// single mux registration covers all three tabs; the suffix selects
// which fragment to render. Unknown suffixes return 404.
func serveRunTabFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveRunTabFragment: w is nil")
	}
	if r == nil {
		panic("serveRunTabFragment: r is nil")
	}
	rest := strings.TrimPrefix(r.URL.Path, "/console/api/run/")
	slash := strings.LastIndex(rest, "/")
	if slash <= 0 || slash >= len(rest)-1 {
		http.NotFound(w, r)
		return
	}
	runID, suffix := rest[:slash], rest[slash+1:]
	if runID == "" || strings.Contains(runID, "/") {
		http.NotFound(w, r)
		return
	}
	switch suffix {
	case "events-tab":
		serveRunEventsTabFragment(w, r, ts, cfg, runID)
	case "io-tab":
		serveRunIOTabFragment(w, r, ts, cfg, runID)
	case "trace-tab":
		serveRunTraceTabFragment(w, r, ts, cfg, runID)
	default:
		http.NotFound(w, r)
	}
}

// serveRunTraceTabFragment reads the run's span trace and streams the
// rendered span tree back as one SSE PatchElements event targeting
// #panel-trace with inner-mode — mirroring the events / io tab
// fragments. An empty trace renders the honest empty-state copy rather
// than a fabricated span; a read error logs + paints the empty state so
// the tab degrades instead of 500ing.
func serveRunTraceTabFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, runID string,
) {
	ds, ok := requireData(w, cfg, "run-trace-tab")
	if !ok {
		return
	}
	rows, err := ds.GetRunTrace(r.Context(), runID)
	if err != nil {
		cfg.Logger.Warn("console: get run trace",
			"run", runID, "err", err)
		rows = nil
	}
	emitTraceTabFragment(w, r, ts, cfg, rows)
}

// emitTraceTabFragment renders the run-trace-tab template against the
// rows and patches #panel-trace inner. Kept separate from
// emitTabFragment because the trace tab renders against []TraceRow
// rather than RunDetailView.
func emitTraceTabFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, rows []TraceRow,
) {
	html, err := renderFragment(ts.base, "run-trace-tab", rows)
	if err != nil {
		cfg.Logger.Error("console: render trace tab fragment",
			"err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	sse := datastar.NewSSE(w, r)
	opts := []datastar.PatchElementOption{
		datastar.WithSelectorID("panel-trace"),
		datastar.WithModeInner(),
	}
	if err := sse.PatchElements(html, opts...); err != nil {
		cfg.Logger.Warn("console: trace tab patch elements", "err", err)
	}
}

// serveRunEventsTabFragment renders the events table for run id and
// streams it back as one SSE PatchElements event targeting
// #panel-events with inner-mode. The handler reuses buildRunDetail
// so the rendered timeline matches what the page would have shown
// had it been eager-rendered.
func serveRunEventsTabFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, runID string,
) {
	ds, ok := requireData(w, cfg, "run-events-tab")
	if !ok {
		return
	}
	view := buildRunDetail(r.Context(), ds, runID)
	if view.NotFound {
		http.NotFound(w, r)
		return
	}
	emitTabFragment(w, r, ts, cfg, "run-events-tab", "panel-events", view)
}

// serveRunIOTabFragment renders the run-level input + (when present)
// output. T04's per-step I/O reuses this same data path but slots
// each blob into the per-row body — the io-tab keeps the run-level
// summary for backwards readability with PR 4's flat layout.
func serveRunIOTabFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, runID string,
) {
	ds, ok := requireData(w, cfg, "run-io-tab")
	if !ok {
		return
	}
	view := buildRunDetail(r.Context(), ds, runID)
	if view.NotFound {
		http.NotFound(w, r)
		return
	}
	emitTabFragment(w, r, ts, cfg, "run-io-tab", "panel-io", view)
}

// emitTabFragment renders one named template against the view and
// emits it as a Datastar PatchElements event scoped to panelID with
// inner-mode. Lifted out of the three serve* handlers so the wire
// format stays in one place — the four lines of SSE setup were the
// only thing varying.
func emitTabFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
	templateName, panelID string, view RunDetailView,
) {
	if templateName == "" {
		panic("emitTabFragment: templateName is empty")
	}
	if panelID == "" {
		panic("emitTabFragment: panelID is empty")
	}
	html, err := renderFragment(ts.base, templateName, view)
	if err != nil {
		cfg.Logger.Error("console: render tab fragment",
			"template", templateName, "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	sse := datastar.NewSSE(w, r)
	opts := []datastar.PatchElementOption{
		datastar.WithSelectorID(panelID),
		datastar.WithModeInner(),
	}
	if err := sse.PatchElements(html, opts...); err != nil {
		cfg.Logger.Warn("console: tab patch elements",
			"template", templateName, "err", err)
	}
}

// buildRunDetail reads run snapshot + history events + workflow def
// and assembles the view. Each data fetch is best-effort; partial
// failures leave the affected section empty.
func buildRunDetail(
	ctx context.Context, ds DataSource, id string,
) RunDetailView {
	if ds == nil {
		panic("buildRunDetail: ds is nil")
	}
	if id == "" {
		panic("buildRunDetail: id is empty")
	}
	run, err := ds.GetRun(ctx, id)
	if err != nil {
		return RunDetailView{RunID: id, NotFound: true}
	}
	view := runDetailBaseView(run)
	def, defErr := ds.GetWorkflow(run.WorkflowID)
	events, _ := ds.ListRunEvents(ctx, id, false)
	view.Duration = runDuration(run, events, time.Now())
	if defErr == nil {
		view.StepRows = BuildStepRows(&def, &run, events, nil, nil)
		view.StepRows = computeTimelineGeometry(
			view.StepRows, events, run.CreatedAt,
			runWindow(run, events, time.Now()))
	}
	view.Events = toEventRows(events)
	view.MaxEventSeq = maxEventSeq(events)
	view.Input = prettyJSON(run.Input)
	out, errMsg := runOutputAndError(run)
	if out != "" {
		view.Output = out
		view.HasOutput = true
	}
	if errMsg != "" {
		view.ErrorMsg = errMsg
		view.HasError = true
	}
	if run.Status == dag.RunStatusFailed {
		populateFailedStep(&view, def, run)
	}
	view.CanCancel = !run.Status.IsTerminal()
	return view
}

// populateFailedStep fills the run-error-banner fields by finding the
// first failed step in the workflow definition's declared order. We
// preserve def order (not map iteration) so the banner points at the
// step closest to the start of the workflow that actually failed —
// matching the operator's mental model of where the run "broke".
func populateFailedStep(
	view *RunDetailView, def dag.WorkflowDef, run dag.WorkflowRun,
) {
	if view == nil {
		panic("populateFailedStep: view is nil")
	}
	if len(def.Steps) == 0 {
		return
	}
	for _, s := range def.Steps {
		st, ok := run.Steps[s.ID]
		if !ok {
			continue
		}
		if st.Status != dag.StepStatusFailed {
			continue
		}
		view.HasError = true
		view.FailedStepID = s.ID
		view.FailedStepError = st.Error
		view.FailedStepAttempts = st.Attempts
		return
	}
}

// runDetailBaseView assembles the always-present header fields.
func runDetailBaseView(run dag.WorkflowRun) RunDetailView {
	statusStr := run.Status.String()
	v := RunDetailView{
		RunID:       run.RunID,
		RunIDShort:  shortRunID(run.RunID),
		WorkflowID:  run.WorkflowID,
		Status:      statusStr,
		StatusIcon:  statusIcon(statusStr),
		TriggerKind: triggerKindFromInput(run.Input),
		StartedAt:   run.CreatedAt.UTC().Format(time.RFC3339),
	}
	// buildRunDetail overwrites Duration once it has read the run's
	// history events (the terminal timestamp source). This base value
	// is the honest fallback used only when events are absent: a
	// labelled elapsed for in-flight runs, "—" for terminal runs whose
	// end timestamp can't be recovered.
	if run.Status.IsTerminal() {
		v.Duration = "—"
	} else {
		v.Duration = formatDuration(time.Since(run.CreatedAt)) + " elapsed"
	}
	return v
}

// runDuration computes the rendered duration for the run-detail header.
// Terminal runs use (last event timestamp − CreatedAt): the terminal
// workflow event the engine already records is the real end time.
// In-flight runs render a labelled elapsed (now − CreatedAt). When the
// timestamps are genuinely absent — a terminal run with no usable
// events, or a zero CreatedAt — the honest "—" is returned rather than
// a fabricated value. Bounded by len(events).
func runDuration(
	run dag.WorkflowRun, events []api.RunEvent, now time.Time,
) string {
	if run.CreatedAt.IsZero() {
		return "—"
	}
	if !run.Status.IsTerminal() {
		return formatDuration(now.Sub(run.CreatedAt)) + " elapsed"
	}
	const eventsMax = 1_000_000
	var latest time.Time
	for i := 0; i < len(events) && i < eventsMax; i++ {
		if events[i].Timestamp.After(latest) {
			latest = events[i].Timestamp
		}
	}
	if latest.IsZero() || latest.Before(run.CreatedAt) {
		return "—"
	}
	return formatDuration(latest.Sub(run.CreatedAt))
}

// runWindow is the gantt denominator: the total span the Timeline tab
// lays steps out against. Terminal runs use (last event − CreatedAt);
// in-flight runs use (now − CreatedAt). Returns zero when the window
// can't be derived honestly — computeTimelineGeometry then skips bars.
// Bounded by len(events).
func runWindow(
	run dag.WorkflowRun, events []api.RunEvent, now time.Time,
) time.Duration {
	if run.CreatedAt.IsZero() {
		return 0
	}
	if !run.Status.IsTerminal() {
		return now.Sub(run.CreatedAt)
	}
	const eventsMax = 1_000_000
	var latest time.Time
	for i := 0; i < len(events) && i < eventsMax; i++ {
		if events[i].Timestamp.After(latest) {
			latest = events[i].Timestamp
		}
	}
	if latest.IsZero() || latest.Before(run.CreatedAt) {
		return 0
	}
	return latest.Sub(run.CreatedAt)
}

// runOutputAndError returns the rendered output JSON (if any) and
// the last failed step's error message (if any).
func runOutputAndError(run dag.WorkflowRun) (string, string) {
	if run.Steps == nil {
		return "", ""
	}
	var lastErr string
	var outputBytes []byte
	for _, s := range run.Steps {
		if s.Status == dag.StepStatusFailed && s.Error != "" {
			lastErr = s.Error
		}
		if len(s.Output) > 0 {
			outputBytes = s.Output
		}
	}
	var out string
	if run.Status == dag.RunStatusCompleted && len(outputBytes) > 0 {
		out = prettyJSON(outputBytes)
	}
	return out, lastErr
}

// maxEventSeq returns the highest JetStream stream sequence across
// the rendered events, or 0 when the list is empty / Seq isn't
// populated. The SSE handler uses this to skip the prefix that the
// static render already painted into the tbody.
func maxEventSeq(events []api.RunEvent) uint64 {
	var max uint64
	for _, e := range events {
		if e.Seq > max {
			max = e.Seq
		}
	}
	return max
}

// toEventRows projects RunEvents → EventRow. Already chronologically
// ordered by the service; we preserve order and add an index for
// the template to use as a stable signal key.
func toEventRows(events []api.RunEvent) []EventRow {
	const previewMax = 200
	out := make([]EventRow, 0, len(events))
	for i, e := range events {
		preview := e.Data
		if len(preview) > previewMax {
			preview = preview[:previewMax]
		}
		out = append(out, EventRow{
			Index:       i,
			Timestamp:   e.Timestamp.UTC().Format(time.RFC3339),
			Type:        e.Type,
			StepID:      e.StepID,
			DataPreview: preview,
			DataFull:    e.Data,
		})
	}
	return out
}

// serveRunSheet renders /console/api/runs/<id>/sheet as a Datastar
// PatchElements event scoped to #sheet-outlet. The run id slug is
// taken verbatim — empty / nested / unknown ids return 404 so the
// operator's URL bar still tells the truth.
func serveRunSheet(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveRunSheet: w is nil")
	}
	if r == nil {
		panic("serveRunSheet: r is nil")
	}
	if ts == nil {
		panic("serveRunSheet: ts is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := sheetSlugFromPath(r.URL.Path, "/console/api/runs/", "/sheet")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	ds, ok := requireData(w, cfg, "run-sheet")
	if !ok {
		return
	}
	view := buildRunDetail(r.Context(), ds, id)
	if view.NotFound {
		http.NotFound(w, r)
		return
	}
	sv := sheetView{
		Title:        "Run " + shortRunID(id),
		BodyTemplate: "run-sheet-body",
		Data:         view,
		FullPageHref: "/console/runs/" + id,
	}
	emitSheetFragment(w, r, ts, cfg, sv, "run-sheet")
}

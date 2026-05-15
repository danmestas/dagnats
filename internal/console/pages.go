package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/console/dagviz"
	"github.com/danmestas/dagnats/internal/trigger"
)

// Server-side pagination defaults. Page size is bounded so any single
// page render is cheap regardless of how many runs the operator has
// accumulated; if they want more they paginate. The size param is
// clamped to [1, pageSizeMax].
const (
	pageSizeDefault = 20
	pageSizeMax     = 100
	pageNumberMax   = 10_000 // safety bound on URL-driven loops
)

// pageData is the common payload for every full-page render. Section
// is the active nav tab. Title is the <title> string. Body is template
// `content`-named data ready to inject. ReadOnly mirrors Config.ReadOnly
// so the layout shows the read-only banner uniformly.
type pageData struct {
	Title    string
	Section  string
	Actor    Actor
	Overview overviewData
	Page     any
	ReadOnly bool
}

// renderPage executes the shared `layout` template with the given
// section-specific data. Caller pre-populates pd; we attach the actor
// and write the output. The templateKey selects which per-page
// template tree to execute against; tree keys come from the
// pageContentFiles map in handler.go.
func renderPage(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, templateKey string, pd pageData,
) {
	if w == nil {
		panic("renderPage: w is nil")
	}
	if r == nil {
		panic("renderPage: r is nil")
	}
	if ts == nil {
		panic("renderPage: ts is nil")
	}
	if templateKey == "" {
		panic("renderPage: templateKey is empty")
	}
	tmpl, ok := ts.pageTemplates[templateKey]
	if !ok {
		panic("renderPage: unknown templateKey " + templateKey)
	}
	actor, _ := ActorFrom(r.Context())
	pd.Actor = actor
	pd.Overview = overviewData{
		Listener: cfg.HTTPAddr,
		AuthMode: cfg.AuthMode.String(),
		Build:    cfg.Build,
	}
	pd.ReadOnly = cfg.ReadOnly
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "layout", pd); err != nil {
		cfg.Logger.Error("console: render page", "section", pd.Section, "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(buf.Bytes())
}

// requireData reports an error to the client and returns false when
// no DataSource is configured. Pages depending on a data source must
// short-circuit through this; it keeps the 503 message uniform.
func requireData(
	w http.ResponseWriter, cfg Config, op string,
) (DataSource, bool) {
	if w == nil {
		panic("requireData: w is nil")
	}
	if op == "" {
		panic("requireData: op is empty")
	}
	if cfg.Data == nil {
		cfg.Logger.Warn("console: data source not configured", "op", op)
		http.Error(w,
			"data source not configured",
			http.StatusServiceUnavailable)
		return nil, false
	}
	return cfg.Data, true
}

// WorkflowsListView is what the workflows-list template binds.
// LastRunTime / LastRunStatus are derived per workflow at render
// time from a single ListRuns scan; both empty when no runs exist.
type WorkflowsListView struct {
	Filter   string
	Sort     string
	Page     int
	Size     int
	HasNext  bool
	HasPrev  bool
	NextPage int
	PrevPage int
	Total    int
	Rows     []WorkflowRow
}

// WorkflowRow is one workflow line on the list page.
type WorkflowRow struct {
	Name          string
	Version       string
	StepCount     int
	TriggerCount  int
	LastRunTime   string
	LastRunStatus string
}

// servePageWorkflowsList renders /console/workflows.
func servePageWorkflowsList(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageWorkflowsList: w is nil")
	}
	if r == nil {
		panic("servePageWorkflowsList: r is nil")
	}
	ds, ok := requireData(w, cfg, "workflows-list")
	if !ok {
		return
	}
	view, err := buildWorkflowsView(r.Context(), ds, r.URL.Query())
	if err != nil {
		cfg.Logger.Error("console: workflows list", "err", err)
		http.Error(w, "list workflows failed", http.StatusInternalServerError)
		return
	}
	renderPage(w, r, ts, cfg, "workflows-list", pageData{
		Title:   "Workflows",
		Section: "workflows",
		Page:    view,
	})
}

// buildWorkflowsView assembles WorkflowsListView from query params.
// Pulled out of the handler so the fragment endpoint can reuse it.
func buildWorkflowsView(
	ctx context.Context, ds DataSource, q map[string][]string,
) (WorkflowsListView, error) {
	if ds == nil {
		panic("buildWorkflowsView: ds is nil")
	}
	if ctx == nil {
		panic("buildWorkflowsView: ctx is nil")
	}
	filter := firstQueryValue(q, "filter")
	sortKey := firstQueryValue(q, "sort")
	if sortKey == "" {
		sortKey = "name"
	}
	page, size := parsePageAndSize(firstQueryValue(q, "page"),
		firstQueryValue(q, "size"))
	defs, err := ds.ListWorkflows(ctx)
	if err != nil {
		return WorkflowsListView{}, fmt.Errorf("list workflows: %w", err)
	}
	defs = filterWorkflows(defs, filter)
	triggers, _ := ds.ListTriggers(ctx)
	runs, _ := ds.ListRuns(ctx, "")
	rows := assembleWorkflowRows(defs, triggers, runs)
	sortWorkflowRows(rows, sortKey)
	total := len(rows)
	start, end, hasNext := paginate(total, page, size)
	view := WorkflowsListView{
		Filter:   filter,
		Sort:     sortKey,
		Page:     page,
		Size:     size,
		Total:    total,
		HasNext:  hasNext,
		HasPrev:  page > 1,
		NextPage: page + 1,
		PrevPage: page - 1,
		Rows:     rows[start:end],
	}
	return view, nil
}

// filterWorkflows returns only definitions whose name contains the
// substring filter (case-insensitive). An empty filter returns the
// input slice verbatim.
func filterWorkflows(defs []dag.WorkflowDef, filter string) []dag.WorkflowDef {
	if filter == "" {
		return defs
	}
	lc := strings.ToLower(filter)
	out := make([]dag.WorkflowDef, 0, len(defs))
	for _, d := range defs {
		if strings.Contains(strings.ToLower(d.Name), lc) {
			out = append(out, d)
		}
	}
	return out
}

// assembleWorkflowRows zips workflow defs with derived counts and
// last-run-stats. Single pass over runs so the cost stays O(N+R).
// Nil triggers/runs slices are tolerated — the api.Service surfaces
// "empty bucket" as a nil slice plus a nil error, and that case is
// expected on a fresh deployment.
func assembleWorkflowRows(
	defs []dag.WorkflowDef,
	triggers []trigger.TriggerDef,
	runs []dag.WorkflowRun,
) []WorkflowRow {
	if defs == nil {
		return nil
	}
	triggerCount := make(map[string]int, len(defs))
	for _, t := range triggers {
		triggerCount[t.WorkflowID]++
	}
	lastRun := lastRunPerWorkflow(runs)
	rows := make([]WorkflowRow, 0, len(defs))
	for _, d := range defs {
		row := WorkflowRow{
			Name:         d.Name,
			Version:      d.Version,
			StepCount:    len(d.Steps),
			TriggerCount: triggerCount[d.Name],
		}
		if lr, ok := lastRun[d.Name]; ok {
			row.LastRunTime = lr.CreatedAt.UTC().Format(time.RFC3339)
			row.LastRunStatus = lr.Status.String()
		}
		rows = append(rows, row)
	}
	return rows
}

// lastRunPerWorkflow picks the most-recent run for each workflow ID
// in a single pass. runs need not be sorted. A nil runs slice is
// tolerated — empty input returns an empty map.
func lastRunPerWorkflow(
	runs []dag.WorkflowRun,
) map[string]dag.WorkflowRun {
	out := make(map[string]dag.WorkflowRun, len(runs))
	for _, r := range runs {
		if existing, ok := out[r.WorkflowID]; ok {
			if !r.CreatedAt.After(existing.CreatedAt) {
				continue
			}
		}
		out[r.WorkflowID] = r
	}
	return out
}

// sortWorkflowRows orders rows by sortKey. Falls back to name when
// the key is unrecognised so the URL stays forgiving. A nil rows
// slice is treated as empty — no panic, nothing to sort.
func sortWorkflowRows(rows []WorkflowRow, sortKey string) {
	if rows == nil {
		return
	}
	if sortKey == "last_run" {
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].LastRunTime > rows[j].LastRunTime
		})
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Name < rows[j].Name
	})
}

// WorkflowDetailView powers /console/workflows/<name>.
type WorkflowDetailView struct {
	Name           string
	Version        string
	RegisteredHint string
	Definition     string
	Warnings       []dag.Warning
	Triggers       []TriggerLine
	RecentRuns     []RunRow
	NotFound       bool
	// DAGSVG is the pre-rendered inline SVG for the static DAG
	// visualisation. Empty when the workflow has 0 steps or layout
	// failed; DAGFallback carries a human-readable explanation in
	// that case.
	DAGSVG      template.HTML
	DAGFallback string
}

// TriggerLine is one trigger entry under a workflow. The kind/target
// strings render directly in the template — no further parsing.
type TriggerLine struct {
	ID      string
	Kind    string
	Target  string
	Enabled bool
}

// servePageWorkflowDetail renders /console/workflows/<name>.
func servePageWorkflowDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageWorkflowDetail: w is nil")
	}
	if r == nil {
		panic("servePageWorkflowDetail: r is nil")
	}
	name := strings.TrimPrefix(r.URL.Path, "/console/workflows/")
	if name == "" || strings.Contains(name, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	ds, ok := requireData(w, cfg, "workflow-detail")
	if !ok {
		return
	}
	view := buildWorkflowDetail(r.Context(), ds, name)
	renderPage(w, r, ts, cfg, "workflow-detail", pageData{
		Title:   "Workflow " + name,
		Section: "workflows",
		Page:    view,
	})
}

// buildWorkflowDetail loads the named workflow, attached triggers,
// recent runs, and validator warnings.
func buildWorkflowDetail(
	ctx context.Context, ds DataSource, name string,
) WorkflowDetailView {
	if ds == nil {
		panic("buildWorkflowDetail: ds is nil")
	}
	if name == "" {
		panic("buildWorkflowDetail: name is empty")
	}
	def, err := ds.GetWorkflow(name)
	if err != nil {
		return WorkflowDetailView{Name: name, NotFound: true}
	}
	defJSON, _ := json.MarshalIndent(def, "", "  ")
	triggers, _ := ds.ListTriggers(ctx)
	attached := triggerLinesFor(triggers, name)
	runs, _ := ds.ListRuns(ctx, name)
	const recentMax = 20
	if len(runs) > recentMax {
		runs = runs[:recentMax]
	}
	hasHTTP := false
	for _, t := range triggers {
		if t.WorkflowID == name && t.HTTP != nil {
			hasHTTP = true
			break
		}
	}
	warnings := dag.ValidateRespondReachability(def, hasHTTP)
	view := WorkflowDetailView{
		Name:       name,
		Version:    def.Version,
		Definition: string(defJSON),
		Warnings:   warnings,
		Triggers:   attached,
		RecentRuns: toRunRows(runs),
	}
	view.DAGSVG, view.DAGFallback = renderStaticDAG(def)
	return view
}

// renderStaticDAG returns the SVG + an optional fallback string for
// the workflow-detail header. Pure projection — failures are converted
// to readable text so the page still renders. Imports kept local so
// pages.go doesn't pull dagviz into other call sites.
func renderStaticDAG(def dag.WorkflowDef) (template.HTML, string) {
	body, err := dagviz.Render(def, nil)
	if err != nil {
		switch {
		case errors.Is(err, dagviz.ErrCycle):
			return "", "Workflow definition has a cycle — DAG omitted."
		case errors.Is(err, dagviz.ErrTooManySteps):
			return "", "Workflow exceeds 30-step visualisation cap — view list below."
		}
		return "", "DAG visualisation unavailable."
	}
	return template.HTML(body), ""
}

// renderLiveDAG produces the run-detail overlay rendering. run is
// non-nil here — buildRunDetail already short-circuited the
// no-run-state case.
func renderLiveDAG(
	def dag.WorkflowDef, run *dag.WorkflowRun,
) (template.HTML, string) {
	if run == nil {
		return renderStaticDAG(def)
	}
	body, err := dagviz.Render(def, run)
	if err != nil {
		switch {
		case errors.Is(err, dagviz.ErrCycle):
			return "", "Workflow definition has a cycle — DAG omitted."
		case errors.Is(err, dagviz.ErrTooManySteps):
			return "", "Workflow exceeds 30-step visualisation cap — see list below."
		}
		return "", "DAG visualisation unavailable."
	}
	return template.HTML(body), ""
}

// triggerLinesFor narrows triggers to those attached to workflowName
// and shapes them for the template. A nil triggers slice is a valid
// empty input — the api.Service returns nil when the triggers bucket
// is empty / missing, and that case is benign.
func triggerLinesFor(
	triggers []trigger.TriggerDef, workflowName string,
) []TriggerLine {
	if workflowName == "" {
		panic("triggerLinesFor: workflowName is empty")
	}
	if triggers == nil {
		return nil
	}
	out := make([]TriggerLine, 0, len(triggers))
	for _, t := range triggers {
		if t.WorkflowID != workflowName {
			continue
		}
		kind, target := triggerKindAndTarget(t)
		out = append(out, TriggerLine{
			ID: t.ID, Kind: kind, Target: target, Enabled: t.Enabled,
		})
	}
	return out
}

// triggerKindAndTarget renders a single trigger to (kind, target)
// strings. Mirrors the CLI's `trigger list` rendering so operators
// see consistent labels across surfaces.
func triggerKindAndTarget(t trigger.TriggerDef) (string, string) {
	if t.Cron != nil {
		return "cron", t.Cron.Expression
	}
	if t.Subject != nil {
		return "subject", t.Subject.Subject
	}
	if t.HTTP != nil {
		return "http", t.HTTP.Method + " " + t.HTTP.Path
	}
	if t.Webhook != nil {
		return "webhook", t.Webhook.Path
	}
	return "unknown", ""
}

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
	renderPage(w, r, ts, cfg, "runs-list", pageData{
		Title:   "Runs",
		Section: "runs",
		Page:    view,
	})
}

// RunsListView powers /console/runs.
type RunsListView struct {
	Workflow  string
	Status    string
	Range     string
	Page      int
	Size      int
	HasNext   bool
	HasPrev   bool
	NextPage  int
	PrevPage  int
	Total     int
	Workflows []string
	Rows      []RunRow
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
	rng := firstQueryValue(q, "range")
	page, size := parsePageAndSize(firstQueryValue(q, "page"),
		firstQueryValue(q, "size"))
	runs, err := ds.ListRuns(ctx, wf)
	if err != nil {
		return RunsListView{}, fmt.Errorf("list runs: %w", err)
	}
	runs = filterRunsByStatus(runs, status)
	runs = filterRunsByRange(runs, rng, time.Now())
	defs, _ := ds.ListWorkflows(ctx)
	wfNames := workflowNamesFromDefs(defs)
	total := len(runs)
	start, end, hasNext := paginate(total, page, size)
	view := RunsListView{
		Workflow: wf, Status: status, Range: rng,
		Page: page, Size: size, Total: total,
		HasNext: hasNext, HasPrev: page > 1,
		NextPage: page + 1, PrevPage: page - 1,
		Workflows: wfNames,
		Rows:      toRunRows(runs[start:end]),
	}
	return view, nil
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
	if r.Status.IsTerminal() {
		row.Duration = "n/a"
	} else {
		row.Duration = formatDuration(time.Since(r.CreatedAt))
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
	Steps       []StepCard
	Events      []EventRow
	// DAGSVG is the live-state overlay rendering. Empty when the
	// workflow def can't be loaded or when the renderer returned an
	// error (cycle / too many steps).
	DAGSVG      template.HTML
	DAGFallback string
}

// StepCard is one cell in the step status grid.
type StepCard struct {
	ID       string
	Status   string
	Icon     string
	Attempts int
	Duration string
	HasError bool
	ErrorMsg string
	Skipped  bool
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
	renderPage(w, r, ts, cfg, "run-detail", pageData{
		Title:   "Run " + shortRunID(id),
		Section: "runs",
		Page:    view,
	})
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
	if defErr == nil {
		view.Steps = stepCardsFor(def, run)
		view.DAGSVG, view.DAGFallback = renderLiveDAG(def, &run)
	}
	events, _ := ds.ListRunEvents(ctx, id, false)
	view.Events = toEventRows(events)
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
	return view
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
	if run.Status.IsTerminal() {
		v.Duration = "n/a"
	} else {
		v.Duration = formatDuration(time.Since(run.CreatedAt))
	}
	return v
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

// stepCardsFor renders one card per step in the workflow definition,
// merged with the live state in the run snapshot. An empty Steps
// slice yields an empty card list — the template handles that.
func stepCardsFor(
	def dag.WorkflowDef, run dag.WorkflowRun,
) []StepCard {
	if len(def.Steps) == 0 {
		return nil
	}
	out := make([]StepCard, 0, len(def.Steps))
	for _, step := range def.Steps {
		state, ok := run.Steps[step.ID]
		card := StepCard{ID: step.ID}
		if !ok {
			card.Status = "pending"
			card.Icon = statusIcon("pending")
			out = append(out, card)
			continue
		}
		statusStr := state.Status.String()
		card.Status = statusStr
		card.Icon = statusIcon(statusStr)
		card.Attempts = state.Attempts
		card.Skipped = state.Status == dag.StepStatusSkipped
		if state.Status == dag.StepStatusFailed && state.Error != "" {
			card.HasError = true
			card.ErrorMsg = state.Error
		}
		out = append(out, card)
	}
	return out
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

// prettyJSON returns indented JSON. Invalid JSON is rendered verbatim
// — operators need to see what was actually stored, not an error.
func prettyJSON(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

// firstQueryValue is a nil-safe wrapper around url.Values lookup.
func firstQueryValue(q map[string][]string, key string) string {
	if key == "" {
		panic("firstQueryValue: key is empty")
	}
	if q == nil {
		return ""
	}
	v, ok := q[key]
	if !ok || len(v) == 0 {
		return ""
	}
	return v[0]
}

// parsePageAndSize parses URL strings into clamped page/size ints.
// Page < 1 defaults to 1; size out of bounds defaults / clamps to
// pageSizeDefault / pageSizeMax. Bounded per project rules.
func parsePageAndSize(pageStr, sizeStr string) (int, int) {
	page := 1
	if pageStr != "" {
		if v, err := strconv.Atoi(pageStr); err == nil && v >= 1 {
			if v > pageNumberMax {
				v = pageNumberMax
			}
			page = v
		}
	}
	size := pageSizeDefault
	if sizeStr != "" {
		if v, err := strconv.Atoi(sizeStr); err == nil && v >= 1 {
			if v > pageSizeMax {
				v = pageSizeMax
			}
			size = v
		}
	}
	return page, size
}

// paginate computes the safe [start, end) indices for a slice of
// length total given 1-indexed page and a positive size. Reports
// whether a next page exists. End is always clamped to total.
func paginate(total, page, size int) (int, int, bool) {
	if total < 0 {
		panic("paginate: total is negative")
	}
	if size <= 0 {
		panic("paginate: size must be positive")
	}
	start := (page - 1) * size
	if start >= total {
		return total, total, false
	}
	end := start + size
	if end > total {
		end = total
	}
	return start, end, end < total
}

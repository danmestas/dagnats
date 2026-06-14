package console

import (
	"bytes"
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
	// BuildInfo carries the build/identity footer payload (R9,
	// #320). renderPage populates it from cfg + a best-effort
	// ConfigSnapshot so every page surfaces a uniform footer
	// regardless of which handler renders the page.
	BuildInfo BuildInfo
	Page      any
	ReadOnly  bool
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
	pd.BuildInfo = buildBuildInfo(r.Context(), cfg)
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
//
// EmptyState is non-nil only on the first page with no filter and no
// rows — the template renders the shared empty-state partial in that
// case and skips it everywhere else (a filter that excluded all rows
// is a "no match" condition, not an empty system).
type WorkflowsListView struct {
	Header     PageHeader
	Filter     string
	Sort       string
	Page       int
	Size       int
	HasNext    bool
	HasPrev    bool
	NextPage   int
	PrevPage   int
	Total      int
	Rows       []WorkflowRow
	EmptyState *EmptyState

	// ReadOnly + CSRFToken carry the per-actor state the inline Run
	// button needs (#329). The template gates the affordance on
	// ReadOnly and embeds the token in the per-row hidden form.
	ReadOnly  bool
	CSRFToken string
}

// WorkflowRow is one workflow line on the list page.
//
// Sparkline carries hours-many activity buckets for the "Activity (24h)"
// column. Nil when the metrics aggregator is unavailable or the
// workflow has no recorded activity — the template renders the empty
// state in that case (no flat-line lies about all-zeros).
type WorkflowRow struct {
	Name          string
	Version       string
	StepCount     int
	TriggerCount  int
	LastRunTime   string
	LastRunStatus string
	Sparkline     []float64

	// Runs24h counts the workflow's runs whose CreatedAt falls within
	// the trailing 24h, folded from the same ListRuns the page already
	// reads — so the value is a real, complete count (zero is honest,
	// unlike the absent-metric case). TriggerKind is the kind of the
	// workflow's first trigger ("cron" | "subject" | "http" | "webhook"),
	// derived via triggerKindAndTarget; empty when the workflow has no
	// trigger so the template can render an honest dash.
	Runs24h     int
	TriggerKind string

	// Runnable is true when the workflow accepts an empty input —
	// the inline Run button (#329) only fires for those. Workflows
	// with a non-empty required-input schema render a disabled
	// affordance with a tooltip pointing at the workflow detail
	// page where typed-input forms will land in a follow-up.
	Runnable bool
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
	view.ReadOnly = cfg.ReadOnly
	view.CSRFToken = csrfTokenFor(r)
	if view.EmptyState != nil {
		view.EmptyState.ReadOnly = cfg.ReadOnly
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
	attachWorkflowSparklines(ctx, ds, rows)
	sortWorkflowRows(rows, sortKey)
	total := len(rows)
	start, end, hasNext := paginate(total, page, size)
	header := buildWorkflowsHeader(rows)
	view := WorkflowsListView{
		Header:   header,
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
	if total == 0 && filter == "" {
		view.EmptyState = newWorkflowsEmptyState()
	}
	return view, nil
}

// newWorkflowsEmptyState builds the EmptyState for the workflows list
// when no workflows are registered. Returns nil on validation failure
// (defensive — the strings here are static so the path is unreachable
// in practice, but the contract says callers tolerate a nil partial).
func newWorkflowsEmptyState() *EmptyState {
	e, err := NewEmptyState(EmptyState{
		Icon:        "workflow",
		Title:       "No workflows registered",
		Description: "Register a workflow definition to start scheduling runs.",
		PrimaryAction: &EmptyStateAction{
			Label: "Read the docs",
			Href:  "/docs",
		},
	})
	if err != nil {
		return nil
	}
	return &e
}

// buildWorkflowsHeader projects the full row set into the count tiles
// shown above the workflows table. "Active" = at least one recorded
// run; "Draft" = registered but never run. Counting happens before the
// pagination slice so the totals reflect every workflow, not just the
// current page.
func buildWorkflowsHeader(rows []WorkflowRow) PageHeader {
	active := 0
	for i := range rows {
		if rows[i].LastRunTime != "" {
			active++
		}
	}
	draft := len(rows) - active
	tiles := []Tile{
		{Label: "workflows", Count: len(rows), Tone: ToneDefault},
		{Label: "active", Count: active, Tone: ToneSuccess,
			Tooltip: "Workflows with at least one recorded run"},
		{Label: "draft", Count: draft, Tone: ToneInfo,
			Tooltip: "Registered workflows that have never run"},
	}
	h, err := NewPageHeader(PageHeader{
		Title:    "Workflows",
		Subtitle: "Registered workflow definitions.",
		Tiles:    tiles,
	})
	if err != nil {
		// Validation errors are programmer errors — the tone constants
		// and labels above are static. Fall back to a bare header so a
		// future bug surfaces in the request log, not as a 500.
		return PageHeader{Title: "Workflows"}
	}
	return h
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
	triggerKind := firstTriggerKindPerWorkflow(triggers)
	runs24h := runCountWithin(runs, sparklineHours*time.Hour, time.Now())
	lastRun := lastRunPerWorkflow(runs)
	rows := make([]WorkflowRow, 0, len(defs))
	for _, d := range defs {
		row := WorkflowRow{
			Name:         d.Name,
			Version:      d.Version,
			StepCount:    len(d.Steps),
			TriggerCount: triggerCount[d.Name],
			Runs24h:      runs24h[d.Name],
			TriggerKind:  triggerKind[d.Name],
			Runnable:     workflowRunnable(d),
		}
		if lr, ok := lastRun[d.Name]; ok {
			row.LastRunTime = lr.CreatedAt.UTC().Format(time.RFC3339)
			row.LastRunStatus = lr.Status.String()
		}
		rows = append(rows, row)
	}
	return rows
}

// firstTriggerKindPerWorkflow maps each workflow ID to the kind of its
// first trigger (in iteration order), reusing triggerKindAndTarget so
// the kind labels match every other surface. Workflows with no trigger
// are simply absent from the map. A nil triggers slice yields an empty
// map. Bounded by len(triggers).
func firstTriggerKindPerWorkflow(
	triggers []trigger.TriggerDef,
) map[string]string {
	const triggersMax = 4096
	if len(triggers) > triggersMax {
		panic("firstTriggerKindPerWorkflow: triggers exceeds max bound")
	}
	out := make(map[string]string, len(triggers))
	for i := 0; i < len(triggers); i++ {
		t := triggers[i]
		if _, seen := out[t.WorkflowID]; seen {
			continue
		}
		kind, _ := triggerKindAndTarget(t)
		out[t.WorkflowID] = kind
	}
	return out
}

// runCountWithin folds runs into a per-workflow count of those whose
// CreatedAt lands within the trailing window ending at now. The runs
// slice is the complete ListRuns read, so the resulting count is real —
// zero means "no runs in window", never "no data".
//
// The loop caps at runsMax rather than panicking: this fold backs the
// /console/workflows page, which reads the same unbounded ListRuns the
// /console/runs page does. buildRunsHeader truncates at runsMax, so a
// deployment with >runsMax total runs must degrade /workflows to an
// undercount the same way — never a 500. Same data, same bound, same
// failure mode.
func runCountWithin(
	runs []dag.WorkflowRun, window time.Duration, now time.Time,
) map[string]int {
	if window <= 0 {
		panic("runCountWithin: window must be positive")
	}
	cutoff := now.Add(-window)
	out := make(map[string]int, len(runs))
	for i := 0; i < len(runs) && i < runsMax; i++ {
		if runs[i].CreatedAt.After(cutoff) {
			out[runs[i].WorkflowID]++
		}
	}
	return out
}

// workflowRunnable reports whether the workflow can be started from
// the inline Run button with an empty input payload. A workflow is
// runnable when it declares no input schema OR declares a schema that
// has no required properties — in both cases an empty `{}` payload
// passes validation and the engine has everything it needs.
//
// The check is conservative: any JSON-parse failure on InputSchema
// classifies the workflow as not-runnable so a malformed schema can't
// silently allow the operator to start a run the engine will reject.
func workflowRunnable(def dag.WorkflowDef) bool {
	if len(def.InputSchema) == 0 {
		return true
	}
	var schema struct {
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(def.InputSchema, &schema); err != nil {
		return false
	}
	return len(schema.Required) == 0
}

// sparklineHours is the canonical request window for list-row
// sparklines: 24 hourly buckets covering the trailing day. Lives here
// so the trigger path and the workflow path agree on the resolution.
const sparklineHours = 24

// runsMax is the safety bound on every fold over the ListRuns slice
// (the runs table header and the workflows-page run counts). Folds cap
// at this many runs and degrade to an undercount rather than panic, so
// a deployment with more total runs never 500s a list page.
const runsMax = 100_000

// attachWorkflowSparklines fetches the 24h activity series for each
// row and attaches it in place. Errors are swallowed — sparkline is a
// progressive enhancement, not load-bearing — but logged via slog
// elsewhere when DataSource surfaces them. Bounded loop by rows.
func attachWorkflowSparklines(
	ctx context.Context, ds DataSource, rows []WorkflowRow,
) {
	if ds == nil {
		return
	}
	if ctx == nil {
		panic("attachWorkflowSparklines: ctx is nil")
	}
	for i := range rows {
		data, err := ds.SparklineData(ctx, "workflow", rows[i].Name, sparklineHours)
		if err != nil {
			continue
		}
		rows[i].Sparkline = data
	}
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
	return view
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
	if since > 0 || until > 0 {
		runs = filterRunsByWindow(runs, since, until)
	} else {
		runs = filterRunsByRange(runs, rng, time.Now())
	}
	defs, _ := ds.ListWorkflows(ctx)
	wfNames := workflowNamesFromDefs(defs)
	total := len(runs)
	start, end, hasNext := paginate(total, page, size)
	first, last := 0, 0
	if total > 0 && end > start {
		first = start + 1
		last = end
	}
	view := RunsListView{
		Header:   buildRunsHeader(runs, time.Now()),
		Workflow: wf, Status: status, Range: rng,
		SinceUnix: since, UntilUnix: until,
		Page: page, Size: size, Total: total,
		HasNext: hasNext, HasPrev: page > 1,
		NextPage: page + 1, PrevPage: page - 1,
		FirstIndex: first, LastIndex: last,
		Workflows: wfNames,
		Rows:      toRunRows(runs[start:end]),
	}
	if total == 0 && wf == "" && status == "" &&
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
	// The runs list reads only the run snapshot — no per-run history —
	// so terminal runs have no end timestamp here. Show the honest "—"
	// rather than a synthetic value; the run-detail page derives the
	// real terminal duration from the run's history events. In-flight
	// runs render a labelled elapsed time so it can't be mistaken for a
	// final duration.
	if r.Status.IsTerminal() {
		row.Duration = "—"
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

// dlqModalView powers the shared dlq-action-modal partial when it's
// served as a stand-alone fragment from /console/api/dlq/<seq>/confirm.
// On the list + detail full-page renders the modal pulls its data from
// the per-row hidden <form> elements; the fragment endpoint exists for
// callers that want the modal markup ad-hoc (e.g. Datastar @get from
// the list's row button if the operator wants the server to be the
// authority on the typed-confirm word + reason text instead of the
// client-rendered fallback).
type dlqModalView struct {
	Sequence   uint64
	Action     string
	ReasonFull string
	Workflow   string
	CSRFToken  string
	ReadOnly   bool
}

// serveDLQConfirmFragment renders the typed-confirm modal as a stand-
// alone HTML fragment for the given DLQ sequence + action. The
// response body is the modal markup + its inline JS; callers either
// inject it into a body-level container or read it for parity checks
// against the full-page render.
//
// The endpoint accepts GET to keep it cacheable per-request and to
// match the @get('...') idiom Datastar uses for hypermedia fetches;
// it does not mutate state.
func serveDLQConfirmFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveDLQConfirmFragment: w is nil")
	}
	if r == nil {
		panic("serveDLQConfirmFragment: r is nil")
	}
	if ts == nil {
		panic("serveDLQConfirmFragment: ts is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	seqStr := dlqConfirmSeqFromPath(r.URL.Path)
	if seqStr == "" {
		http.NotFound(w, r)
		return
	}
	action := strings.ToLower(r.URL.Query().Get("action"))
	if action != "retry" && action != "discard" {
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	ds, ok := requireData(w, cfg, "dlq-confirm-fragment")
	if !ok {
		return
	}
	view, ok := buildDLQModalView(r, ds, cfg, seqStr, action)
	if !ok {
		http.NotFound(w, r)
		return
	}
	html, err := renderFragment(ts.base, "dlq-action-modal-fragment", view)
	if err != nil {
		cfg.Logger.Error("console: render dlq-action-modal-fragment",
			"err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := w.Write([]byte(html)); err != nil {
		cfg.Logger.Warn("console: write dlq-confirm-fragment",
			"err", err)
	}
}

// dlqConfirmSeqFromPath extracts the <seq> token from a URL of the
// shape /console/api/dlq/<seq>/confirm. Returns the empty string for
// any malformed path so the caller can 404.
func dlqConfirmSeqFromPath(path string) string {
	if path == "" {
		return ""
	}
	const prefix = "/console/api/dlq/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	const suffix = "/confirm"
	if !strings.HasSuffix(rest, suffix) {
		return ""
	}
	seq := strings.TrimSuffix(rest, suffix)
	if seq == "" || strings.Contains(seq, "/") {
		return ""
	}
	return seq
}

// buildDLQModalView resolves seqStr against the data source and
// returns the modal-binding view. ok=false on missing/garbage seq so
// the caller can 404 instead of rendering an empty modal.
func buildDLQModalView(
	r *http.Request, ds DataSource, cfg Config,
	seqStr, action string,
) (dlqModalView, bool) {
	if r == nil {
		panic("buildDLQModalView: r is nil")
	}
	if ds == nil {
		panic("buildDLQModalView: ds is nil")
	}
	detail := buildDLQDetail(r.Context(), ds, seqStr)
	if detail.NotFound {
		return dlqModalView{}, false
	}
	return dlqModalView{
		Sequence:   detail.Sequence,
		Action:     action,
		ReasonFull: detail.ReasonFull,
		Workflow:   detail.Workflow,
		CSRFToken:  csrfTokenFor(r),
		ReadOnly:   cfg.ReadOnly,
	}, true
}

// searchLimitDefault bounds the palette result list. 10 is enough to
// scroll through visually without paginating; the search is fast even
// against thousands of workflows because we cap before the slice
// crosses the wire.
const searchLimitDefault = 10

// serveSearch backs the cmd+k palette. GET /console/api/search?q=<term>
// returns the command-results partial wrapped in a Datastar
// PatchElements SSE event so the client patches the list without a
// full page reload. The endpoint accepts an empty query and renders
// the explicit "No results." copy — the operator must always see
// honest feedback, not stale rows from the previous keystroke.
func serveSearch(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSearch: w is nil")
	}
	if r == nil {
		panic("serveSearch: r is nil")
	}
	if ts == nil {
		panic("serveSearch: ts is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ds, ok := requireData(w, cfg, "search")
	if !ok {
		return
	}
	hits, err := ds.Search(r.Context(), r.URL.Query().Get("q"), searchLimitDefault)
	if err != nil {
		cfg.Logger.Error("console: search", "err", err)
		http.Error(w, "search failed", http.StatusInternalServerError)
		return
	}
	html, err := renderFragment(ts.base, "command-results", hits)
	if err != nil {
		cfg.Logger.Error("console: render command-results", "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	sse := datastar.NewSSE(w, r)
	patchErr := sse.PatchElements(html,
		datastar.WithSelectorID("command-results"),
		datastar.WithModeInner())
	if patchErr != nil {
		cfg.Logger.Warn("console: search patch elements", "err", patchErr)
	}
}

// sheetView packages the fields the side-sheet shell template binds.
// BodyTemplate is a switch key the shell uses to pick which inner
// partial to render against Data; using a switch keeps html/template's
// type-safe contract intact (calling {{template .X .Y}} dynamically
// requires every name to be parse-time known).
type sheetView struct {
	Title        string
	BodyTemplate string
	Data         any
	FullPageHref string
}

// dispatchDLQAPIFragment routes /console/api/dlq/<seq>/{confirm,sheet}
// to the matching handler. Both endpoints share the prefix because
// the mux is registered on /console/api/dlq/ as a catch-all; the
// suffix selects the response shape. Unknown suffixes 404.
func dispatchDLQAPIFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchDLQAPIFragment: w is nil")
	}
	if r == nil {
		panic("dispatchDLQAPIFragment: r is nil")
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/confirm"):
		serveDLQConfirmFragment(w, r, ts, cfg)
	case strings.HasSuffix(r.URL.Path, "/sheet"):
		serveDLQSheet(w, r, ts, cfg)
	default:
		http.NotFound(w, r)
	}
}

// serveDLQSheet renders /console/api/dlq/<seq>/sheet as a Datastar
// PatchElements SSE event that patches the side-sheet markup into
// #sheet-outlet (inner mode). The shell binds the dlq-sheet-body
// partial against the DLQDetailView.
func serveDLQSheet(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveDLQSheet: w is nil")
	}
	if r == nil {
		panic("serveDLQSheet: r is nil")
	}
	if ts == nil {
		panic("serveDLQSheet: ts is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	seqStr := sheetSeqFromPath(r.URL.Path, "/console/api/dlq/", "/sheet")
	if seqStr == "" {
		http.NotFound(w, r)
		return
	}
	ds, ok := requireData(w, cfg, "dlq-sheet")
	if !ok {
		return
	}
	detail := buildDLQDetail(r.Context(), ds, seqStr)
	if detail.NotFound {
		http.NotFound(w, r)
		return
	}
	view := sheetView{
		Title:        "DLQ entry #" + seqStr,
		BodyTemplate: "dlq-sheet-body",
		Data:         detail,
		FullPageHref: "/console/dlq/" + seqStr,
	}
	emitSheetFragment(w, r, ts, cfg, view, "dlq-sheet")
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

// emitSheetFragment renders the side-sheet shell with the given view
// and emits the result as a Datastar PatchElements event targeting
// #sheet-outlet in inner mode. The label is included in slog warnings
// so a render or write failure is easy to attribute.
func emitSheetFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
	view sheetView, label string,
) {
	if ts == nil {
		panic("emitSheetFragment: ts is nil")
	}
	if label == "" {
		panic("emitSheetFragment: label is empty")
	}
	html, err := renderFragment(ts.base, "side-sheet", view)
	if err != nil {
		cfg.Logger.Error("console: render side-sheet",
			"label", label, "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	sse := datastar.NewSSE(w, r)
	patchErr := sse.PatchElements(html,
		datastar.WithSelectorID("sheet-outlet"),
		datastar.WithModeInner())
	if patchErr != nil {
		cfg.Logger.Warn("console: sheet patch elements",
			"label", label, "err", patchErr)
	}
}

// sheetSeqFromPath extracts the <seq> token from a URL of the shape
// `<prefix><seq><suffix>`. Returns "" when the path doesn't match or
// the seq contains a slash. The pure-string version is enough — the
// caller validates the seq numerically when it looks up the entry.
func sheetSeqFromPath(path, prefix, suffix string) string {
	if path == "" || prefix == "" || suffix == "" {
		return ""
	}
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	if !strings.HasSuffix(rest, suffix) {
		return ""
	}
	seq := strings.TrimSuffix(rest, suffix)
	if seq == "" || strings.Contains(seq, "/") {
		return ""
	}
	return seq
}

// sheetSlugFromPath is sheetSeqFromPath with run-id-friendly
// validation. Same logic — kept distinct so the call sites read
// clearly at the route layer.
func sheetSlugFromPath(path, prefix, suffix string) string {
	return sheetSeqFromPath(path, prefix, suffix)
}

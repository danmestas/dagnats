package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

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
	StepCount      int
	Steps          []WorkflowStepRow
	Warnings       []dag.Warning
	Triggers       []TriggerLine
	RecentRuns     []RunRow
	NotFound       bool
	// Runnable mirrors WorkflowRow.Runnable on the list: true when the
	// workflow can be started from the console with no typed input.
	// Computed in buildWorkflowDetail via workflowRunnable(def).
	Runnable bool
	// ReadOnly and CSRFToken power the header Run button. Populated by
	// servePageWorkflowDetail post-build, mirroring the list handler.
	ReadOnly  bool
	CSRFToken string
}

// WorkflowStepRow is one node in the rendered step-DAG. Num is the
// 1-based position; IsEntry is true when the step has no dependencies.
// TypeClass is a CSS-safe suffix for coloring the type pill.
type WorkflowStepRow struct {
	Num       int
	Name      string
	TypeLabel string
	TypeClass string
	DependsOn []string
	IsEntry   bool
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
	view.ReadOnly = cfg.ReadOnly
	view.CSRFToken = csrfTokenFor(r)
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
	steps := workflowStepRows(def.Steps)
	view := WorkflowDetailView{
		Name:       name,
		Version:    def.Version,
		Definition: string(defJSON),
		StepCount:  len(steps),
		Steps:      steps,
		Warnings:   warnings,
		Triggers:   attached,
		RecentRuns: toRunRows(runs),
		Runnable:   workflowRunnable(def),
	}
	return view
}

// workflowStepRows projects definition steps into render rows for the
// numbered step-DAG. A nil steps slice is a valid empty input — a
// workflow with no steps renders an empty DAG, not a panic.
func workflowStepRows(steps []dag.StepDef) []WorkflowStepRow {
	if steps == nil {
		return nil
	}
	out := make([]WorkflowStepRow, 0, len(steps))
	for i := range steps {
		label := steps[i].Type.String()
		out = append(out, WorkflowStepRow{
			Num:       i + 1,
			Name:      steps[i].ID,
			TypeLabel: label,
			TypeClass: label,
			DependsOn: steps[i].DependsOn,
			IsEntry:   len(steps[i].DependsOn) == 0,
		})
	}
	if len(out) != len(steps) {
		panic("workflowStepRows: row count diverged from step count")
	}
	return out
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

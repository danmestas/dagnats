// ui/handlers.go
// HTTP handlers for the DagNats dashboard. Renders HTML pages using
// Go templates with Basecoat UI components and Datastar attributes
// for real-time reactivity via SSE.
package ui

import (
	"context"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

// Handler serves the DagNats dashboard UI.
type Handler struct {
	svc       *api.Service
	templates *template.Template
}

// pageData is the base data passed to all page templates.
type pageData struct {
	Title      string
	ActivePage string
}

// dashboardData extends pageData for the dashboard page.
type dashboardData struct {
	pageData
	TotalRuns     int
	ActiveRuns    int
	CompletedRuns int
	FailedRuns    int
	RecentRuns    []dag.WorkflowRun
}

// workflowListData extends pageData for the workflow list.
type workflowListData struct {
	pageData
	Workflows []dag.WorkflowDef
}

// workflowDetailData extends pageData for a single workflow.
type workflowDetailData struct {
	pageData
	Workflow   dag.WorkflowDef
	DAG        template.HTML
	RecentRuns []dag.WorkflowRun
}

// runsListData extends pageData for the runs table.
type runsListData struct {
	pageData
	Runs       []dag.WorkflowRun
	FilterWF   string
	FilterStat string
}

// runDetailData extends pageData for a single run.
type runDetailData struct {
	pageData
	Run     dag.WorkflowRun
	Def     dag.WorkflowDef
	DAG     template.HTML
	Gantt   template.HTML
	Events  []protocol.Event
	ActiveTab string
}

// NewHandler creates a Handler with parsed templates. Panics if
// template parsing fails — templates are embedded and must be valid.
func NewHandler(svc *api.Service) *Handler {
	if svc == nil {
		panic("NewHandler: svc must not be nil")
	}
	funcMap := template.FuncMap{
		"statusClass":    statusClass,
		"statusIcon":     statusIcon,
		"formatTime":     formatTime,
		"formatDuration": formatDuration,
		"truncate":       truncate,
		"json":           jsonString,
	}
	tmplFS, err := fs.Sub(templateFS, "templates")
	if err != nil {
		panic("NewHandler: fs.Sub failed: " + err.Error())
	}
	tmpl, err := template.New("").
		Funcs(funcMap).
		ParseFS(tmplFS, "*.html", "partials/*.html")
	if err != nil {
		panic("NewHandler: template parse failed: " +
			err.Error())
	}
	return &Handler{svc: svc, templates: tmpl}
}

// RegisterRoutes adds UI routes to the given ServeMux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	if mux == nil {
		panic("RegisterRoutes: mux must not be nil")
	}
	// HTML pages.
	mux.HandleFunc("/", h.handleDashboard)
	mux.HandleFunc("/workflows", h.handleWorkflows)
	mux.HandleFunc("/workflows/", h.handleWorkflowDetail)
	mux.HandleFunc("/runs", h.handleRuns)
	mux.HandleFunc("/runs/", h.handleRunDetail)

	// Datastar SSE endpoints.
	mux.HandleFunc(
		"/ui/dashboard/live", h.handleDashboardLive,
	)
	mux.HandleFunc("/ui/runs/", h.handleRunLive)

	// Static assets.
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic("RegisterRoutes: static fs.Sub: " + err.Error())
	}
	mux.Handle("/static/",
		http.StripPrefix("/static/",
			http.FileServer(http.FS(staticSub)),
		),
	)
}

// handleDashboard renders the main dashboard with metrics.
func (h *Handler) handleDashboard(
	w http.ResponseWriter, r *http.Request,
) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	ctx := context.Background()
	runs, _ := h.svc.ListRuns(ctx, "", "")

	data := dashboardData{
		pageData: pageData{
			Title:      "Dashboard",
			ActivePage: "dashboard",
		},
	}
	data.TotalRuns = len(runs)
	for _, run := range runs {
		switch run.Status {
		case dag.RunStatusRunning, dag.RunStatusPending:
			data.ActiveRuns++
		case dag.RunStatusCompleted:
			data.CompletedRuns++
		case dag.RunStatusFailed:
			data.FailedRuns++
		}
	}

	// Recent runs: last 10, newest first.
	recent := runs
	sortRunsByCreatedDesc(recent)
	if len(recent) > 10 {
		recent = recent[:10]
	}
	data.RecentRuns = recent

	h.render(w, "dashboard.html", data)
}

// handleWorkflows renders the workflow list page.
func (h *Handler) handleWorkflows(
	w http.ResponseWriter, r *http.Request,
) {
	ctx := context.Background()
	defs, _ := h.svc.ListWorkflows(ctx)
	data := workflowListData{
		pageData: pageData{
			Title:      "Workflows",
			ActivePage: "workflows",
		},
		Workflows: defs,
	}
	h.render(w, "workflows.html", data)
}

// handleWorkflowDetail renders a single workflow with DAG.
func (h *Handler) handleWorkflowDetail(
	w http.ResponseWriter, r *http.Request,
) {
	name := strings.TrimPrefix(r.URL.Path, "/workflows/")
	if name == "" {
		http.NotFound(w, r)
		return
	}
	def, err := h.svc.GetWorkflow(name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	ctx := context.Background()
	runs, _ := h.svc.ListRuns(ctx, name, "")
	sortRunsByCreatedDesc(runs)
	if len(runs) > 10 {
		runs = runs[:10]
	}

	// Build DAG with empty step states (definition view).
	emptySteps := make(map[string]dag.StepState, len(def.Steps))
	for _, s := range def.Steps {
		emptySteps[s.ID] = dag.StepState{}
	}
	svg := buildDAGSVG(def, emptySteps)

	data := workflowDetailData{
		pageData: pageData{
			Title:      "Workflow: " + name,
			ActivePage: "workflows",
		},
		Workflow:   def,
		DAG:        renderDAGSVG(svg),
		RecentRuns: runs,
	}
	h.render(w, "workflow.html", data)
}

// handleRuns renders the runs table with optional filters.
func (h *Handler) handleRuns(
	w http.ResponseWriter, r *http.Request,
) {
	ctx := context.Background()
	wf := r.URL.Query().Get("workflow")
	st := r.URL.Query().Get("status")
	runs, _ := h.svc.ListRuns(ctx, wf, st)
	sortRunsByCreatedDesc(runs)

	data := runsListData{
		pageData: pageData{
			Title:      "Runs",
			ActivePage: "runs",
		},
		Runs:       runs,
		FilterWF:   wf,
		FilterStat: st,
	}
	h.render(w, "runs.html", data)
}

// handleRunDetail renders a single run with tabs.
func (h *Handler) handleRunDetail(
	w http.ResponseWriter, r *http.Request,
) {
	path := strings.TrimPrefix(r.URL.Path, "/runs/")
	runID := strings.Split(path, "/")[0]
	if runID == "" {
		http.NotFound(w, r)
		return
	}
	ctx := context.Background()
	run, err := h.svc.GetRun(ctx, runID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	def, _ := h.svc.GetWorkflow(run.WorkflowID)
	events, _ := h.svc.GetRunEvents(ctx, runID)

	svg := buildDAGSVG(def, run.Steps)
	gantt := buildGanttSVG(events)
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "overview"
	}

	data := runDetailData{
		pageData: pageData{
			Title:      "Run: " + runID[:8],
			ActivePage: "runs",
		},
		Run:       run,
		Def:       def,
		DAG:       renderDAGSVG(svg),
		Gantt:     renderGanttSVG(gantt),
		Events:    events,
		ActiveTab: tab,
	}
	h.render(w, "run.html", data)
}

// handleDashboardLive serves SSE for dashboard auto-refresh.
// Sends updated metric cards every 3 seconds.
func (h *Handler) handleDashboardLive(
	w http.ResponseWriter, r *http.Request,
) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported",
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	const maxTicks = 600
	for i := 0; i < maxTicks; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
		runs, _ := h.svc.ListRuns(
			context.Background(), "", "",
		)
		active, completed, failed := 0, 0, 0
		for _, run := range runs {
			switch run.Status {
			case dag.RunStatusRunning,
				dag.RunStatusPending:
				active++
			case dag.RunStatusCompleted:
				completed++
			case dag.RunStatusFailed:
				failed++
			}
		}
		html := fmt.Sprintf(
			`<div id="metric-total" `+
				`class="metric-value">%d</div>`,
			len(runs),
		)
		writeSSE(w, "datastar-patch-elements", html)
		html = fmt.Sprintf(
			`<div id="metric-active" `+
				`class="metric-value metric-active">`+
				`%d</div>`,
			active,
		)
		writeSSE(w, "datastar-patch-elements", html)
		html = fmt.Sprintf(
			`<div id="metric-completed" `+
				`class="metric-value metric-completed">`+
				`%d</div>`,
			completed,
		)
		writeSSE(w, "datastar-patch-elements", html)
		html = fmt.Sprintf(
			`<div id="metric-failed" `+
				`class="metric-value metric-failed">`+
				`%d</div>`,
			failed,
		)
		writeSSE(w, "datastar-patch-elements", html)
		flusher.Flush()
	}
}

// handleRunLive serves SSE for live run detail updates.
// Subscribes to run state and patches DAG + step table.
func (h *Handler) handleRunLive(
	w http.ResponseWriter, r *http.Request,
) {
	path := strings.TrimPrefix(r.URL.Path, "/ui/runs/")
	runID := strings.TrimSuffix(path, "/live")
	if runID == "" {
		http.Error(w, "missing run ID", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported",
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	const maxPolls = 300
	for i := 0; i < maxPolls; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
		run, err := h.svc.GetRun(
			context.Background(), runID,
		)
		if err != nil {
			continue
		}
		def, err := h.svc.GetWorkflow(run.WorkflowID)
		if err != nil {
			continue
		}

		// Re-render DAG with current step states.
		svg := buildDAGSVG(def, run.Steps)
		dagHTML := string(renderDAGSVG(svg))
		writeSSE(w, "datastar-patch-elements",
			`<div id="live-dag">`+dagHTML+`</div>`)

		// Re-render step status table.
		stepsHTML := renderStepTable(def, run)
		writeSSE(w, "datastar-patch-elements",
			`<div id="live-steps">`+stepsHTML+`</div>`)

		// Re-render run status badge.
		badgeHTML := fmt.Sprintf(
			`<span id="run-status" class="badge %s">`+
				`%s</span>`,
			statusClass(run.Status.String()),
			run.Status.String(),
		)
		writeSSE(w, "datastar-patch-elements", badgeHTML)

		flusher.Flush()

		// Stop polling when run is terminal.
		if run.Status == dag.RunStatusCompleted ||
			run.Status == dag.RunStatusFailed ||
			run.Status == dag.RunStatusCancelled {
			return
		}
	}
}

// writeSSE writes a single Datastar SSE event. The elements
// keyword tells Datastar this is an HTML fragment to patch.
func writeSSE(w http.ResponseWriter, event, data string) {
	fmt.Fprintf(w, "event: %s\ndata: elements %s\n\n",
		event, strings.ReplaceAll(data, "\n", ""))
}

// render executes a named template and writes the result.
func (h *Handler) render(
	w http.ResponseWriter, name string, data any,
) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := h.templates.ExecuteTemplate(w, name, data)
	if err != nil {
		http.Error(w, "template error: "+err.Error(),
			http.StatusInternalServerError)
	}
}

// renderStepTable builds an HTML table of step states.
func renderStepTable(
	def dag.WorkflowDef, run dag.WorkflowRun,
) string {
	var b strings.Builder
	b.WriteString(`<table class="table"><thead><tr>` +
		`<th>Step</th><th>Task</th><th>Status</th>` +
		`<th>Attempts</th><th>Iterations</th>` +
		`</tr></thead><tbody>`)
	for _, step := range def.Steps {
		st := run.Steps[step.ID]
		fmt.Fprintf(&b,
			`<tr><td class="font-mono">%s</td>`+
				`<td>%s</td>`+
				`<td><span class="badge %s">%s</span></td>`+
				`<td>%d</td><td>%d</td></tr>`,
			step.ID, step.Task,
			statusClass(st.Status.String()),
			st.Status.String(),
			st.Attempts, st.Iterations,
		)
	}
	b.WriteString(`</tbody></table>`)
	return b.String()
}

// sortRunsByCreatedDesc sorts runs newest-first in-place.
func sortRunsByCreatedDesc(runs []dag.WorkflowRun) {
	// Simple insertion sort — bounded by maxRuns (1000).
	for i := 1; i < len(runs); i++ {
		key := runs[i]
		j := i - 1
		for j >= 0 && runs[j].CreatedAt.Before(key.CreatedAt) {
			runs[j+1] = runs[j]
			j--
		}
		runs[j+1] = key
	}
}

// Template helper functions.

func statusClass(s string) string {
	switch s {
	case "completed":
		return "badge-success"
	case "failed":
		return "badge-destructive"
	case "running":
		return "badge-warning"
	case "queued":
		return "badge-info"
	case "pending":
		return "badge-secondary"
	case "skipped":
		return "badge-outline"
	default:
		return "badge-secondary"
	}
}

func statusIcon(s string) string {
	switch s {
	case "completed":
		return "check-circle"
	case "failed":
		return "x-circle"
	case "running":
		return "loader"
	case "queued":
		return "clock"
	case "pending":
		return "circle"
	case "skipped":
		return "skip-forward"
	default:
		return "circle"
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04:05")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func jsonString(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	s := string(b)
	if len(s) > 500 {
		return s[:500] + "..."
	}
	return s
}

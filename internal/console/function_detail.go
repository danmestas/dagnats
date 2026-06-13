// function_detail.go owns /console/functions/{name} — the read-only
// drill-in from the Functions (task-type) list. It mirrors the
// worker-detail block in ops_pages.go: dispatch -> serve -> build ->
// view struct, with the pure projection helpers split out so they test
// without an adapter.
//
// Honesty boundary. A function (task type) has no first-class record;
// it is referenced by workers (who serve it) and by workflow steps (who
// call it). So the page surfaces only what is genuinely backed:
//   - Providers: AggregateTaskTypes gives the owner worker ids; the
//     live worker rows give each one's status. Real join, no synthesis.
//   - Recent invocations: a run exercises a function iff its workflow
//     has a step whose Task equals the function name. Built by
//     cross-referencing ListRuns against GetWorkflow — real runs, real
//     reference, no new engine query. Columns are Time | Run id | Status
//     only: runRowFromRun cannot derive a terminal duration from the run
//     snapshot, so a Duration column would be a wall of em-dashes (the
//     same dead-column trap that dropped the worker-detail counter tiles).
//   - Contract schema: schemas live on WorkflowDef, not the function.
//     Rendered ONLY when exactly one referencing workflow exposes a
//     non-empty schema, attributed to that workflow. Multiple referencing
//     workflows -> ambiguous -> omitted rather than falsely attributed.
//
// Deliberately absent (no backing data / mutation): the Invoke modal
// (functions are called by workflows, not invoked directly — StartRun
// starts a workflow, not a bare task type), the per-function telemetry
// tiles + sparkline (no per-function histogram feed), and the providers
// "In-flight" column (not on the registration).
package console

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/danmestas/dagnats/dag"
)

// FunctionDetailView powers the read-only /console/functions/{name}
// page. NotFound flags an unknown function name (renders the honest
// not-found state, still 200 with chrome). Health is "healthy" when at
// least one provider has a live worker status, "no worker" otherwise.
// InputSchema/OutputSchema are non-empty only when a single referencing
// workflow exposes them, in which case SchemaSourceWorkflow names it.
type FunctionDetailView struct {
	Function             string
	NotFound             bool
	Service              string
	Health               string
	Providers            []FunctionProviderRow
	Invocations          []RunRow
	InputSchema          string
	OutputSchema         string
	SchemaSourceWorkflow string
}

// FunctionProviderRow is one worker registered to serve the function.
// Status is the live status from the worker rows ("active"/"stale"), or
// "no worker" when the owner id is absent from the live directory.
type FunctionProviderRow struct {
	WorkerID string
	Status   string
}

// invocationsMax bounds the invocations table so a deployment with a
// large run history can't render an unbounded page.
const invocationsMax = 50

// dispatchFunctions routes /console/functions/<name> to the read-only
// detail view. The trailing-slash prefix lands here; an empty or
// embedded-slash name 404s (mirrors dispatchWorkers). The exact
// /console/functions path is served separately by servePageTaskTypes.
func dispatchFunctions(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchFunctions: w is nil")
	}
	if r == nil {
		panic("dispatchFunctions: r is nil")
	}
	name := strings.TrimPrefix(r.URL.Path, "/console/functions/")
	if name == "" || strings.Contains(name, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	servePageFunctionDetail(w, r, ts, cfg, name)
}

// servePageFunctionDetail renders the read-only detail for one function.
// A read miss or unknown name degrades to the honest not-found state
// within the page chrome — the view is observational and never 500s.
func servePageFunctionDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, name string,
) {
	if w == nil {
		panic("servePageFunctionDetail: w is nil")
	}
	if name == "" {
		panic("servePageFunctionDetail: name is empty")
	}
	ds, ok := requireData(w, cfg, "function-detail")
	if !ok {
		return
	}
	view := buildFunctionDetailView(r.Context(), ds, name)
	renderPage(w, r, ts, cfg, "function-detail", pageData{
		Title:   "Function " + name,
		Section: "functions",
		Page:    view,
	})
}

// buildFunctionDetailView assembles the view from the DataSource. An
// unknown function returns NotFound so the page paints the honest empty
// state. Every read miss degrades to empty rather than erroring — the
// page is observational.
func buildFunctionDetailView(
	ctx context.Context, ds DataSource, name string,
) FunctionDetailView {
	if ctx == nil {
		panic("buildFunctionDetailView: ctx is nil")
	}
	if name == "" {
		panic("buildFunctionDetailView: name is empty")
	}
	rows, _ := ds.AggregateTaskTypes(ctx)
	row, found := findTaskTypeRow(rows, name)
	if !found {
		return FunctionDetailView{Function: name, NotFound: true}
	}
	statusByID := workerStatusByID(workerRowsOrEmpty(ctx, ds))
	providers := functionProvidersFrom(row.OwnerWorkerIDs, statusByID)
	defs := referencingWorkflows(ctx, ds, name)
	inSchema, outSchema, src := functionSchemaFor(defs, name)
	return FunctionDetailView{
		Function:             name,
		Service:              row.Service,
		Health:               functionHealth(providers),
		Providers:            providers,
		Invocations:          functionInvocations(ctx, ds, name),
		InputSchema:          inSchema,
		OutputSchema:         outSchema,
		SchemaSourceWorkflow: src,
	}
}

// findTaskTypeRow returns the row whose TaskType matches name. Bounded by
// len(rows). The bool reports a hit so an empty row is distinguishable
// from a real one.
func findTaskTypeRow(rows []TaskTypeRow, name string) (TaskTypeRow, bool) {
	const rowsMax = 10000
	for i := 0; i < len(rows) && i < rowsMax; i++ {
		if rows[i].TaskType == name {
			return rows[i], true
		}
	}
	return TaskTypeRow{}, false
}

// workerRowsOrEmpty reads the live worker rows, collapsing a read miss to
// an empty slice so the join below renders "no worker" rather than 500ing.
func workerRowsOrEmpty(ctx context.Context, ds DataSource) []WorkerStatusRow {
	rows, _ := ds.ListWorkerRows(ctx)
	return rows
}

// workerStatusByID indexes worker rows by id for the provider join.
// Bounded by len(rows).
func workerStatusByID(rows []WorkerStatusRow) map[string]string {
	const rowsMax = 10000
	out := make(map[string]string, len(rows))
	for i := 0; i < len(rows) && i < rowsMax; i++ {
		out[rows[i].WorkerID] = rows[i].Status
	}
	return out
}

// functionProvidersFrom joins owner worker ids to their live status.
// An owner absent from the status map falls back to "no worker" — the
// function is registered but unserved. Pure so tests drive it directly.
// Bounded by len(owners).
func functionProvidersFrom(
	owners []string, statusByID map[string]string,
) []FunctionProviderRow {
	const ownersMax = 10000
	out := make([]FunctionProviderRow, 0, len(owners))
	for i := 0; i < len(owners) && i < ownersMax; i++ {
		status, ok := statusByID[owners[i]]
		if !ok || status == "" {
			status = "no worker"
		}
		out = append(out, FunctionProviderRow{
			WorkerID: owners[i],
			Status:   status,
		})
	}
	return out
}

// functionHealth returns "healthy" when at least one provider carries a
// live worker status, "no worker" otherwise. Bounded by len(providers).
func functionHealth(providers []FunctionProviderRow) string {
	const providersMax = 10000
	for i := 0; i < len(providers) && i < providersMax; i++ {
		if providers[i].Status != "no worker" {
			return "healthy"
		}
	}
	return "no worker"
}

// functionInvocations reads the run list and projects the runs that
// exercised this function into render rows, newest first, capped. A read
// miss collapses to an empty slice. Memoizes GetWorkflow within the call
// so each distinct workflow is fetched once.
func functionInvocations(
	ctx context.Context, ds DataSource, name string,
) []RunRow {
	runs, _ := ds.ListRuns(ctx, "")
	defs := workflowDefsForRuns(ctx, ds, runs)
	matched := invocationsForFunction(runs, defs, name)
	sort.SliceStable(matched, func(i, j int) bool {
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})
	out := make([]RunRow, 0, invocationsMax)
	for i := 0; i < len(matched) && i < invocationsMax; i++ {
		out = append(out, runRowFromRun(matched[i]))
	}
	return out
}

// workflowDefsForRuns fetches each distinct workflow a run references,
// once. A lookup miss is skipped — the run simply won't match any
// function. Bounded by len(runs).
func workflowDefsForRuns(
	ctx context.Context, ds DataSource, runs []dag.WorkflowRun,
) map[string]dag.WorkflowDef {
	const runsMax = 100000
	defs := make(map[string]dag.WorkflowDef, len(runs))
	for i := 0; i < len(runs) && i < runsMax; i++ {
		id := runs[i].WorkflowID
		if id == "" {
			continue
		}
		if _, seen := defs[id]; seen {
			continue
		}
		def, err := ds.GetWorkflow(id)
		if err != nil {
			continue
		}
		defs[id] = def
	}
	return defs
}

// invocationsForFunction keeps the runs whose workflow def has a step
// referencing name. Pure so tests drive it with fixed inputs. Bounded by
// len(runs). A run whose workflow is absent from defs is dropped (lookup
// miss degrades to omission, never fabrication).
func invocationsForFunction(
	runs []dag.WorkflowRun,
	defs map[string]dag.WorkflowDef,
	name string,
) []dag.WorkflowRun {
	const runsMax = 100000
	out := make([]dag.WorkflowRun, 0, len(runs))
	for i := 0; i < len(runs) && i < runsMax; i++ {
		def, ok := defs[runs[i].WorkflowID]
		if !ok {
			continue
		}
		if workflowReferences(def, name) {
			out = append(out, runs[i])
		}
	}
	return out
}

// workflowReferences reports whether any step in def calls the named
// task type. Bounded by len(def.Steps).
func workflowReferences(def dag.WorkflowDef, name string) bool {
	const stepsMax = 100000
	for i := 0; i < len(def.Steps) && i < stepsMax; i++ {
		if def.Steps[i].Task == name {
			return true
		}
	}
	return false
}

// referencingWorkflows lists the workflow defs whose steps call name.
// Backs the schema-attribution gate. A read miss collapses to nil.
// Bounded by len(all).
func referencingWorkflows(
	ctx context.Context, ds DataSource, name string,
) []dag.WorkflowDef {
	all, _ := ds.ListWorkflows(ctx)
	const defsMax = 100000
	out := make([]dag.WorkflowDef, 0, len(all))
	for i := 0; i < len(all) && i < defsMax; i++ {
		if workflowReferences(all[i], name) {
			out = append(out, all[i])
		}
	}
	return out
}

// functionSchemaFor returns the input/output schema for the function and
// the workflow it came from, but ONLY when exactly one referencing
// workflow exposes a non-empty schema. Multiple referencing workflows is
// ambiguous attribution — return empty so the page shows "No schema
// registered" rather than picking a workflow's schema arbitrarily. Pure
// so tests drive it. Bounded by len(defs).
func functionSchemaFor(
	defs []dag.WorkflowDef, name string,
) (inputSchema, outputSchema, sourceWorkflow string) {
	const defsMax = 100000
	bearing := make([]dag.WorkflowDef, 0, len(defs))
	for i := 0; i < len(defs) && i < defsMax; i++ {
		if !workflowReferences(defs[i], name) {
			continue
		}
		if len(defs[i].InputSchema) == 0 && len(defs[i].OutputSchema) == 0 {
			continue
		}
		bearing = append(bearing, defs[i])
	}
	if len(bearing) != 1 {
		return "", "", ""
	}
	def := bearing[0]
	return string(def.InputSchema), string(def.OutputSchema), def.Name
}

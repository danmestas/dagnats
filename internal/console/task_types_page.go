// task_types_page.go owns /console/task-types — the Tier-3 R11
// task-type registry view (#328, parent #274 R11). The page answers:
// what task types is this deployment ready to run, who owns each one,
// and how have they performed recently.
//
// Aggregation is intentionally narrow. ConfigSnapshot is deployment-
// shape (workers, streams, KV buckets, endpoints) and only refreshes
// when the operator hits /console/config; task-type liveness needs to
// fan out across every worker on every page load. So a separate
// DataSource.AggregateTaskTypes method, separate lifecycle, separate
// concerns — per ADR-015 R11 spec audit (Q5 audit-locked).
//
// Service prefix grouping uses the `service::task` convention from
// ADR-017 / #322. Ungrouped task types fall under the synthetic
// "(default)" group so every row finds a home without a sentinel.
//
// Recent invocations / avg duration / failure rate are placeholders
// today. The metrics aggregator does not carry per-task-type
// histograms yet — the page renders an em-dash for those columns so
// it does not lie about zero. A future PR can fold in the histogram
// once the aggregator surface widens.
package console

import (
	"context"
	"net/http"
	"sort"
	"strings"
)

// defaultServiceGroup is the synthetic group name for task types
// that carry no `service::` prefix. Bare task types like "email"
// land here so the page renders one row per task type without a
// magic-string sentinel scattered through the template.
const defaultServiceGroup = "(default)"

// TaskTypesPageView is the binding the task_types_list.html template
// renders. Header carries the page-header strip (tile counts);
// Groups is one entry per service prefix (sorted, default last);
// EmptyState renders when no workers reported.
type TaskTypesPageView struct {
	Header     PageHeader
	Groups     []TaskTypeGroup
	EmptyState *EmptyState
}

// TaskTypeGroup is one service-prefix bucket. Name is the service
// prefix ("billing", "email", ...) or "(default)" for ungrouped.
// Rows are the task types in that group, sorted by name.
//
// ServiceDescription is the Description field from the matching
// `services` KV entry (#322 / #335). Empty when no service is
// registered under Name — the row group still renders, just without
// the operator-facing tooltip. Lifted from the first row in Rows
// during groupTaskTypeRows; every row in a group carries the same
// description, so picking any of them is fine.
type TaskTypeGroup struct {
	Name               string
	ServiceDescription string
	Rows               []TaskTypeRow
}

// TaskTypeRow is one row in the registry table. TaskType is the full
// task-type string as advertised by the worker (e.g. "billing::charge"
// or "email"). OwnerWorkerIDs lists every worker registration that
// claims to handle this task type — multiple entries means redundancy.
//
// RecentInvocations / AvgDurationMS / FailureRate are -1 today (the
// "unmeasured" sentinel). The renderer maps -1 to an em-dash so a
// future histogram wiring drops in without a schema break.
type TaskTypeRow struct {
	TaskType          string
	Service           string
	OwnerWorkerIDs    []string
	RecentInvocations int
	AvgDurationMS     int
	FailureRate       float64
	// RunHref is the click-through target for the row chevron — the
	// per-function read-only detail page at /console/functions/<name>.
	RunHref string
	// ServiceDescription mirrors the Description field on the
	// matching `services` KV entry (#322 / #335). Empty when no
	// service is registered under Service. Replicated on every
	// row in a service group so groupTaskTypeRows can lift it
	// onto the TaskTypeGroup header without a second lookup.
	ServiceDescription string
}

// servePageTaskTypes renders /console/task-types.
func servePageTaskTypes(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageTaskTypes: w is nil")
	}
	if r == nil {
		panic("servePageTaskTypes: r is nil")
	}
	ds, ok := requireData(w, cfg, "task-types")
	if !ok {
		return
	}
	view := buildTaskTypesView(r.Context(), ds)
	if view.EmptyState != nil {
		view.EmptyState.ReadOnly = cfg.ReadOnly
	}
	renderPage(w, r, ts, cfg, "task-types-list", pageData{
		Title:   "Functions",
		Section: "functions",
		Page:    view,
	})
}

// buildTaskTypesView assembles the page view from the DataSource.
// Errors collapse to the empty state — the page is observational, so
// a transient failure paints empty rather than 500ing.
func buildTaskTypesView(
	ctx context.Context, ds DataSource,
) TaskTypesPageView {
	rows, err := ds.AggregateTaskTypes(ctx)
	if err != nil || len(rows) == 0 {
		return TaskTypesPageView{
			Header:     taskTypesHeader(0, 0, 0),
			EmptyState: taskTypesEmptyState(),
		}
	}
	groups := groupTaskTypeRows(rows)
	return TaskTypesPageView{
		Header: taskTypesHeader(len(rows), len(groups), distinctWorkerCount(rows)),
		Groups: groups,
	}
}

// distinctWorkerCount counts the unique worker IDs across every
// function row. OwnerWorkerIDs is already on each row (the aggregation
// derives it from the live worker registrations), so this is an honest
// count over data the page fetched — not a synthetic placeholder. A
// worker that handles two functions is counted once.
func distinctWorkerCount(rows []TaskTypeRow) int {
	const rowsMax = 10000
	seen := make(map[string]struct{})
	for i := 0; i < len(rows) && i < rowsMax; i++ {
		for _, id := range rows[i].OwnerWorkerIDs {
			if id == "" {
				continue
			}
			seen[id] = struct{}{}
		}
	}
	return len(seen)
}

// taskTypesHeader builds the page header strip. Three tiles: total
// task-type count, service-group count, and distinct worker count.
// Operator can see at a glance "how many handlers", "how many
// namespaces", and "how many workers back them" without scanning the
// table. FAIL RATE 24h is intentionally absent — the page does not
// fetch a per-function failure histogram, so a tile would lie.
func taskTypesHeader(taskTypes, groups, workers int) PageHeader {
	header, err := NewPageHeader(PageHeader{
		Title:    "Functions",
		Subtitle: "Every task type any live worker handles.",
		Tiles: []Tile{
			{Label: "FUNCTIONS", Count: taskTypes, Tone: ToneDefault},
			{Label: "WORKERS", Count: workers, Tone: ToneSuccess,
				Tooltip: "Distinct workers registered to handle these functions"},
			{Label: "SERVICES", Count: groups, Tone: ToneInfo},
		},
	})
	if err != nil {
		return PageHeader{Title: "Functions"}
	}
	return header
}

// taskTypesEmptyState returns the no-workers empty state. The CLI
// hint points the operator at the worker SDK; this matches the
// pattern from #310 / #316 (every empty state carries an
// operator-actionable primary action).
func taskTypesEmptyState() *EmptyState {
	es, err := NewEmptyState(EmptyState{
		Icon:        "task",
		Title:       "No task types registered",
		Description: "Start a worker that calls RegisterTask to populate this list.",
		PrimaryAction: &EmptyStateAction{
			Label: "Read the worker docs",
			Href:  "/docs",
		},
	})
	if err != nil {
		return nil
	}
	return &es
}

// groupTaskTypeRows folds a flat row set into service-prefix groups.
// Default group always renders last so the namespaced groups read
// top-down alphabetically and the bare task types form a clear
// trailing block.
func groupTaskTypeRows(rows []TaskTypeRow) []TaskTypeGroup {
	if len(rows) == 0 {
		return nil
	}
	const maxRows = 10000
	if len(rows) > maxRows {
		panic("groupTaskTypeRows: rows exceeds max bound")
	}
	buckets := make(map[string][]TaskTypeRow, len(rows))
	for _, row := range rows {
		key := row.Service
		if key == "" {
			key = defaultServiceGroup
		}
		buckets[key] = append(buckets[key], row)
	}
	for name := range buckets {
		sort.Slice(buckets[name], func(i, j int) bool {
			return buckets[name][i].TaskType < buckets[name][j].TaskType
		})
	}
	out := make([]TaskTypeGroup, 0, len(buckets))
	for name, rs := range buckets {
		// Description is replicated on every row in a group (set by
		// attachServiceDescriptions in the adapter). Lifting from rs[0]
		// is safe — buckets[name] is guaranteed non-empty because we
		// only insert after walking a row through.
		desc := ""
		if len(rs) > 0 {
			desc = rs[0].ServiceDescription
		}
		out = append(out, TaskTypeGroup{
			Name:               name,
			ServiceDescription: desc,
			Rows:               rs,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return taskTypeGroupLess(out[i].Name, out[j].Name)
	})
	return out
}

// taskTypeGroupLess sorts namespaced groups alphabetically and pins
// the (default) bucket to the bottom of the list. Lifted out of the
// sort closure so the rule is testable in isolation if a future PR
// reshuffles the ordering.
func taskTypeGroupLess(a, b string) bool {
	if a == defaultServiceGroup {
		return false
	}
	if b == defaultServiceGroup {
		return true
	}
	return a < b
}

// splitServicePrefix returns (service, taskType-as-given). When the
// task type carries the `service::name` convention from ADR-017 the
// first half becomes the service group; otherwise service is empty
// and the row lands in (default). Trailing `::` is treated as no
// prefix — operators sometimes leave dangling separators in dev.
func splitServicePrefix(taskType string) string {
	idx := strings.Index(taskType, "::")
	if idx <= 0 || idx >= len(taskType)-2 {
		return ""
	}
	return taskType[:idx]
}

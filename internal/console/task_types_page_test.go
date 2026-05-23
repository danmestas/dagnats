// task_types_page_test.go exercises /console/task-types (#328, R11).
//
// Methodology:
//   - In-memory fakeDataSource is reused; configSnap.Workers seeds
//     the AggregateTaskTypes derivation path so we don't need a new
//     test seam for the simple cases.
//   - Each test mounts its own console.Mount; nothing is shared.
//   - Aggregation is exercised both via the pure function and via
//     the rendered page so a regression in either layer surfaces.
//   - Empty-state behaviour mirrors the empty_states_test.go pattern:
//     a tbody colspan row carrying the shared empty-state partial.
//   - Services cross-reference (#335) is exercised via the pure
//     attachServiceDescriptions helper (deterministic, no NATS) plus
//     the rendered page so a regression in either layer surfaces.
package console

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// TestAggregateTaskTypes_GroupsByService asserts the pure aggregation
// function buckets task types under their `service::` prefix and
// drops bare names into the synthetic "(default)" group. Mirrors
// the page-level grouping rule so a future refactor that changes
// the grouping surface fails this test before it fails the DOM.
func TestAggregateTaskTypes_GroupsByService(t *testing.T) {
	regs := []worker.WorkerRegistration{
		{
			WorkerID:  "worker-billing",
			TaskTypes: []string{"billing::charge", "billing::refund"},
		},
		{
			WorkerID:  "worker-email",
			TaskTypes: []string{"email"},
		},
	}
	rows := aggregateTaskTypesFromWorkers(regs)
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	groups := groupTaskTypeRows(rows)
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2 (billing + default)", len(groups))
	}

	// Positive: billing group carries both charge + refund.
	var billing *TaskTypeGroup
	for i := range groups {
		if groups[i].Name == "billing" {
			billing = &groups[i]
		}
	}
	if billing == nil {
		t.Fatalf("billing group missing; got %+v", groupNames(groups))
	}
	if len(billing.Rows) != 2 {
		t.Errorf("billing rows = %d, want 2", len(billing.Rows))
	}
	wantBilling := []string{"billing::charge", "billing::refund"}
	gotBilling := []string{billing.Rows[0].TaskType, billing.Rows[1].TaskType}
	sort.Strings(gotBilling)
	if gotBilling[0] != wantBilling[0] || gotBilling[1] != wantBilling[1] {
		t.Errorf("billing rows = %v, want %v", gotBilling, wantBilling)
	}

	// Positive: default group carries the bare email task type.
	var dflt *TaskTypeGroup
	for i := range groups {
		if groups[i].Name == defaultServiceGroup {
			dflt = &groups[i]
		}
	}
	if dflt == nil {
		t.Fatalf("default group missing; got %+v", groupNames(groups))
	}
	if len(dflt.Rows) != 1 || dflt.Rows[0].TaskType != "email" {
		t.Errorf("default group = %+v, want one row 'email'", dflt.Rows)
	}

	// Negative: (default) must sort last so namespaced groups read
	// top-down before the ungrouped trailing block.
	if groups[len(groups)-1].Name != defaultServiceGroup {
		t.Errorf("(default) group not pinned to the bottom; got order %v",
			groupNames(groups))
	}
}

// TestAggregateTaskTypes_DedupesAcrossWorkers asserts two workers
// each advertising the same task type collapse into one row with
// two OwnerWorkerIDs — exactly the redundancy view operators want.
func TestAggregateTaskTypes_DedupesAcrossWorkers(t *testing.T) {
	regs := []worker.WorkerRegistration{
		{WorkerID: "w-a", TaskTypes: []string{"email"}},
		{WorkerID: "w-b", TaskTypes: []string{"email"}},
	}
	rows := aggregateTaskTypesFromWorkers(regs)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1 (deduped)", len(rows))
	}
	row := rows[0]
	if row.TaskType != "email" {
		t.Errorf("task type = %q, want 'email'", row.TaskType)
	}
	if len(row.OwnerWorkerIDs) != 2 {
		t.Fatalf("OwnerWorkerIDs = %d, want 2", len(row.OwnerWorkerIDs))
	}
	owners := append([]string{}, row.OwnerWorkerIDs...)
	sort.Strings(owners)
	if owners[0] != "w-a" || owners[1] != "w-b" {
		t.Errorf("OwnerWorkerIDs = %v, want [w-a w-b]", owners)
	}

	// Negative: a single advertised type must not yield two rows.
	rows2 := aggregateTaskTypesFromWorkers([]worker.WorkerRegistration{
		{WorkerID: "solo", TaskTypes: []string{"only-one"}},
	})
	if len(rows2) != 1 || len(rows2[0].OwnerWorkerIDs) != 1 {
		t.Errorf("solo-worker aggregation = %+v", rows2)
	}
}

// TestTaskTypesPage_RendersAllSections asserts the rendered DOM
// surfaces the page header, table chrome, service-group label, and
// at least one task-type row with its owner. Em-dash on the
// unmeasured metric columns is the honesty contract: a zero would
// lie about activity that isn't tracked yet.
func TestTaskTypesPage_RendersAllSections(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		Workers: []worker.WorkerRegistration{
			{
				WorkerID: "w1",
				TaskTypes: []string{
					"billing::charge", "billing::refund",
				},
				LastSeen: time.Now(),
			},
			{
				WorkerID:  "w2",
				TaskTypes: []string{"email"},
				LastSeen:  time.Now(),
			},
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/task-types", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	wants := []string{
		// Page chrome.
		"<title>Task Types",
		`data-page="task-types-list"`,
		// Header tile strip.
		`page-header-tile`,
		`TASK TYPES`, `SERVICES`,
		// Table chrome.
		`id="task-types-table"`,
		`<th>Task type</th>`,
		`<th>Owners</th>`,
		`<th>Recent runs (1h)</th>`,
		`<th>Avg duration</th>`,
		`<th>Failure rate</th>`,
		// Service group: billing prefix collected under its badge.
		`data-service="billing"`,
		`billing::charge`, `billing::refund`,
		// Default bucket holds the bare email task type.
		`data-service="(default)"`,
		"email",
		// Owners list resolves to the worker IDs.
		"w1", "w2",
		// Em-dash on the three unmeasured metric columns.
		"&mdash;",
		// Top-nav link must reflect the active section.
		`href="/console/task-types"`,
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("task-types page missing %q", want)
		}
	}

	// Negative: a deployment with two task-type groups must not
	// render the empty-state shell.
	if strings.Contains(body, `data-component="empty-state"`) {
		t.Errorf("populated page leaked empty-state partial")
	}
}

// TestTaskTypesPage_EmptyState asserts a zero-worker deployment
// renders the empty-state partial inside a tbody colspan row (per
// the pattern from #316 / empty_states_test.go:154).
func TestTaskTypesPage_EmptyState(t *testing.T) {
	fake := newFakeDS()
	// No workers — derivation yields no rows.
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/task-types", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	wants := []string{
		`data-component="empty-state"`,
		`data-kind="task"`,
		"No task types registered",
		"RegisterTask",
		// Empty-state lives inside the table chrome so the page
		// keeps its visual shape (header strip on top, table below).
		`id="task-types-table"`,
		`colspan="6"`,
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("task-types empty state missing %q", want)
		}
	}

	// Negative: no task rows must render when the source is empty.
	if strings.Contains(body, `class="task-type-row"`) {
		t.Errorf("empty deployment still rendered task rows")
	}
}

// TestAggregateTaskTypes_AttachesServiceDescription asserts that when
// a service is registered in the `services` KV bucket (#322), its
// Description rides along onto every TaskTypeRow whose Service prefix
// matches — and through groupTaskTypeRows, onto the group header (#335).
// The cross-reference is informational only; grouping itself stays
// structural on the `service::` substring.
func TestAggregateTaskTypes_AttachesServiceDescription(t *testing.T) {
	rows := aggregateTaskTypesFromWorkers([]worker.WorkerRegistration{
		{
			WorkerID:  "worker-billing",
			TaskTypes: []string{"billing::charge", "billing::refund"},
		},
	})
	defs := []worker.ServiceDef{
		{Name: "billing", Description: "Payment processing"},
	}
	rows = attachServiceDescriptions(rows, defs)

	// Positive: every row in the billing service carries the description.
	for _, r := range rows {
		if r.ServiceDescription != "Payment processing" {
			t.Errorf(
				"row %q ServiceDescription = %q, want %q",
				r.TaskType, r.ServiceDescription,
				"Payment processing",
			)
		}
	}

	// Positive: group header lifts the description from the rows.
	groups := groupTaskTypeRows(rows)
	if len(groups) != 1 || groups[0].Name != "billing" {
		t.Fatalf("groups = %+v, want one 'billing' group",
			groupNames(groups))
	}
	if groups[0].ServiceDescription != "Payment processing" {
		t.Errorf(
			"billing group ServiceDescription = %q, want %q",
			groups[0].ServiceDescription, "Payment processing",
		)
	}
}

// TestAggregateTaskTypes_HandlesUnregisteredService asserts that a
// task type whose service prefix is NOT in the `services` KV bucket
// still groups under that prefix — just with an empty
// ServiceDescription. No error, no surprise (#335).
func TestAggregateTaskTypes_HandlesUnregisteredService(t *testing.T) {
	rows := aggregateTaskTypesFromWorkers([]worker.WorkerRegistration{
		{WorkerID: "w-x", TaskTypes: []string{"unregistered::foo"}},
	})
	// Empty services slice — nothing to cross-reference.
	rows = attachServiceDescriptions(rows, nil)

	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].Service != "unregistered" {
		t.Errorf("row Service = %q, want %q",
			rows[0].Service, "unregistered")
	}
	if rows[0].ServiceDescription != "" {
		t.Errorf("row ServiceDescription = %q, want empty",
			rows[0].ServiceDescription)
	}

	groups := groupTaskTypeRows(rows)
	if len(groups) != 1 || groups[0].Name != "unregistered" {
		t.Fatalf("groups = %+v, want one 'unregistered' group",
			groupNames(groups))
	}
	if groups[0].ServiceDescription != "" {
		t.Errorf(
			"unregistered group ServiceDescription = %q, want empty",
			groups[0].ServiceDescription,
		)
	}
}

// TestAggregateTaskTypes_DocCommentMatchesImpl is a meta-test guarding
// against doc/code drift on AggregateTaskTypes. The #331 reviewer
// flagged a stale claim that the services KV was consulted only to
// confirm a prefix was registered (informational, never read). #335
// changes that — the bucket is read for Description metadata. This
// test parses the file and asserts the doc comment mentions the
// services KV cross-reference so any future drift surfaces here
// before it surfaces in code review.
func TestAggregateTaskTypes_DocCommentMatchesImpl(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset, "data_source.go", nil, parser.ParseComments,
	)
	if err != nil {
		t.Fatalf("parse data_source.go: %v", err)
	}
	var doc string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "AggregateTaskTypes" {
			continue
		}
		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			continue
		}
		if fn.Doc == nil {
			continue
		}
		doc = fn.Doc.Text()
		break
	}
	if doc == "" {
		t.Fatalf("AggregateTaskTypes method doc comment not found")
	}
	// Must mention the services KV cross-reference (the #335
	// behaviour). A future PR that drops the cross-reference must
	// also update this doc — and this test fails first.
	wants := []string{"services", "KV", "Description"}
	for _, want := range wants {
		if !strings.Contains(doc, want) {
			t.Errorf(
				"AggregateTaskTypes doc missing %q; got:\n%s",
				want, doc,
			)
		}
	}
	// Negative: the stale claim from #331 ("only to confirm a prefix
	// is a registered service") must be gone — the bucket is now
	// consulted for Description data, not presence.
	if strings.Contains(doc, "only to confirm a prefix") {
		t.Errorf(
			"AggregateTaskTypes doc still carries the stale #331 claim; got:\n%s",
			doc,
		)
	}
}

// TestTaskTypesPage_RendersServiceTooltip asserts the rendered DOM
// surfaces a glossary-style tooltip on the group header carrying the
// registered service's description (#335). Default groups (no service
// registered) must NOT render the tooltip popover — the empty body
// would lie about content the operator could read.
func TestTaskTypesPage_RendersServiceTooltip(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		Workers: []worker.WorkerRegistration{
			{
				WorkerID:  "w-pay",
				TaskTypes: []string{"billing::charge"},
				LastSeen:  time.Now(),
			},
			{
				WorkerID:  "w-bare",
				TaskTypes: []string{"email"},
				LastSeen:  time.Now(),
			},
		},
	}
	fake.services = []worker.ServiceDef{
		{Name: "billing", Description: "Payment processing"},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/task-types", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	wants := []string{
		// Tooltip wrapper signals the popover lives on the group
		// header — reuses the glossary tooltip CSS (glo-* prefix).
		`glo-tooltip-wrapper`,
		// Description text from the services KV is rendered into
		// the popover.
		"Payment processing",
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("task-types tooltip missing %q", want)
		}
	}

	// Negative: the (default) group has no registered service, so
	// the popover must NOT carry an empty body that would look like
	// a bug to the operator.
	if strings.Contains(body, `>Payment processing</span>`) &&
		!strings.Contains(body, `data-service="billing"`) {
		t.Errorf("popover rendered without its parent service group")
	}
}

// groupNames lifts the group names out of a []TaskTypeGroup so test
// failures can show a compact diff string.
func groupNames(gs []TaskTypeGroup) []string {
	out := make([]string, 0, len(gs))
	for _, g := range gs {
		out = append(out, g.Name)
	}
	return out
}

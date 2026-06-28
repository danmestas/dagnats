// row_click_href_test.go pins the delegated row-click drill-in contract
// (m4). The decorative row-chevron used to look clickable but carried no
// href/handler. The fix adds a single data-href to each <tr> on the seven
// drillable list contexts; a shared delegated handler in console.js reads
// that attribute and navigates, bailing on interactive descendants.
//
// The JS click itself is not unit-testable in Go, so these tests assert
// the STRUCTURE the handler depends on: every wired row carries a
// data-href pointing at the correct detail URL (full run id, not the
// short display id; trigger/stream/workflow/worker/function ids verbatim).
// A separate test asserts the rebuilt console.js bundle embeds the
// delegated handler so wiring without behavior cannot ship green.
//
// Methodology:
//   - Render fragments directly via loadTemplates()/renderFragment for the
//     four fragment contexts (workflows, runs, trigger-row, triggers).
//   - Render full pages via console.Mount + httptest for streams, workers,
//     and task-types (functions), which have no standalone fragment.
//   - Two assertions per test minimum (positive data-href + negative space).
//   - The bundle test gunzips the embedded console.js.gz and asserts the
//     delegated handler signature survives the esbuild/minify pass.
package console

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// TestRowClick_workflowsTbody_dataHref pins the workflows row data-href at
// the workflow detail page keyed on Name.
func TestRowClick_workflowsTbody_dataHref(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := WorkflowsListView{Rows: []WorkflowRow{{Name: "alpha", Version: "v1"}}}
	html, err := renderFragment(set.base, "workflows-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment workflows-tbody: %v", err)
	}
	if !strings.Contains(html, `data-href="/console/workflows/alpha"`) {
		t.Errorf("workflows row must carry data-href to its detail page; got\n%s", html)
	}
	// Negative space: the chevron stays a pure visual partial — the only
	// refs to the detail URL are the first-column name link and the row
	// data-href (exactly two), never a third handler on the chevron cell.
	if strings.Count(html, "/console/workflows/alpha") != 2 {
		t.Errorf("workflows row should reference detail URL exactly twice "+
			"(name link + row data-href); got\n%s", html)
	}
}

// TestRowClick_runsTbody_dataHrefUsesFullID is the load-bearing run test:
// the row data-href must use the FULL run id, not the short display id.
func TestRowClick_runsTbody_dataHrefUsesFullID(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := RunsListView{Rows: []RunRow{{
		RunID:       "run-aaaaaaaaaaaa-1",
		RunIDShort:  "run-aaaa",
		WorkflowID:  "alpha",
		Status:      "running",
		TriggerKind: "manual",
		StartedAt:   "2026-05-22T00:00:00Z",
		Duration:    "1s",
	}}}
	html, err := renderFragment(set.base, "runs-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment runs-tbody: %v", err)
	}
	// Positive: the row data-href uses the full run id.
	if !strings.Contains(html, `data-href="/console/runs/run-aaaaaaaaaaaa-1"`) {
		t.Errorf("runs row data-href must use the FULL run id; got\n%s", html)
	}
	// Negative space: the short id must never appear inside a data-href.
	if strings.Contains(html, `data-href="/console/runs/run-aaaa"`) {
		t.Errorf("runs row data-href must NOT use the short display id; got\n%s", html)
	}
}

// TestRowClick_triggerRow_dataHref pins the single trigger-row fragment
// (used for live-appended rows) at the trigger detail page.
func TestRowClick_triggerRow_dataHref(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	patch := triggerRowPatch{Row: TriggerRow{
		ID:          "trg-1",
		Kind:        "cron",
		Target:      "*/5 * * * *",
		Workflow:    "alpha",
		Enabled:     true,
		StatusLabel: "active",
	}}
	html, err := renderFragment(set.base, "trigger-row", patch)
	if err != nil {
		t.Fatalf("renderFragment trigger-row: %v", err)
	}
	if !strings.Contains(html, `data-href="/console/triggers/trg-1"`) {
		t.Errorf("trigger-row must carry data-href to its detail page; got\n%s", html)
	}
	// Negative space: the toggle switch stays an interactive descendant the
	// handler must skip — it must still render role="switch".
	if !strings.Contains(html, `role="switch"`) {
		t.Errorf("trigger-row must keep its role=switch toggle; got\n%s", html)
	}
}

// TestRowClick_triggersTbody_dataHref pins the bulk triggers fragment.
func TestRowClick_triggersTbody_dataHref(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := TriggersListView{Rows: []TriggerRow{{
		ID:          "trg-2",
		Kind:        "cron",
		Target:      "*/5 * * * *",
		Workflow:    "alpha",
		Enabled:     true,
		StatusLabel: "active",
	}}}
	html, err := renderFragment(set.base, "triggers-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment triggers-tbody: %v", err)
	}
	if !strings.Contains(html, `data-href="/console/triggers/trg-2"`) {
		t.Errorf("triggers row must carry data-href to its detail page; got\n%s", html)
	}
	if !strings.Contains(html, `role="switch"`) {
		t.Errorf("triggers row must keep its role=switch toggle; got\n%s", html)
	}
}

// TestRowClick_streamsList_dataHref renders the full streams page and
// asserts the row data-href keyed on stream Name.
func TestRowClick_streamsList_dataHref(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap.Streams = []StreamSnapshot{
		{Name: "TASK_QUEUES", Subjects: []string{"tasks.>"}, Provisioned: true},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/streams", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `data-href="/console/streams/TASK_QUEUES"`) {
		t.Errorf("streams row must carry data-href to its detail page; got page body")
	}
	// Negative space: the name-cell anchor still points at the same detail.
	if !strings.Contains(body, `<a href="/console/streams/TASK_QUEUES">`) {
		t.Errorf("streams row must keep its focusable name link; got page body")
	}
}

// TestRowClick_workersList_dataHref renders the full workers page and
// asserts the row data-href keyed on WorkerID. Workers HAS a detail route
// (/console/workers/{id}, dispatchWorkers), so the row is wired, not
// de-chevroned.
func TestRowClick_workersList_dataHref(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap.Workers = []worker.WorkerRegistration{{
		WorkerID:  "wk-alpha",
		TaskTypes: []string{"email"},
		Hostname:  "host-1",
		LastSeen:  time.Now(),
	}}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/workers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `data-href="/console/workers/wk-alpha"`) {
		t.Errorf("workers row must carry data-href to its detail page; got page body")
	}
	if !strings.Contains(body, `<a href="/console/workers/wk-alpha">`) {
		t.Errorf("workers row must keep its focusable name link; got page body")
	}
}

// TestRowClick_taskTypesList_dataHref renders the functions page and
// asserts the row data-href matches the per-function detail RunHref. Uses
// the same seedFunctionFake() the existing function-list test relies on.
func TestRowClick_taskTypesList_dataHref(t *testing.T) {
	fake := seedFunctionFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/functions", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `data-href="/console/functions/billing::charge"`) {
		t.Errorf("function row must carry data-href to its detail page; got page body")
	}
	if !strings.Contains(body, `<a href="/console/functions/billing::charge">`) {
		t.Errorf("function row must keep its focusable name link; got page body")
	}
}

// TestRowClick_bundleEmbedsDelegatedHandler gunzips the embedded
// console.js.gz and asserts the delegated row-click handler survives the
// esbuild/minify pass. Wiring data-href without a handler would ship a
// dead affordance; this test fails the build if the bundle was not rebuilt.
func TestRowClick_bundleEmbedsDelegatedHandler(t *testing.T) {
	gz, err := assetsFS.ReadFile("assets/console.js.gz")
	if err != nil {
		t.Fatalf("read console.js.gz: %v", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip console.js: %v", err)
	}
	js := string(raw)
	// The handler keys off tr[data-href]; the literal must survive minify.
	if !strings.Contains(js, "data-href") {
		t.Errorf("rebuilt console.js bundle must reference data-href (delegated handler missing)")
	}
	// The interactive-descendant guard is the load-bearing branch; assert
	// one of its sentinel selectors survives so a stub cannot pass.
	if !strings.Contains(js, `role="switch"`) {
		t.Errorf("rebuilt console.js bundle must include the interactive-descendant guard")
	}
}

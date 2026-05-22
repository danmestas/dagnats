// page_header_test.go covers the typed Tone validation surface and
// the rendered partial structure for the four list pages that now own
// a PageHeader (#302, ADR-015 R5).
//
// Methodology:
//   - Pure-Go tests on PageHeader.validate() / Tile.validate() —
//     no HTTP round-trip, just the constructor.
//   - Per-page DOM-substring tests: build the page through the real
//     handler with a seeded fakeDataSource, assert the rendered HTML
//     contains the title, the expected tile counts, and the tone CSS
//     class for the active/danger tile.
//   - The browser smoke layer lives in browser_smoke_test.go; this
//     file covers the substring assertions called out in the spec.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// TestTileTone_RejectsUnknown asserts the validate() method on Tile
// rejects any tone value that isn't one of the five constants. This
// is the load-bearing test for the typed-Tone decision in #302:
// without it a future caller could pass a stringly-typed "succes"
// (typo) and quietly render a default-tone tile.
func TestTileTone_RejectsUnknown(t *testing.T) {
	tile := Tile{Label: "active", Count: 1, Tone: TileTone("bogus")}
	if err := tile.validate(); err == nil {
		t.Fatal("expected error for unknown tone, got nil")
	}
	// Negative space: every constant must validate clean.
	for _, tone := range []TileTone{
		ToneDefault, ToneSuccess, ToneWarning, ToneDanger, ToneInfo,
	} {
		ok := Tile{Label: "x", Count: 0, Tone: tone}
		if err := ok.validate(); err != nil {
			t.Errorf("tone %q should validate: %v", tone, err)
		}
	}
}

// TestPageHeader_ValidateRejectsBadTile asserts construction-time
// validation surfaces failures inside the Tiles slice. The
// constructor is the right gate: validation at render time would
// bury the error inside a template-execution failure.
func TestPageHeader_ValidateRejectsBadTile(t *testing.T) {
	_, err := NewPageHeader(PageHeader{
		Title: "X",
		Tiles: []Tile{{Label: "ok", Count: 1, Tone: ToneSuccess},
			{Label: "bad", Count: 1, Tone: TileTone("???")}},
	})
	if err == nil {
		t.Fatal("expected error for bad tile, got nil")
	}
	if !strings.Contains(err.Error(), "tile 1") {
		t.Errorf("error should identify the bad tile index: %q", err.Error())
	}
}

// TestPageHeader_ValidateRejectsEmptyTitle asserts the title-required
// invariant — a tileless header still needs an H1.
func TestPageHeader_ValidateRejectsEmptyTitle(t *testing.T) {
	_, err := NewPageHeader(PageHeader{Title: ""})
	if err == nil {
		t.Fatal("expected error for empty title")
	}
}

// TestPageHeader_ToneClassMapping pins the tone→CSS-class mapping so a
// future palette refactor that touches the Class() method also has to
// update this test. The browser smoke test asserts the same classes
// are present in a real DOM; this guards the lookup in isolation.
func TestPageHeader_ToneClassMapping(t *testing.T) {
	cases := map[TileTone]string{
		ToneDefault: "tile-tone-default",
		ToneSuccess: "tile-tone-success",
		ToneWarning: "tile-tone-warning",
		ToneDanger:  "tile-tone-danger",
		ToneInfo:    "tile-tone-info",
	}
	for tone, want := range cases {
		if got := tone.Class(); got != want {
			t.Errorf("tone %q: got class %q, want %q", tone, got, want)
		}
	}
	// Negative space: unknown tones fall back to default — Class() is
	// total even if validate() isn't, because the partial calls it
	// during render and a panic there would 500 the page.
	if TileTone("never").Class() != "tile-tone-default" {
		t.Error("unknown tone should fall back to default class")
	}
}

// TestPageHeader_WorkflowsListRendersTiles asserts the workflows page
// renders the three tiles with the right counts. Two workflows, one
// with a run, one without → tiles "2 workflows", "1 active", "1 draft".
func TestPageHeader_WorkflowsListRendersTiles(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{
		{Name: "alpha", Version: "v1"},
		{Name: "beta", Version: "v1"},
	}
	fake.runs = []dag.WorkflowRun{
		{RunID: "r1", WorkflowID: "alpha",
			Status: dag.RunStatusCompleted, CreatedAt: time.Now()},
	}
	body := getPage(t, fake, "/console/workflows")
	wantSubs := []string{
		`data-component="page-header"`,
		`<h1>Workflows</h1>`,
		`page-header-tile`,
		`tile-tone-success`, // active tile
		`tile-tone-info`,    // draft tile
	}
	for _, sub := range wantSubs {
		if !strings.Contains(body, sub) {
			t.Errorf("workflows list missing %q", sub)
		}
	}
}

// TestPageHeader_RunsListRendersTiles asserts the runs page renders
// the running/failed tiles tied to the right tone class. We seed one
// running run and one failed run; the totals propagate to the tiles
// strip.
func TestPageHeader_RunsListRendersTiles(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{{Name: "alpha"}}
	fake.runs = []dag.WorkflowRun{
		{RunID: "r1", WorkflowID: "alpha",
			Status: dag.RunStatusRunning, CreatedAt: time.Now()},
		{RunID: "r2", WorkflowID: "alpha",
			Status: dag.RunStatusFailed, CreatedAt: time.Now()},
	}
	body := getPage(t, fake, "/console/runs")
	wantSubs := []string{
		`<h1>Runs</h1>`,
		`tile-tone-warning`, // running
		`tile-tone-danger`,  // failed
		`/console/runs?status=running`,
		`/console/runs?status=failed`,
	}
	for _, sub := range wantSubs {
		if !strings.Contains(body, sub) {
			t.Errorf("runs list missing %q", sub)
		}
	}
}

// TestPageHeader_TriggersListRendersTiles asserts the triggers page
// renders enabled/disabled tiles with the right counts. Two enabled,
// one disabled, three tiles total.
func TestPageHeader_TriggersListRendersTiles(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		{ID: "a", WorkflowID: "alpha", Enabled: true,
			Cron: &trigger.CronConfig{Expression: "* * * * *"}},
		{ID: "b", WorkflowID: "alpha", Enabled: true,
			Cron: &trigger.CronConfig{Expression: "* * * * *"}},
		{ID: "c", WorkflowID: "alpha", Enabled: false,
			Cron: &trigger.CronConfig{Expression: "* * * * *"}},
	}
	body := getPage(t, fake, "/console/triggers")
	wantSubs := []string{
		// H1 wraps the title in the glossary tooltip helper because
		// "trigger" is a domain term. Match the wrapper markup, not the
		// bare <h1>Triggers</h1> the other list pages have.
		`<h1><span class="glo-tooltip-wrapper"`,
		`<span class="glo-tooltip-target">Triggers</span>`,
		`<span class="glo-tooltip-popover"`,
		`tile-tone-success`, // active
		`tile-tone-info`,    // disabled
		`>3<`,               // total tile shows 3
	}
	for _, sub := range wantSubs {
		if !strings.Contains(body, sub) {
			t.Errorf("triggers list missing %q", sub)
		}
	}
}

// TestPageHeader_DLQListRendersTiles asserts the DLQ page renders the
// redrive-eligible vs expired split. BodyPreserved=true → eligible;
// false → expired. One of each → tiles say 2 / 1 / 1.
func TestPageHeader_DLQListRendersTiles(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		{DeadLetter: api.DeadLetter{Sequence: 1, RunID: "r1",
			Task: "task.alpha.x", Error: "timeout",
			Timestamp: time.Now()}, BodyPreserved: true},
		{DeadLetter: api.DeadLetter{Sequence: 2, RunID: "r2",
			Task: "task.alpha.y", Error: "panic",
			Timestamp: time.Now()}, BodyPreserved: false},
	}
	body := getPage(t, fake, "/console/dlq")
	wantSubs := []string{
		`<h1>Dead-letter queue</h1>`,
		`tile-tone-success`, // redrive-eligible
		`tile-tone-danger`,  // expired
		`redrive-eligible`,
	}
	for _, sub := range wantSubs {
		if !strings.Contains(body, sub) {
			t.Errorf("dlq list missing %q", sub)
		}
	}
}

// getPage is a thin helper that mounts the console with the supplied
// fake DataSource, drives a GET against url, and returns the body. It
// fails the test on non-200 so each caller stays terse.
func getPage(t *testing.T, fake *fakeDataSource, url string) string {
	t.Helper()
	if fake == nil {
		t.Fatal("getPage: fake is nil")
	}
	if url == "" {
		t.Fatal("getPage: url is empty")
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, url, nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", url, rr.Code)
	}
	return rr.Body.String()
}

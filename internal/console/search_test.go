// search_test.go covers the cmd+k command palette wiring (T11).
//
// Methodology:
//   - DataSource.Search is exercised against fakeDataSource so the
//     substring + prefix rules are checked without standing up NATS.
//     Each assertion targets one rule (substring, empty, run prefix
//     min length) so a regression points at the exact contract that
//     broke.
//   - /console/api/search renders the command-results partial inside
//     a Datastar SSE patch event. We assert on the event framing and
//     the rendered <li> rows together — the wire format and the
//     template both need to stay correct.
//   - TestCommandPalette_mountedOnEveryPage spot-checks that the
//     overlay markup arrives on representative pages (dashboard,
//     workflows, runs, dlq) so the cmd+k shortcut works everywhere
//     instead of only the dashboard.
package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

func TestSearch_matchesRunIDsAndWorkflows(t *testing.T) {
	t.Parallel()
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{
		sampleWorkflow("demo-pipeline"),
		sampleWorkflow("alpha-pipeline"),
		sampleWorkflow("nightly-report"),
	}
	fake.runs = []dag.WorkflowRun{
		{
			RunID:      "abcd1234-run-id",
			WorkflowID: "demo-pipeline",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  time.Now().Add(-time.Hour),
		},
	}
	hits, err := fake.Search(context.Background(), "pipeline", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	workflowHits := 0
	for _, h := range hits {
		if h.Kind == "workflow" {
			workflowHits++
		}
	}
	if workflowHits != 2 {
		t.Errorf("workflow hits: %d want 2 (demo-pipeline + alpha-pipeline)",
			workflowHits)
	}
	// Negative-space: the third workflow has no "pipeline" in its name
	// and must NOT show up — guards against accidental match-all bugs.
	for _, h := range hits {
		if h.ID == "nightly-report" {
			t.Errorf("nightly-report leaked into results: %+v", h)
		}
	}
}

func TestSearch_emptyQueryReturnsNoHits(t *testing.T) {
	t.Parallel()
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("demo")}
	for _, q := range []string{"", "  ", "\t"} {
		hits, err := fake.Search(context.Background(), q, 10)
		if err != nil {
			t.Fatalf("search(%q): %v", q, err)
		}
		if len(hits) != 0 {
			t.Errorf("search(%q): got %d hits, want 0", q, len(hits))
		}
	}
}

func TestSearch_runIDPrefixRequiresMin4Chars(t *testing.T) {
	t.Parallel()
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		{
			RunID:      "abcdef1234567890",
			WorkflowID: "wf",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  time.Now(),
		},
	}
	// Below the floor: must skip the run scan entirely.
	hits, err := fake.Search(context.Background(), "abc", 10)
	if err != nil {
		t.Fatalf("search short: %v", err)
	}
	for _, h := range hits {
		if h.Kind == "run" {
			t.Errorf("3-char query matched a run: %+v", h)
		}
	}
	// At-or-above the floor: must find the run by prefix.
	hits, err = fake.Search(context.Background(), "abcd", 10)
	if err != nil {
		t.Fatalf("search at-floor: %v", err)
	}
	foundRun := false
	for _, h := range hits {
		if h.Kind == "run" && h.ID == "abcdef1234567890" {
			foundRun = true
		}
	}
	if !foundRun {
		t.Errorf("4-char prefix query missed run abcdef… — hits: %+v", hits)
	}
}

func TestSearchEndpoint_rendersResultsAsSSEPatch(t *testing.T) {
	t.Parallel()
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha-pipeline")}
	fake.triggers = []trigger.TriggerDef{{
		ID:         "nightly-cron",
		WorkflowID: "alpha-pipeline",
		Cron:       &trigger.CronConfig{Expression: "@daily"},
	}}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet, "/console/api/search?q=pipeline", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	// Datastar SSE framing: event header + selector targeting the
	// results container.
	for _, sub := range []string{
		"event: datastar-patch-elements",
		"selector #command-results",
		`href="/console/workflows/alpha-pipeline"`,
		`class="cmdk-result-label">alpha-pipeline`,
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("search response missing %q\n--body--\n%s", sub, body)
		}
	}
}

func TestSearchEndpoint_emptyQueryShowsNoResultsCopy(t *testing.T) {
	t.Parallel()
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet, "/console/api/search?q=", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	// Honesty contract: the palette must render an explicit empty
	// state instead of silently leaving the previous results in place.
	if !strings.Contains(rr.Body.String(), "cmdk-empty") {
		t.Errorf("empty-query response missing cmdk-empty marker:\n%s",
			rr.Body.String())
	}
}

func TestCommandPalette_mountedOnEveryPage(t *testing.T) {
	t.Parallel()
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("demo")}
	fake.runs = []dag.WorkflowRun{
		{
			RunID:      "run-1",
			WorkflowID: "demo",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  time.Now(),
		},
	}
	h := mountWithFake(t, fake)
	pages := []string{
		"/console/",
		"/console/workflows",
		"/console/runs",
		"/console/triggers",
		"/console/dlq",
	}
	for _, p := range pages {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, p, nil))
		if rr.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200", p, rr.Code)
			continue
		}
		body := rr.Body.String()
		if !strings.Contains(body, `id="command-palette"`) {
			t.Errorf("%s: missing command-palette overlay", p)
		}
		if !strings.Contains(body, `id="command-results"`) {
			t.Errorf("%s: missing command-results container", p)
		}
		// Datastar attribute MUST be kebab-form (`data-on:input`) — the
		// regression class from earlier phases was `data-on-input` which
		// silently no-op'd. Asserting the canonical form here keeps the
		// palette wired against future engine updates.
		if !strings.Contains(body, "data-on:input") {
			t.Errorf("%s: missing data-on:input attribute (kebab-form regression)",
				p)
		}
	}
}

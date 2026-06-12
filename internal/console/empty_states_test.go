// internal/console/empty_states_test.go
// Methodology: render every list page with an empty fake DataSource
// and assert each page surfaces an operator-actionable primary
// action (or, for DLQ, the existing healthy-zero copy). The CTAs
// give a first-time operator something to do; without them every
// page was just a blank table.
//
// PR #310 (R4) layered the shared empty-state partial on top of the
// existing tutorial blocks: the partial owns the icon + title +
// description + optional primary action (matching iii); the tutorial
// blocks below still own the CLI cheat sheets. Tests in this file
// assert both: the old `console-empty-action` substrings still
// render, and the new `console-empty-state` block carries the typed
// title/description/action from EmptyState.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func renderPath(t *testing.T, path string) string {
	t.Helper()
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: status=%d", path, rec.Code)
	}
	return rec.Body.String()
}

func TestEmptyState_workflowsShowsRegisterCTA(t *testing.T) {
	body := renderPath(t, "/console/workflows")
	if !strings.Contains(body, "console-empty-action") {
		t.Errorf("workflows empty state missing CTA block")
	}
	if !strings.Contains(body, "dagnats workflow register") {
		t.Errorf("workflows CTA missing register command")
	}
}

func TestEmptyState_runsShowsRunStartCTA(t *testing.T) {
	body := renderPath(t, "/console/runs")
	if !strings.Contains(body, "console-empty-action") {
		t.Errorf("runs empty state missing CTA block")
	}
	if !strings.Contains(body, "dagnats run start") {
		t.Errorf("runs CTA missing 'dagnats run start' hint")
	}
	if !strings.Contains(body, "/console/triggers") {
		t.Errorf("runs CTA missing triggers cross-link")
	}
}

func TestEmptyState_triggersShowsCreateCTA(t *testing.T) {
	body := renderPath(t, "/console/triggers")
	if !strings.Contains(body, "console-empty-action") {
		t.Errorf("triggers empty state missing CTA block")
	}
	if !strings.Contains(body, "dagnats trigger create") {
		t.Errorf("triggers CTA missing cron-create command")
	}
}

func TestEmptyState_auditShowsTryActionsCTA(t *testing.T) {
	body := renderPath(t, "/console/audit")
	if !strings.Contains(body, "console-empty-action") {
		t.Errorf("audit empty state missing CTA block")
	}
	if !strings.Contains(body, "/console/dlq") {
		t.Errorf("audit CTA missing DLQ link")
	}
}

func TestEmptyState_dlqStaysWithHealthyCopy(t *testing.T) {
	// DLQ keeps the existing happy-zero-state copy from PR 5b.
	body := renderPath(t, "/console/dlq")
	if !strings.Contains(body, "workflows are healthy") {
		t.Errorf("DLQ empty state lost healthy copy")
	}
}

// TestEmptyState_ValidateRejectsEmptyTitle exercises the construction-
// time validation surface directly. Mirrors the NewPageHeader test
// pattern (page_header_test.go).
func TestEmptyState_ValidateRejectsEmptyTitle(t *testing.T) {
	cases := []struct {
		name string
		es   EmptyState
		want string
	}{
		{
			name: "empty title",
			es:   EmptyState{Description: "x"},
			want: "title is empty",
		},
		{
			name: "empty description",
			es:   EmptyState{Title: "x"},
			want: "description is empty",
		},
		{
			name: "action missing label",
			es: EmptyState{
				Title: "x", Description: "y",
				PrimaryAction: &EmptyStateAction{Href: "/z"},
			},
			want: "label is empty",
		},
		{
			name: "action missing href",
			es: EmptyState{
				Title: "x", Description: "y",
				PrimaryAction: &EmptyStateAction{Label: "Go"},
			},
			want: "href is empty",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewEmptyState(tc.es)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err=%q does not contain %q", err.Error(), tc.want)
			}
		})
	}
	// Positive: a fully-valid struct should round-trip.
	got, err := NewEmptyState(EmptyState{
		Title: "T", Description: "D",
		PrimaryAction: &EmptyStateAction{Label: "L", Href: "/h"},
	})
	if err != nil {
		t.Fatalf("valid struct rejected: %v", err)
	}
	if got.Title != "T" || got.PrimaryAction.Href != "/h" {
		t.Errorf("round-trip lost fields: %+v", got)
	}
}

// TestEmptyState_WorkflowsList asserts the partial renders on the
// workflows list with icon + title + description + action when no
// workflows are registered. Substrings target the typed shape from
// newWorkflowsEmptyState().
func TestEmptyState_WorkflowsList(t *testing.T) {
	body := renderPath(t, "/console/workflows")
	wants := []string{
		`data-component="empty-state"`,
		`data-kind="workflow"`,
		"No workflows registered",
		"Register a workflow definition",
		"console-empty-state-action",
		`href="/docs"`,
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("workflows empty-state missing %q", want)
		}
	}
}

// TestEmptyState_RunsList — same shape as workflows, runs flavour.
func TestEmptyState_RunsList(t *testing.T) {
	body := renderPath(t, "/console/runs")
	wants := []string{
		`data-component="empty-state"`,
		`data-kind="run"`,
		"No runs yet",
		"Start a run from the CLI",
		`href="/console/triggers"`,
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("runs empty-state missing %q", want)
		}
	}
}

// TestEmptyState_TriggersList — uses the Zap-equivalent ⚡ glyph
// (data-kind="trigger"). Copy mirrors iii's triggers.tsx:766.
func TestEmptyState_TriggersList(t *testing.T) {
	body := renderPath(t, "/console/triggers")
	wants := []string{
		`data-component="empty-state"`,
		`data-kind="trigger"`,
		"No triggers configured",
		"cron, webhook",
		"console-empty-state-action",
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("triggers empty-state missing %q", want)
		}
	}
}

// TestEmptyState_DLQList — the "good news" variant: title celebrates
// health, no primary action, "workflows are healthy" copy survives.
func TestEmptyState_DLQList(t *testing.T) {
	body := renderPath(t, "/console/dlq")
	wants := []string{
		`data-component="empty-state"`,
		`data-kind="dlq"`,
		"Your workflows are healthy",
		"No dead letters",
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("dlq empty-state missing %q", want)
		}
	}
	// And no primary action button on this page.
	if strings.Contains(body, "console-empty-state-action") {
		t.Errorf("dlq empty state should not render a primary action")
	}
}

// TestEmptyState_ReadOnly verifies that when CONSOLE_READ_ONLY is on,
// the primary action renders as an aria-disabled span with the
// tooltip referencing CONSOLE_READ_ONLY=false. Workflows page is the
// test surface (it has a primary action).
func TestEmptyState_ReadOnly(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/workflows", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	body := rec.Body.String()
	// The action is rendered as an aria-disabled span, not an anchor.
	if !strings.Contains(body, `aria-disabled="true"`) {
		t.Errorf("read-only empty-state action missing aria-disabled")
	}
	if !strings.Contains(body, "CONSOLE_READ_ONLY") {
		t.Errorf("read-only empty-state action missing CONSOLE_READ_ONLY tooltip")
	}
	// And no hyperlink form for the action.
	if strings.Contains(body, `<a class="btn btn-outline console-empty-state-action"`) {
		t.Errorf("read-only mode still rendered an <a> for the action")
	}
}

func TestNotFound_listsPopularDestinations(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/zzz-unknown", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "console-popular-destinations") {
		t.Errorf("404 missing popular-destinations block")
	}
	for _, link := range []string{
		"/console/workflows",
		"/console/runs",
		"/console/triggers",
		"/console/dlq",
		"/console/metrics",
		"/console/audit",
	} {
		if !strings.Contains(body, link) {
			t.Errorf("404 popular destinations missing %s", link)
		}
	}
}

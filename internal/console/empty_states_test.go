// internal/console/empty_states_test.go
// Methodology: render every list page with an empty fake DataSource
// and assert each page surfaces an operator-actionable primary
// action (or, for DLQ, the existing healthy-zero copy). The CTAs
// give a first-time operator something to do; without them every
// page was just a blank table.
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
	body := renderPath(t, "/console/ops/audit")
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
		"/console/ops/metrics",
		"/console/ops/audit",
	} {
		if !strings.Contains(body, link) {
			t.Errorf("404 popular destinations missing %s", link)
		}
	}
}

// internal/console/onboarding_test.go
// Batch 9 (ITEM 2) replaced the dismissible "Welcome to the dagnats
// console" onboarding card with a slim, persistent one-line explainer
// bar carrying the mockup's exact "Workers register Functions ·
// Triggers fire Workflows · ..." copy.
//
// Methodology: render /console/ via the fake DataSource and assert the
// explainer bar (with bold keywords) is present and the old welcome
// card + its dismiss button + its now-dead JS reference are gone.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDashboardExplainerReplacesWelcomeCard pins the exact explainer
// copy + bolded keywords and asserts the dismissible welcome card is
// gone. Positive: explainer present with bold Workers/Functions/Run.
// Negative: no onboarding aside / dismiss button / onboarding.js.
func TestDashboardExplainerReplacesWelcomeCard(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	if !strings.Contains(body, `class="dashboard-explainer"`) {
		t.Errorf("dashboard missing the slim explainer bar")
	}
	// Exact mockup phrasing fragments (bold keywords + connectives).
	for _, frag := range []string{
		"<b>Workers</b> register", "<b>Functions</b>",
		"<b>Triggers</b> fire", "<b>Workflows</b>",
		"<b>Workflow</b> is a DAG of steps",
		"call a\n    <b>Function</b>", "every firing is a <b>Run</b>",
		"watch live and replay",
	} {
		if !strings.Contains(body, frag) {
			t.Errorf("explainer missing fragment %q", frag)
		}
	}
	// Negative space: the dismissible welcome card and its wiring are
	// fully removed (no dead affordance).
	if strings.Contains(body, "Welcome to the dagnats console") {
		t.Errorf("welcome card copy must be removed")
	}
	if strings.Contains(body, `id="console-onboarding-dismiss"`) {
		t.Errorf("onboarding dismiss button must be removed")
	}
	if strings.Contains(body, "/console/assets/onboarding.js") {
		t.Errorf("dead onboarding.js reference must be removed")
	}
}

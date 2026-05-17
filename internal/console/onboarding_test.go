// internal/console/onboarding_test.go
// Methodology: render /console/ via the existing fake DataSource and
// assert the onboarding aside is present (hidden) in the layout +
// the onboarding.js asset is referenced. The actual show / hide is
// pure JS — the browser_smoke_test covers the localStorage gating
// when an operator runs the agent-browser harness.
package console

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOnboardingBannerRenderedOnDashboard(t *testing.T) {
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
	if !strings.Contains(body, `id="console-onboarding"`) {
		t.Errorf("dashboard missing onboarding aside element")
	}
	if !strings.Contains(body, `id="console-onboarding-dismiss"`) {
		t.Errorf("dashboard missing onboarding dismiss button")
	}
	if !strings.Contains(body, "Welcome to the dagnats console") {
		t.Errorf("onboarding header copy not rendered")
	}
	// Ships hidden by default; the JS makes it visible only when
	// localStorage doesn't carry the dismiss flag.
	if !strings.Contains(body, `hidden`) {
		t.Errorf("onboarding aside must start hidden")
	}
}

func TestOnboardingAssetReferencedByLayout(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/", nil,
	))
	if !strings.Contains(rec.Body.String(),
		"/console/assets/onboarding.js") {
		t.Errorf("layout missing /console/assets/onboarding.js script tag")
	}
}

func TestOnboardingAssetServed(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/assets/onboarding.js", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("onboarding.js status = %d, want 200", rec.Code)
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	asStr := string(body)
	if !strings.Contains(asStr, "dagnats-console-onboarded") {
		t.Errorf("onboarding.js missing the localStorage key constant")
	}
	if !strings.Contains(asStr, "console-onboarding-dismiss") {
		t.Errorf("onboarding.js missing the dismiss button id")
	}
}

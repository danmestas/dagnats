// internal/console/metrics_anomaly_click_test.go
// Methodology: pure-Go assertion of the URL shape the anomaly-click
// handler navigates to. Pulled from the JS expectation so the Go
// test catches drift even when the JS unit-test (in
// browser_smoke_test.go) is gated on a working agent-browser
// install. Verifies the contract the runs-list filter accepts.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunsList_acceptsSinceUntilParams(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	url := "/console/runs?workflow=alpha&status=failed" +
		"&since=1700000000&until=1700000180"
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<h1>Runs</h1>") {
		t.Errorf("runs page didn't render")
	}
}

func TestRunsList_invalidSinceParamFallsThroughToRange(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	// Garbage in since / until → parseUnixSecsParam returns 0, the
	// filter switch falls through to filterRunsByRange. Should still
	// 200, not 500.
	url := "/console/runs?since=garbage&until=notanumber"
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestMetricsAsset_exportsAnomalyURLForBuilder(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/assets/metrics.js", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics.js status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// The client's runs URL must match what RunsListView accepts
	// (workflow, status, since, until). The function name is
	// stable so this test can guard the wire shape.
	if !strings.Contains(body, "__dagnatsAnomalyURLFor") {
		t.Errorf("metrics.js missing exported anomalyURLFor")
	}
	if !strings.Contains(body, `"/console/runs?"`) {
		t.Errorf("metrics.js missing /console/runs URL prefix")
	}
	if !strings.Contains(body, "status=failed") {
		t.Errorf("metrics.js missing status=failed default")
	}
}

// TestMetricsAsset_zoomSurvivesLiveRefresh guards the drag-zoom
// affordance: the SSE refresh path (applySetData) must NOT re-pin the
// x-scale while the user has an active manual zoom. The chart cursor is
// drag-zoomable on x; a forced setScale on every 4Hz tick would snap the
// user's zoom back to the server window within ~250ms, making zoom dead.
// Positive: a userZoomed guard exists and gates the forced setScale.
// Negative: applySetData must not call setScale("x", ...) without first
// checking the zoom flag.
func TestMetricsAsset_zoomSurvivesLiveRefresh(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/assets/metrics.js", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics.js status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// The zoom flag must exist and be wired into the cursor/scale hook.
	if !strings.Contains(body, "__userZoomed") {
		t.Errorf("metrics.js missing __userZoomed guard flag")
	}
	// The forced re-pin in applySetData must be gated on the flag being
	// false, never unconditional.
	if !strings.Contains(body, "!canvas.__userZoomed") {
		t.Errorf("metrics.js forced setScale not gated on !__userZoomed")
	}
	// A setScale hook must clear the flag when x returns to the pinned
	// full window (zoom reset / double-click), so live refresh resumes.
	if !strings.Contains(body, "setScale:") {
		t.Errorf("metrics.js missing setScale hook to detect zoom reset")
	}
}

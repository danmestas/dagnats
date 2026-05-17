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

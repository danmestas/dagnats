// internal/console/end_of_arc_smoke_test.go
// Methodology: PR 8 closes the control-plane arc (PRs 1-8). This
// smoke test boots a Mount with a fake DataSource and hits every
// page rendered across the arc, asserting:
//   - no 500 anywhere (sanity over the full surface),
//   - the response body looks like layout-wrapped HTML.
//
// It's intentionally fixture-light: the page-specific tests already
// cover the rendered shape. This guard catches a latent regression
// in mount wiring (a renamed handler, a missing template entry).
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEndOfArc_everyPageReturnsValidHTML(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	// The list is the surface the operator can reach via the nav,
	// the dashboard links, the 404 popular-destinations, and the
	// programmatic redirects. Order matches the nav left-to-right.
	pages := []struct {
		path string
		want int
	}{
		{"/console/", http.StatusOK},
		{"/console/workflows", http.StatusOK},
		{"/console/runs", http.StatusOK},
		{"/console/triggers", http.StatusOK},
		{"/console/dlq", http.StatusOK},
		{"/console/ops", http.StatusOK},
		{"/console/ops/workers", http.StatusOK},
		{"/console/ops/leases", http.StatusOK},
		{"/console/ops/kv", http.StatusOK},
		{"/console/ops/audit", http.StatusOK},
		{"/console/ops/metrics", http.StatusOK},
		// Unknown path → layout-wrapped 404.
		{"/console/this-is-not-a-page", http.StatusNotFound},
	}
	const maxPages = 32
	if len(pages) > maxPages {
		t.Fatalf("test list exceeds maxPages (%d)", maxPages)
	}
	for _, p := range pages {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p.path, nil))
		if rec.Code != p.want {
			t.Errorf("GET %s: status=%d, want %d\n%s",
				p.path, rec.Code, p.want, truncBody(rec.Body.String()))
			continue
		}
		body := rec.Body.String()
		if !strings.Contains(body, "<!doctype html>") &&
			!strings.Contains(body, "<!DOCTYPE html>") {
			t.Errorf("GET %s: response is not HTML-doctype-wrapped", p.path)
		}
		if !strings.Contains(body, `class="console-header"`) {
			t.Errorf("GET %s: response missing layout chrome", p.path)
		}
	}
}

func truncBody(s string) string {
	const n = 400
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

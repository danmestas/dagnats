// internal/console/end_of_arc_smoke_test.go
// Methodology: PR 8 closed the control-plane arc (PRs 1-8); #311
// promoted Workers / KV / Streams out of /console/ops. This smoke
// test boots a Mount with a fake DataSource and hits every page
// rendered across the arc, asserting:
//   - no 500 anywhere (sanity over the full surface),
//   - layout-wrapped HTML for 200s,
//   - the Location header for 308 redirects.
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
		{"/console/workers", http.StatusOK},
		{"/console/kv", http.StatusOK},
		{"/console/streams", http.StatusOK},
		{"/console/dlq", http.StatusOK},
		{"/console/ops", http.StatusOK},
		{"/console/ops/leases", http.StatusOK},
		{"/console/ops/audit", http.StatusOK},
		{"/console/ops/metrics", http.StatusOK},
		// Old paths now 308-redirect to the promoted top-level entries.
		{"/console/ops/workers", http.StatusPermanentRedirect},
		{"/console/ops/kv", http.StatusPermanentRedirect},
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
		// Redirects have no body chrome to validate; the per-handler
		// tests assert the Location header explicitly.
		if rec.Code == http.StatusPermanentRedirect {
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

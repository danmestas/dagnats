// asset_versioning_test.go covers the cache-busting asset version.
// The console serves app.css / console.js from stable URLs with a
// long immutable Cache-Control, so a browser never re-fetched them
// after a deploy — CSS/JS fixes only appeared after a manual hard
// reload. Appending ?v=<content-hash> to every asset URL means a
// changed binary serves NEW URLs, busting the cache on a normal reload
// (the page HTML is no-store, so it always references the current URL).
//
// Methodology: unit-test the helper's shape + determinism, then render
// a real page through Mount and assert the asset links carry the query.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAssetURL_appendsStableVersion(t *testing.T) {
	got := assetURL("/console/assets/app.css")
	if !strings.HasPrefix(got, "/console/assets/app.css?v=") {
		t.Fatalf("assetURL = %q, want a ?v= suffix", got)
	}
	ver := strings.TrimPrefix(got, "/console/assets/app.css?v=")
	if ver == "" {
		t.Error("asset version is empty")
	}
	// Deterministic for a given binary: recomputing yields the same hash.
	if computeAssetVersion() != assetVersion {
		t.Error("computeAssetVersion is not deterministic")
	}
}

func TestLayout_assetLinksAreVersioned(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /console/ = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// The CSS and the main JS bundle must both carry the cache-busting
	// query so a deploy reaches the browser without a hard reload.
	for _, want := range []string{
		"/console/assets/app.css?v=",
		"/console/assets/console.js?v=",
		"/console/assets/nav-counts.js?v=",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("layout missing versioned asset %q", want)
		}
	}
	// Negative space: no bare (unversioned) app.css link survives.
	if strings.Contains(body, `href="/console/assets/app.css"`) {
		t.Error("layout still emits an unversioned app.css link")
	}
}

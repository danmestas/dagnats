// nav_ia_test.go covers the B3 nav/IA remediation: the Ops hub is
// removed and its children (Metrics / Audit / Leases) are promoted to
// top-level routes, the old /console/ops* paths 301-redirect to their
// new homes, and the primary nav carries the regraded 3-layer rail
// with leading glyph signifiers and no mystery-meat tooltip wrappers.
//
// Methodology:
//   - In-memory fakeDataSource feeds page renders.
//   - httptest.Recorder asserts status + Location + body substrings.
//   - Each test creates its own console.Mount; nothing is shared.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNavIA_promotedTopLevelRoutes asserts the promoted children now
// answer at the top level with a 200.
func TestNavIA_promotedTopLevelRoutes(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	for _, path := range []string{
		"/console/metrics",
		"/console/audit",
		"/console/leases",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200", path, rr.Code)
			continue
		}
		if !strings.Contains(rr.Body.String(), `class="console-header"`) {
			t.Errorf("GET %s: missing layout chrome", path)
		}
	}
}

// TestNavIA_oldOpsPathsRedirect asserts every legacy /console/ops* URL
// 301-redirects to its promoted home so bookmarks don't break.
func TestNavIA_oldOpsPathsRedirect(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	cases := []struct {
		from string
		to   string
	}{
		{"/console/ops", "/console/"},
		{"/console/ops/metrics", "/console/metrics"},
		{"/console/ops/audit", "/console/audit"},
		{"/console/ops/leases", "/console/leases"},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, c.from, nil))
		if rr.Code != http.StatusMovedPermanently {
			t.Errorf("GET %s: status = %d, want 301", c.from, rr.Code)
			continue
		}
		if got := rr.Header().Get("Location"); got != c.to {
			t.Errorf("GET %s: Location = %q, want %q", c.from, got, c.to)
		}
	}

	// A bookmarked deep link with a query string must carry it across the
	// redirect — that is the whole reason the helper preserves RawQuery.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/ops/audit?actor=alice", nil))
	if got := rr.Header().Get("Location"); got != "/console/audit?actor=alice" {
		t.Errorf("query not preserved: Location = %q, want %q", got, "/console/audit?actor=alice")
	}
}

// TestNavIA_promotedRoutesHighlightNav asserts each promoted page sets
// the Section that lights up its new top-level nav item.
func TestNavIA_promotedRoutesHighlightNav(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	cases := []struct {
		path string
		// after is the href substring that must be immediately followed
		// (ignoring nav-alignment whitespace) by an is-active class.
		after string
	}{
		{"/console/metrics", `href="/console/metrics"`},
		{"/console/audit", `href="/console/audit"`},
		{"/console/leases", `href="/console/leases"`},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, c.path, nil))
		body := rr.Body.String()
		idx := strings.Index(body, c.after)
		if idx < 0 {
			t.Errorf("GET %s: nav href %q absent", c.path, c.after)
			continue
		}
		tail := body[idx+len(c.after):]
		// The very next attribute on this anchor is class; assert it
		// carries is-active before the anchor's closing >.
		end := strings.IndexByte(tail, '>')
		if end < 0 || !strings.Contains(tail[:end], "is-active") {
			t.Errorf("GET %s: nav item not active", c.path)
		}
	}
}

// TestNavIA_navSetNoOpsItem asserts the regraded nav: the promoted
// items are present, the Ops landing item is gone, and the DLQ / KV
// links are plain (no mystery-meat tooltip wrapper). Functions
// replaces the Task Types label.
func TestNavIA_navSetNoOpsItem(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		`href="/console/metrics"`,
		`href="/console/audit"`,
		`href="/console/leases"`,
		`href="/console/functions"`,
		"console-nav-glyph",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("nav missing %q", want)
		}
	}
	for _, gone := range []string{
		`href="/console/ops"`,
		"glo-tooltip-wrapper",
		">Task Types<",
	} {
		if strings.Contains(body, gone) {
			t.Errorf("nav still references %q after remediation", gone)
		}
	}
}

// TestNavIA_navCrawlNo404 crawls every primary-nav href and asserts
// none 404 — the regraded nav must not point at dead routes.
func TestNavIA_navCrawlNo404(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	for _, path := range []string{
		"/console/",
		"/console/workflows",
		"/console/functions",
		"/console/workers",
		"/console/triggers",
		"/console/runs",
		"/console/dlq",
		"/console/logs",
		"/console/metrics",
		"/console/server",
		"/console/connections",
		"/console/streams",
		"/console/consumers",
		"/console/concurrency",
		"/console/kv",
		"/console/audit",
		"/console/leases",
		"/console/config",
		// Batch 6 detail routes: even with an empty fake these render the
		// honest not-found state at 200, never a 404.
		"/console/streams/WORKFLOW_HISTORY",
		"/console/triggers/cron-1",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code == http.StatusNotFound {
			t.Errorf("nav link %s returned 404", path)
		}
	}
}

// TestNavIA_functionsRename asserts the page that used to be "Task
// Types" now titles itself "Functions".
func TestNavIA_functionsRename(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/functions", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /console/functions: status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Functions") {
		t.Errorf("functions page missing 'Functions' title")
	}
	if strings.Contains(body, "Task Types") {
		t.Errorf("functions page still says 'Task Types'")
	}
}

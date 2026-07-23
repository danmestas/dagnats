// routes_test.go covers the typed console route registry: the ordered
// inventory produced by consoleRoutes, the pre-registration validation
// in validateConsoleRoutes, and the non-mutating guarantee of
// registerConsoleRoutes when validation fails.
//
// Methodology:
//   - The inventory test pins every unconditional pattern in the exact
//     order consoleRoutes emits it, then asserts count, order, and the
//     absence of duplicates. It is the reviewable-as-data source of truth.
//   - Validation tests craft deliberately invalid slices (empty pattern,
//     nil handler, duplicate pattern) and assert a returned error.
//   - The non-mutation test drives an invalid slice through
//     registerConsoleRoutes and proves the mux was never touched by
//     probing a pattern that would have been installed and expecting 404.
//   - The method-dispatch test locks /console/triggers 405 + Allow header.
//   - The builder-precondition tests drive the two invariants that would
//     otherwise fail silently at request time rather than at boot: a
//     redirect target missing its leading slash (resolved relative to the
//     request path by http.Redirect) and a jsSourceRoute basename carrying
//     a path separator (which would corrupt both the URL pattern and the
//     embedded sources/ lookup). Each asserts the panic fires on the bad
//     input and does not fire on the good one.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// wantConsoleRoutePatterns is the full ordered inventory of every
// unconditional console pattern. Fixtures are gated separately and are
// intentionally absent (DAGNATS_ENV is unset in tests, but the fixture
// route is appended by consoleRoutes only when fixturesEnabled()).
var wantConsoleRoutePatterns = []string{
	// page + dispatch subtrees
	"/console/",
	"/console/workflows",
	"/console/workflows/",
	"/console/runs",
	"/console/runs/lookup",
	"/console/runs/",
	"/console/triggers",
	"/console/triggers/",
	"/console/traces",
	"/console/traces/",
	"/console/dlq",
	"/console/dlq/",
	"/console/workers",
	"/console/workers/",
	"/console/services",
	"/console/kv",
	"/console/streams",
	"/console/streams/",
	"/console/consumers",
	"/console/server",
	"/console/connections",
	"/console/concurrency",
	"/console/agents",
	"/console/logs",
	"/console/logs/export",
	"/console/logs/clear",
	"/console/config",
	"/console/task-types",
	"/console/functions",
	"/console/functions/",
	"/console/metrics",
	"/console/audit",
	// api fragments + data
	"/console/api/run/",
	"/console/api/fragments/workflows-list",
	"/console/api/fragments/runs-list",
	"/console/api/dlq/",
	"/console/api/runs/",
	"/console/api/search",
	"/console/api/nav-counts",
	"/console/api/metrics/chart/",
	// server-sent events
	"/console/sse/logs",
	"/console/sse/metrics",
	"/console/sse/heartbeat",
	"/console/sse/dashboard",
	"/console/sse/runs",
	"/console/sse/runs/",
	"/console/sse/agents",
	"/console/sse/triggers",
	"/console/sse/dlq",
	// legacy /console/ops* redirects
	"/console/ops",
	"/console/ops/workers",
	"/console/ops/leases",
	"/console/ops/kv",
	"/console/ops/audit",
	"/console/ops/metrics",
	// static assets
	"/console/assets/console.js",
	"/console/assets/basecoat.css",
	"/console/assets/uplot.min.js",
	"/console/assets/app.css",
	"/console/assets/connection-state.js",
	"/console/assets/toast.js",
	"/console/assets/count-chip.js",
	"/console/assets/metrics.js",
	"/console/assets/build-info-copy.js",
	"/console/assets/sidebar-collapse.js",
	"/console/assets/nav-counts.js",
	"/console/assets/logs.js",
	"/console/assets/fonts/",
}

func testTemplateSet(t *testing.T) *templateSet {
	t.Helper()
	ts, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	return ts
}

func TestConsoleRoutes_inventoryOrderAndUniqueness(t *testing.T) {
	ts := testTemplateSet(t)
	got := consoleRoutes(ts, Config{})

	if len(got) != len(wantConsoleRoutePatterns) {
		t.Fatalf("route count = %d, want %d", len(got), len(wantConsoleRoutePatterns))
	}
	seen := make(map[string]struct{}, len(got))
	for i, rt := range got {
		if rt.pattern != wantConsoleRoutePatterns[i] {
			t.Errorf("route[%d] pattern = %q, want %q", i, rt.pattern, wantConsoleRoutePatterns[i])
		}
		if rt.handler == nil {
			t.Errorf("route[%d] %q has nil handler", i, rt.pattern)
		}
		if _, dup := seen[rt.pattern]; dup {
			t.Errorf("route[%d] %q is a duplicate", i, rt.pattern)
		}
		seen[rt.pattern] = struct{}{}
	}
}

func TestValidateConsoleRoutes_accepts_validTable(t *testing.T) {
	ts := testTemplateSet(t)
	if err := validateConsoleRoutes(consoleRoutes(ts, Config{})); err != nil {
		t.Fatalf("validateConsoleRoutes(valid) = %v, want nil", err)
	}
}

func TestValidateConsoleRoutes_rejects(t *testing.T) {
	ok := func(w http.ResponseWriter, r *http.Request) {}
	cases := []struct {
		name  string
		table []consoleRoute
	}{
		{"empty pattern", []consoleRoute{{pattern: "", handler: ok}}},
		{"nil handler", []consoleRoute{{pattern: "/console/x", handler: nil}}},
		{"duplicate pattern", []consoleRoute{
			{pattern: "/console/x", handler: ok},
			{pattern: "/console/x", handler: ok},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := validateConsoleRoutes(c.table); err == nil {
				t.Fatalf("validateConsoleRoutes(%s) = nil, want error", c.name)
			}
		})
	}
}

func TestRegisterConsoleRoutes_leavesMuxUnmodifiedOnInvalidTable(t *testing.T) {
	mux := http.NewServeMux()
	invalid := []consoleRoute{
		{pattern: "/console/dup", handler: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTeapot)
		}},
		{pattern: "/console/dup", handler: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTeapot)
		}},
	}
	if err := registerConsoleRoutes(mux, invalid); err == nil {
		t.Fatalf("registerConsoleRoutes(invalid) = nil, want error")
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/dup", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("after failed registration, GET /console/dup = %d, want 404 (mux untouched)", rr.Code)
	}
}

func TestRedirectTo_rejectsNonAbsoluteTarget(t *testing.T) {
	for _, bad := range []string{"", "console/", "https://evil.example/x"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("redirectTo(%q) did not panic, want panic", bad)
				}
			}()
			redirectTo(bad)
		}()
	}
	if redirectTo("/console/") == nil {
		t.Fatalf("redirectTo(%q) = nil handler, want non-nil", "/console/")
	}
}

func TestJSSourceRoute_rejectsBasenameWithSeparator(t *testing.T) {
	// A separator-bearing basename already blows up downstream on the
	// embedded-read miss, so the panic alone proves nothing: assert the
	// message names jsSourceRoute, pinning the contract at the layer that
	// owns it rather than at an accidental file-not-found.
	for _, bad := range []string{"", "sources/toast.js", "../toast.js"} {
		func() {
			defer func() {
				got, ok := recover().(string)
				if !ok {
					t.Fatalf("jsSourceRoute(%q) did not panic with a string, want panic", bad)
				}
				if !strings.HasPrefix(got, "jsSourceRoute:") {
					t.Fatalf("jsSourceRoute(%q) panicked with %q, want a jsSourceRoute: contract", bad, got)
				}
			}()
			jsSourceRoute(bad)
		}()
	}
	got := jsSourceRoute("toast.js")
	if got.pattern != "/console/assets/toast.js" {
		t.Fatalf("jsSourceRoute pattern = %q, want %q", got.pattern, "/console/assets/toast.js")
	}
	if got.handler == nil {
		t.Fatalf("jsSourceRoute handler = nil, want non-nil")
	}
}

func TestConsoleTriggersRoot_methodDispatchAndAllowHeader(t *testing.T) {
	h := newTestConsole(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/console/triggers", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /console/triggers = %d, want 405", rr.Code)
	}
	if got := rr.Header().Get("Allow"); got != "GET, POST" {
		t.Fatalf("Allow = %q, want %q", got, "GET, POST")
	}
}

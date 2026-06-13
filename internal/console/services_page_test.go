// services_page_test.go exercises the /console/services roster page and
// the pure serviceRowsFromDefs projection without standing up NATS.
//
// Methodology:
//   - The page tests reuse the fakeDataSource + mountWithFake helpers
//     from pages_test.go. Seeding fake.services drives the render so the
//     row/empty-state logic gets coverage without a JetStream KV bucket
//     existing. Assertions look for stable substrings the template emits
//     (positive space) and confirm fabricated columns / dead drill-ins
//     are absent (negative space).
//   - serviceRowsFromDefs is a pure function over []worker.ServiceDef, so
//     its test builds defs in-memory and asserts the derived
//     Name / Registered / Note fields, the honest dash on empties, the
//     name sort, and the empty-input contract directly.
//   - Each page test creates its own console.Mount with the fake; tests
//     never share state. Min 2 assertions per test (positive + negative).
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// TestServiceRowsFromDefs_projectsRealFields asserts the pure projection
// maps the three ServiceDef fields the KV carries, renders the honest
// dash for empty Description and zero RegisteredAt, sorts by Name, and
// returns an empty slice for empty input.
func TestServiceRowsFromDefs_projectsRealFields(t *testing.T) {
	at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	defs := []worker.ServiceDef{
		{Name: "zeta", Description: "last alphabetically", RegisteredAt: at},
		{Name: "alpha", Description: "", RegisteredAt: time.Time{}},
	}
	rows := serviceRowsFromDefs(defs)
	if len(rows) != 2 {
		t.Fatalf("rows len: got %d, want 2", len(rows))
	}
	// Sorted by Name: alpha first.
	if rows[0].Name != "alpha" {
		t.Errorf("rows[0].Name: got %q, want alpha", rows[0].Name)
	}
	if rows[1].Name != "zeta" {
		t.Errorf("rows[1].Name: got %q, want zeta", rows[1].Name)
	}
	// Empty Description and zero RegisteredAt render the honest dash.
	if rows[0].Note != dash {
		t.Errorf("alpha Note: got %q, want %q", rows[0].Note, dash)
	}
	if rows[0].Registered != dash {
		t.Errorf("alpha Registered: got %q, want %q", rows[0].Registered, dash)
	}
	// Real fields project through verbatim.
	if rows[1].Note != "last alphabetically" {
		t.Errorf("zeta Note: got %q", rows[1].Note)
	}
	if !strings.HasPrefix(rows[1].Registered, "2026-06-01T12:00:00") {
		t.Errorf("zeta Registered: got %q, want RFC3339", rows[1].Registered)
	}
}

func TestServiceRowsFromDefs_emptyInput(t *testing.T) {
	rows := serviceRowsFromDefs(nil)
	if rows == nil {
		t.Fatalf("rows: got nil, want non-nil empty slice")
	}
	if len(rows) != 0 {
		t.Errorf("rows len: got %d, want 0", len(rows))
	}
}

// TestServePageServices_rendersRoster asserts the page renders the real
// columns from seeded services, omits the fabricated mockup columns, and
// is non-clickable (no detail drill-in).
func TestServePageServices_rendersRoster(t *testing.T) {
	fake := newFakeDS()
	fake.services = []worker.ServiceDef{
		{
			Name:         "trigger-svc",
			Description:  "fires scheduled workflows",
			RegisteredAt: time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC),
		},
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/services", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"trigger-svc", "fires scheduled workflows",
		// Real column headers backed by the KV registration.
		">Service<", ">Registered<", ">Note<",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// Negative space: columns the KV does not carry must NOT be rendered
	// as headers — rendering them would fabricate liveness/version data.
	for _, absent := range []string{
		">Kind<", ">Version<", ">Commit<", ">Instances<",
		">Status<", ">Last seen<",
	} {
		if strings.Contains(body, absent) {
			t.Errorf("body fabricates omitted column header %q", absent)
		}
	}
	// Negative space: no detail drill-in exists, so rows are not
	// clickable — no chevron, no per-service href.
	if strings.Contains(body, `href="/console/services/`) {
		t.Errorf("roster row links to a non-existent detail page")
	}
}

func TestServePageServices_emptyState(t *testing.T) {
	fake := newFakeDS()
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/services", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "No services registered.") {
		t.Errorf("empty state missing the no-services notice")
	}
	// Negative space: no fabricated service name leaks into the page.
	if strings.Contains(body, "trigger-svc") {
		t.Errorf("empty page rendered a fabricated service row")
	}
}

// TestServePageServices_navActiveAndBadge asserts the Services nav link
// is marked active on its own page and carries the badge placeholder the
// client fills from /console/api/nav-counts.
func TestServePageServices_navActiveAndBadge(t *testing.T) {
	fake := newFakeDS()
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/services", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `data-nav-count="services"`) {
		t.Errorf("desktop nav missing services badge placeholder")
	}
	// The active class attaches to the services link on its own page.
	idx := strings.Index(body, `href="/console/services"`)
	if idx < 0 {
		t.Fatalf("nav missing services link")
	}
	if !strings.Contains(body[idx:idx+200], "is-active") {
		t.Errorf("services nav link not marked is-active on its own page")
	}
}

// TestServePageServices_noDetailRoute guards the honesty decision that no
// per-service detail drill-in exists: a trailing-path request must 404
// rather than route to a fabricated endpoints view.
func TestServePageServices_noDetailRoute(t *testing.T) {
	fake := newFakeDS()
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(
		http.MethodGet, "/console/services/trigger-svc", nil,
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("detail path status: got %d, want 404", rec.Code)
	}
	// Positive: the roster route itself is reachable (200).
	rosterReq := httptest.NewRequest(http.MethodGet, "/console/services", nil)
	rosterRec := httptest.NewRecorder()
	handler.ServeHTTP(rosterRec, rosterReq)
	if rosterRec.Code != http.StatusOK {
		t.Fatalf("roster path status: got %d, want 200", rosterRec.Code)
	}
}

// TestNavCountsIncludesServices asserts the nav-counts endpoint now
// reports a real services count from the same KV read, and still omits
// the key when the read errors (no fabricated 0 badge).
func TestNavCountsIncludesServices(t *testing.T) {
	fake := newFakeDS()
	fake.services = []worker.ServiceDef{{Name: "a"}, {Name: "b"}}
	counts := navCountsBody(t, fake)
	if got, ok := counts["services"]; !ok || got != 2 {
		t.Errorf("nav-counts[services] = %d ok=%v, want 2 true", got, ok)
	}

	errFake := newFakeDS()
	errFake.serviceRowsErr = errNotFound("services", "kv")
	errCounts := navCountsBody(t, errFake)
	if _, ok := errCounts["services"]; ok {
		t.Errorf("services must be omitted when ListServiceRows errors")
	}
}

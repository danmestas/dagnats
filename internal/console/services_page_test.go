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
		// Real column headers. Service/Registered/Note from KV;
		// Version/Instances/Status from $SRV discovery (asserted below).
		">Service<", ">Registered<", ">Note<",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// Version / Instances / Status are now LIVE columns backed by $SRV
	// discovery (#449 Phase 2a). The fake projects KV rows only (no NATS
	// responder), so they render the honest dash here.
	for _, want := range []string{">Version<", ">Instances<", ">Status<"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing live column header %q", want)
		}
	}
	// Negative space: columns with no honest backing must NOT be rendered
	// as headers — rendering them would fabricate data. LastSeen carries
	// no $SRV last-activity timestamp; Kind/Commit are unbacked.
	for _, absent := range []string{
		">Kind<", ">Commit<", ">Last seen<",
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

// TestMergeDiscovery_nilMapAllDash asserts that when discovery
// transport-failed entirely (nil map) every live column degrades to the
// honest dash — we cannot distinguish online from stale, so we claim
// neither.
func TestMergeDiscovery_nilMapAllDash(t *testing.T) {
	rows := []ServiceRow{{Name: "svc-a", Registered: "x", Note: "n"}}
	out := mergeDiscovery(rows, nil)
	if len(out) != 1 {
		t.Fatalf("rows len: got %d, want 1", len(out))
	}
	if out[0].Version != dash || out[0].Instances != dash {
		t.Errorf("nil discovery: Version=%q Instances=%q, want both dash",
			out[0].Version, out[0].Instances)
	}
	if out[0].Status != dash {
		t.Errorf("nil discovery Status: got %q, want dash", out[0].Status)
	}
}

// TestMergeDiscovery_unionSynthesizesDiscoveryOnly asserts a service seen
// only via $SRV (not in the KV roster) gets a synthesized row — this is
// what makes dagnats-api (which does not self-register in KV) appear.
func TestMergeDiscovery_unionSynthesizesDiscoveryOnly(t *testing.T) {
	rows := []ServiceRow{{Name: "rostered", Registered: "x"}}
	d := map[string]serviceDiscovery{
		"dagnats-api": {
			Name: "dagnats-api", Version: "0.9.0", Instances: 1,
			HadStats: true, NumErrors: 0,
		},
	}
	out := mergeDiscovery(rows, d)
	if len(out) != 2 {
		t.Fatalf("union rows: got %d, want 2", len(out))
	}
	var synth *ServiceRow
	for i := range out {
		if out[i].Name == "dagnats-api" {
			synth = &out[i]
		}
	}
	if synth == nil {
		t.Fatalf("discovery-only service not synthesized: %+v", out)
	}
	if synth.Status != "online" || synth.Version != "0.9.0" {
		t.Errorf("synth row: Status=%q Version=%q, want online/0.9.0",
			synth.Status, synth.Version)
	}
	if !strings.Contains(synth.Note, "discovered") {
		t.Errorf("synth Note: got %q, want a discovered-via-$SRV note",
			synth.Note)
	}
}

// TestMergeDiscovery_statusMapping asserts the honest Status mapping:
// STATS+0 errors -> online; STATS+errors -> degraded; PING-but-no-STATS
// -> unknown (neutral, never online); rostered-but-no-PING -> stale.
func TestMergeDiscovery_statusMapping(t *testing.T) {
	rows := []ServiceRow{
		{Name: "online-svc"},
		{Name: "degraded-svc"},
		{Name: "unknown-svc"},
		{Name: "stale-svc"},
	}
	d := map[string]serviceDiscovery{
		"online-svc": {
			Name: "online-svc", Instances: 1, HadStats: true, NumErrors: 0,
		},
		"degraded-svc": {
			Name: "degraded-svc", Instances: 1, HadStats: true, NumErrors: 1,
		},
		"unknown-svc": {
			Name: "unknown-svc", Instances: 1, HadStats: false,
		},
	}
	out := mergeDiscovery(rows, d)
	byName := map[string]ServiceRow{}
	for _, r := range out {
		byName[r.Name] = r
	}
	if byName["online-svc"].Status != "online" {
		t.Errorf("STATS+0 errors: got %q, want online", byName["online-svc"].Status)
	}
	if byName["degraded-svc"].Status != "degraded" {
		t.Errorf("errors>0: got %q, want degraded", byName["degraded-svc"].Status)
	}
	if byName["unknown-svc"].Status != "unknown" {
		t.Errorf("no STATS: got %q, want unknown (never online)",
			byName["unknown-svc"].Status)
	}
	if byName["stale-svc"].Status != "stale" {
		t.Errorf("rostered no PING: got %q, want stale", byName["stale-svc"].Status)
	}
	// Meaningful negative: a degraded (PING+STATS) service must not be
	// misclassified as stale (which means no PING responder at all).
	if byName["degraded-svc"].Status == "stale" {
		t.Errorf("degraded service misclassified as stale")
	}
}

// TestMergeDiscovery_statsWithoutPingNotOnline guards the honesty bug
// where a STATS reply with no confirmed PING responder (Instances == 0)
// would otherwise be claimed "online" with zero confirmed liveness. The
// row must NOT be online and its Instances must be dashed.
func TestMergeDiscovery_statsWithoutPingNotOnline(t *testing.T) {
	rows := []ServiceRow{{Name: "ghost"}}
	d := map[string]serviceDiscovery{
		// HadStats but Instances == 0: STATS arrived without a PING.
		"ghost": {Name: "ghost", Version: "1.0.0", Instances: 0, HadStats: true},
	}
	out := mergeDiscovery(rows, d)
	if len(out) != 1 {
		t.Fatalf("rows len: got %d, want 1", len(out))
	}
	if out[0].Status == "online" {
		t.Errorf("zero confirmed instances falsely claimed online")
	}
	if out[0].Instances != dash {
		t.Errorf("Instances: got %q, want dash (no confirmed responder)",
			out[0].Instances)
	}
}

// TestTotalInstances_sumsNumericCells exercises the numeric-sum path:
// dashed cells contribute nothing, numeric cells sum.
func TestTotalInstances_sumsNumericCells(t *testing.T) {
	rows := []ServiceRow{
		{Name: "a", Instances: "1"},
		{Name: "b", Instances: "2"},
		{Name: "c", Instances: dash},
	}
	if got := totalInstances(rows); got != 3 {
		t.Errorf("totalInstances: got %d, want 3", got)
	}
	// Negative space: an all-dashed roster sums to zero, not a guess.
	allDash := []ServiceRow{{Name: "x", Instances: dash}}
	if got := totalInstances(allDash); got != 0 {
		t.Errorf("all-dash totalInstances: got %d, want 0", got)
	}
}

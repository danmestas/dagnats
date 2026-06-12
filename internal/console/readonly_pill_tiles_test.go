// readonly_pill_tiles_test.go covers two remediation slices:
//
//   - T5 (partial): a persistent read-only STATUS pill in the shared
//     top-bar chrome. It is a signifier of the deployment mode, not an
//     interactive toggle — the test asserts it renders when ReadOnly is
//     true and is ABSENT when false. There is no client control that
//     flips the mode (a fake toggle would be a Norman false affordance
//     and a security hole), so the test also asserts no posture-toggle
//     form/button ships alongside the pill.
//
//   - T7 (data-available only): the Functions (task-types) page gains a
//     WORKERS tile counting the DISTINCT workers backing the registered
//     functions. The source is TaskTypeRow.OwnerWorkerIDs, which the
//     page already fetches — no new data dependency. The negative-space
//     assertion: a tile is NOT added for FAIL RATE 24h, whose data the
//     page does not carry.
//
// Methodology: each test mounts its own console.Mount via the shared
// fake DataSource; nothing is shared. Pill assertions read the rendered
// layout chrome; tile assertions read both the pure header builder and
// the rendered Functions page so a regression in either layer surfaces.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestReadOnlyPill_rendersWhenReadOnly asserts the persistent read-only
// status pill ships in the top-bar chrome when ReadOnly is true.
func TestReadOnlyPill_rendersWhenReadOnly(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "console-mode-pill") {
		t.Errorf("expected read-only mode pill in chrome when ReadOnly=true")
	}
	if !strings.Contains(body, "Read-only") {
		t.Errorf("expected pill to label the mode as Read-only")
	}
}

// TestReadOnlyPill_absentWhenReadWrite is the negative space: when the
// deployment is read-write the amber pill must NOT render. A muted
// "Read-write" affordance is acceptable, but the alarm-toned pill class
// must never appear in read-write mode.
func TestReadOnlyPill_absentWhenReadWrite(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, "console-mode-pill") {
		t.Errorf("read-only mode pill must NOT render when ReadOnly=false")
	}
}

// TestReadOnlyPill_isNotAToggle guards the Norman honesty rule: the
// pill is a signifier, not a control. No posture-toggle form, no
// "Destructive" toggle, and no clickable mode switch may ship.
func TestReadOnlyPill_isNotAToggle(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	body := rr.Body.String()
	// A control gating nothing is a false affordance; the deferred
	// "Destructive" toggle must not appear.
	if strings.Contains(body, "Destructive") {
		t.Errorf("Destructive toggle must not ship — it gates actions that do not exist yet")
	}
	// The pill region must not be wrapped in an actionable element.
	for _, frag := range []string{
		`name="read_only"`, `id="read-only-toggle"`, `data-mode-toggle`,
	} {
		if strings.Contains(body, frag) {
			t.Errorf("read-only pill must not be an interactive toggle (found %q)", frag)
		}
	}
}

// TestFunctionsHeader_workersTile asserts the Functions page header
// gains a WORKERS tile counting the DISTINCT workers across every
// registered function. Two functions backed by two distinct workers
// (one shared) must yield WORKERS=2, not 3.
func TestFunctionsHeader_workersTile(t *testing.T) {
	rows := []TaskTypeRow{
		{TaskType: "billing::charge", OwnerWorkerIDs: []string{"w-1", "w-2"}},
		{TaskType: "email", OwnerWorkerIDs: []string{"w-2"}},
	}
	header := taskTypesHeader(len(rows), 1, distinctWorkerCount(rows))
	got := tileByLabel(header, "WORKERS")
	if got == nil {
		t.Fatalf("expected a WORKERS tile on the Functions header")
	}
	if got.Count != 2 {
		t.Errorf("WORKERS count = %d, want 2 (distinct w-1, w-2)", got.Count)
	}
	// Negative space: no FAIL RATE tile — the page carries no
	// per-function failure histogram, so a tile would lie.
	if tileByLabel(header, "FAIL RATE 24H") != nil {
		t.Errorf("FAIL RATE 24H tile must not ship — the data is not fetched")
	}
}

// TestDistinctWorkerCount covers the pure helper's de-duplication.
func TestDistinctWorkerCount(t *testing.T) {
	cases := []struct {
		name string
		rows []TaskTypeRow
		want int
	}{
		{"empty", nil, 0},
		{"single", []TaskTypeRow{{OwnerWorkerIDs: []string{"a"}}}, 1},
		{
			"dedup-across-rows",
			[]TaskTypeRow{
				{OwnerWorkerIDs: []string{"a", "b"}},
				{OwnerWorkerIDs: []string{"b", "c"}},
			},
			3,
		},
		{
			"dedup-within-row",
			[]TaskTypeRow{{OwnerWorkerIDs: []string{"a", "a"}}},
			1,
		},
	}
	for _, tc := range cases {
		if got := distinctWorkerCount(tc.rows); got != tc.want {
			t.Errorf("%s: distinctWorkerCount = %d, want %d",
				tc.name, got, tc.want)
		}
	}
}

// TestFunctionsPage_rendersWorkersTile drives the rendered page so a
// regression in the view-builder (not just the pure header) surfaces.
func TestFunctionsPage_rendersWorkersTile(t *testing.T) {
	fake := newFakeDS()
	fake.taskTypeRows = []TaskTypeRow{
		{TaskType: "billing::charge", Service: "billing",
			OwnerWorkerIDs: []string{"w-1", "w-2"}},
		{TaskType: "email", OwnerWorkerIDs: []string{"w-2"}},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/functions", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "WORKERS") {
		t.Errorf("expected WORKERS tile label on rendered Functions page")
	}
}

// tileByLabel returns the first tile with the given label, or nil.
func tileByLabel(h PageHeader, label string) *Tile {
	for i := range h.Tiles {
		if h.Tiles[i].Label == label {
			return &h.Tiles[i]
		}
	}
	return nil
}

// runs_id_filter_test.go exercises the find-run-by-id affordance after
// it changed from a dead-end detail redirect into a substring filter
// over the runs LIST.
//
// Methodology:
//   - Pure handler tests against fakeDataSource (no NATS).
//   - Each subtest seeds its own fake; tests never share state.
//   - Positive + negative space: the matching RunID is present AND a
//     non-matching one is absent in the rendered table.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

// idFilterRun returns a terminal run snapshot with a fixed RunID so
// the substring filter has something stable to match against.
func idFilterRun(id string) dag.WorkflowRun {
	return dag.WorkflowRun{
		RunID:      id,
		WorkflowID: "wf",
		Status:     dag.RunStatusCompleted,
	}
}

// TestRunsList_idSubstringFilter narrows the runs table to runs whose
// RunID contains the (case-insensitive) ?id= substring. The matching
// run renders; a non-matching run does not. This replaces the old
// exact-match-redirect behaviour: a partial id now FILTERS the list
// rather than dead-ending at a "No run snapshot found" detail page.
func TestRunsList_idSubstringFilter(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		idFilterRun("abc12345deadbeef"),
		idFilterRun("zzz99999cafef00d"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs?id=abc12345", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "abc12345deadbeef") {
		t.Errorf("expected matching run id in filtered list")
	}
	if strings.Contains(body, "zzz99999cafef00d") {
		t.Errorf("did not expect non-matching run id in filtered list")
	}
}

// TestRunsList_idSubstringFilterCaseInsensitive matches regardless of
// case so an operator pasting an upper-cased fragment still narrows.
func TestRunsList_idSubstringFilterCaseInsensitive(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		idFilterRun("abc12345deadbeef"),
		idFilterRun("zzz99999cafef00d"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs?id=ABC12345", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "abc12345deadbeef") {
		t.Errorf("expected case-insensitive match in filtered list")
	}
	if strings.Contains(body, "zzz99999cafef00d") {
		t.Errorf("did not expect non-matching run id in filtered list")
	}
}

// TestRunsList_emptyIdUnfiltered keeps the full list when ?id= is empty
// (the noop path) — every seeded run is still present.
func TestRunsList_emptyIdUnfiltered(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		idFilterRun("abc12345deadbeef"),
		idFilterRun("zzz99999cafef00d"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs?id=", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "abc12345deadbeef") {
		t.Errorf("empty id should keep first run in unfiltered list")
	}
	if !strings.Contains(body, "zzz99999cafef00d") {
		t.Errorf("empty id should keep second run in unfiltered list")
	}
}

// TestRunIDLookup_redirectsToFilter confirms the legacy lookup route
// now redirects to the runs list with the ?id= substring filter rather
// than dead-ending at /console/runs/<id>. The behaviour intentionally
// changed: a partial id must NARROW the table, not navigate to a detail
// page that 404s for non-exact ids.
func TestRunIDLookup_redirectsToFilter(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs/lookup?id=abc12345", nil))
	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/console/runs?id=abc12345" {
		t.Errorf("Location = %q, want /console/runs?id=abc12345", loc)
	}
}

// TestRunIDLookup_emptyRedirectsToList sends an empty input back to the
// unfiltered runs list (the noop path).
func TestRunIDLookup_emptyRedirectsToList(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs/lookup?id=", nil))
	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/console/runs" {
		t.Errorf("Location = %q, want /console/runs", loc)
	}
}

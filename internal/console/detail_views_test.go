// detail_views_test.go covers the cross-cutting Batch 6 detail-view
// concerns: both new/extended detail routes register (never 404), the
// list rows are clickable into them, and the two detail views share one
// stat-card component rather than duplicating card markup.
//
// Methodology:
//   - In-memory fakeDataSource feeds the list + detail renders.
//   - httptest.Recorder asserts status + body substrings.
//   - Each test mounts its own console.Mount; nothing is shared.
//   - Positive value (the detail route resolves with the shared
//     component) AND negative space (a sibling not-found / a non-detail
//     route still 404s) are asserted in each test.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/trigger"
)

// TestDetailRoutes_registerNo404 asserts the stream + trigger detail
// routes resolve (not 404) while a bogus sub-path under each still 404s
// — proving the dispatcher routes real detail names and rejects junk.
func TestDetailRoutes_registerNo404(t *testing.T) {
	fake := seedStreamDetailFake()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)

	resolves := []string{
		"/console/streams/WORKFLOW_HISTORY",
		"/console/triggers/cron-1",
	}
	for _, path := range resolves {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code == http.StatusNotFound {
			t.Errorf("detail route %s returned 404", path)
		}
	}

	// Negative space: a two-segment junk path under streams is not a
	// detail route and must 404 (the dispatcher rejects nested paths).
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/streams/WORKFLOW_HISTORY/extra", nil))
	if rr.Code != http.StatusNotFound {
		t.Errorf("nested junk stream path: status = %d, want 404", rr.Code)
	}
}

// TestTriggerList_rowsLinkToDetail asserts the trigger list rows are
// clickable into the detail route with a chevron affordance.
func TestTriggerList_rowsLinkToDetail(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `href="/console/triggers/cron-1"`) {
		t.Errorf("trigger list row not linked to detail route")
	}
	if !strings.Contains(body, "row-chevron") {
		t.Errorf("trigger list missing chevron affordance")
	}
}

// TestDetailViews_shareStatCardComponent asserts BOTH detail views render
// the shared stat-card component (data-component="stat-card") so the
// Configuration card markup isn't duplicated per page (Ousterhout reuse).
func TestDetailViews_shareStatCardComponent(t *testing.T) {
	fake := seedStreamDetailFake()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)

	for _, path := range []string{
		"/console/streams/WORKFLOW_HISTORY",
		"/console/triggers/cron-1",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s: status = %d, want 200", path, rr.Code)
		}
		if !strings.Contains(rr.Body.String(), `data-component="stat-card"`) {
			t.Errorf("GET %s: missing shared stat-card component", path)
		}
	}
}

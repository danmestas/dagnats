// worker_detail_test.go covers the read-only worker detail view:
// GET /console/workers/{id} renders an Identity stat-card and a
// "Registered functions" table sourced from the same ListWorkers read
// that backs the workers list page — no second directory round-trip,
// no fabricated data.
//
// Methodology:
//   - In-memory fakeDataSource feeds configSnap.Workers (the same seam
//     the list page reads); httptest.Recorder asserts status + body.
//   - Each test mounts its own console.Mount via mountWithFake; nothing
//     is shared.
//   - Positive value: a known worker renders its real registration
//     fields (id, host, task types, language/transport/max-tasks) and
//     its derived liveness status. Negative space: an unknown id renders
//     an honest not-found state (still 200 with chrome, never a
//     fabricated identity card), the empty path 404s, and the page never
//     renders the mockup's unbacked features (counter tiles, in-flight
//     tasks table, Drain/Resume/Decommission actions).
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// seedWorkerDetailFake returns a fake with one fresh-heartbeat worker
// carrying a full registration so the detail page's projection is
// observable.
func seedWorkerDetailFake() *fakeDataSource {
	fake := newFakeDS()
	fake.configSnap.Workers = []worker.WorkerRegistration{
		{
			WorkerID:  "worker-alpha",
			TaskTypes: []string{"email", "render"},
			Language:  "go",
			Transport: "nats",
			MaxTasks:  8,
			Pid:       4242,
			Hostname:  "host-01",
			Version:   "1.2.3",
			LastSeen:  time.Now(),
		},
	}
	return fake
}

// TestWorkerDetail_rendersIdentity asserts a known fresh worker renders
// its real registration fields and the active status.
func TestWorkerDetail_rendersIdentity(t *testing.T) {
	fake := seedWorkerDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers/worker-alpha", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"worker-alpha", "host-01", "email, render",
		"go", "nats", "8", "4242", "1.2.3",
		"active", "Registered functions",
		`data-component="page-header"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("worker detail missing %q", want)
		}
	}
	// Negative space: no honest-dash for fields that have real data.
	if strings.Contains(body, ">—<") {
		t.Errorf("worker detail rendered a dash for a populated field: %s",
			truncBody(body))
	}
}

// TestWorkerDetail_staleStatus asserts a worker whose LastSeen is older
// than MaxWorkerStaleness renders the stale status, not active.
func TestWorkerDetail_staleStatus(t *testing.T) {
	fake := newFakeDS()
	// The worker id deliberately carries no "stale" substring so the body
	// assertion below discriminates the Status value rather than incidental
	// chrome (the id is rendered verbatim in the header and Identity card).
	fake.configSnap.Workers = []worker.WorkerRegistration{
		{
			WorkerID:  "w-02",
			TaskTypes: []string{"email"},
			Language:  "go",
			Transport: "nats",
			MaxTasks:  1,
			Hostname:  "host-02",
			LastSeen:  time.Now().Add(-2 * worker.MaxWorkerStaleness),
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers/w-02", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "stale") {
		t.Errorf("stale worker missing stale status: %s", truncBody(body))
	}
}

// TestWorkerDetail_unknownIdNotFound asserts an unregistered id renders
// the honest not-found state (200 with chrome) and never a fabricated
// identity card.
func TestWorkerDetail_unknownIdNotFound(t *testing.T) {
	fake := seedWorkerDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers/does-not-exist", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "No worker named") {
		t.Errorf("unknown worker missing honest not-found state: %s",
			truncBody(body))
	}
	// Must not fabricate an identity card for a worker that doesn't exist.
	if strings.Contains(body, "Registered functions") {
		t.Errorf("unknown worker rendered a functions table")
	}
}

// TestWorkerDetail_emptyPathDispatch asserts the bare trailing-slash path
// (empty id) routes to the not-found page rather than the detail view.
func TestWorkerDetail_emptyPathDispatch(t *testing.T) {
	fake := seedWorkerDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers/", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("empty id status = %d, want 404", rr.Code)
	}
}

// TestWorkerDetail_omitsUnbackedFeatures guards the honesty contract: the
// mockup's counter tiles, in-flight tasks table, and lifecycle actions
// have no backing telemetry or mutation, so they must not render.
func TestWorkerDetail_omitsUnbackedFeatures(t *testing.T) {
	fake := seedWorkerDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers/worker-alpha", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, banned := range []string{
		"Drain", "Resume", "Decommission",
		"dedup hits", "redelivered", "processed",
		"In-flight tasks", "AckWait remaining",
	} {
		if strings.Contains(body, banned) {
			t.Errorf("worker detail leaked unbacked feature %q", banned)
		}
	}
}

// TestWorkersList_rowLinksToDetail asserts the list rows are clickable
// and carry a chevron affordance linking to the per-worker detail route.
func TestWorkersList_rowLinksToDetail(t *testing.T) {
	fake := seedWorkerDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `href="/console/workers/worker-alpha"`) {
		t.Errorf("workers list row not linked to detail route")
	}
	if !strings.Contains(body, "row-chevron") {
		t.Errorf("workers list missing chevron affordance")
	}
}

// TestWorkerDetailFromRegistrations drives the pure helper with a fixed
// now so liveness classification is deterministic. Positive: a matched
// fresh worker projects every field. Negative: an absent id returns
// Found:false and a zero Pid/MaxTasks dashes to empty.
func TestWorkerDetailFromRegistrations(t *testing.T) {
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	regs := []worker.WorkerRegistration{
		{
			WorkerID:  "w1",
			TaskTypes: []string{"a", "b"},
			Language:  "python",
			Transport: "nats",
			MaxTasks:  0, // unreported -> should dash via positiveIntOrEmpty
			Pid:       0,
			Hostname:  "h1",
			LastSeen:  now.Add(-1 * time.Second),
		},
	}
	got := workerDetailFromRegistrations(regs, "w1", now)
	if !got.Found {
		t.Fatalf("expected Found for w1")
	}
	if got.Status != "active" {
		t.Errorf("status = %q, want active", got.Status)
	}
	if got.TaskTypes != "a, b" {
		t.Errorf("task types = %q, want %q", got.TaskTypes, "a, b")
	}
	if len(got.Functions) != 2 || got.Functions[0].Function != "a" {
		t.Errorf("functions = %+v, want two rows starting with a", got.Functions)
	}
	if got.MaxTasks != "" || got.Pid != "" {
		t.Errorf("zero MaxTasks/Pid must be empty (dashed by view), got %q/%q",
			got.MaxTasks, got.Pid)
	}
	// Negative space: an absent id is not found.
	miss := workerDetailFromRegistrations(regs, "nope", now)
	if miss.Found {
		t.Errorf("expected Found=false for unknown id")
	}
	// Liveness discrimination on the Status field itself (not incidental
	// chrome text): a LastSeen older than MaxWorkerStaleness classifies
	// stale, a fresh one classifies active. This is the genuine coverage
	// for the stale branch in projectWorkerDetail.
	staleRegs := []worker.WorkerRegistration{
		{
			WorkerID: "w2",
			Language: "go",
			LastSeen: now.Add(-2 * worker.MaxWorkerStaleness),
		},
	}
	stale := workerDetailFromRegistrations(staleRegs, "w2", now)
	if stale.Status != "stale" {
		t.Errorf("stale status = %q, want stale", stale.Status)
	}
	freshRegs := []worker.WorkerRegistration{
		{
			WorkerID: "w3",
			Language: "go",
			LastSeen: now.Add(-1 * time.Second),
		},
	}
	fresh := workerDetailFromRegistrations(freshRegs, "w3", now)
	if fresh.Status != "active" {
		t.Errorf("fresh status = %q, want active", fresh.Status)
	}
}

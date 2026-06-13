// ops_pages_test.go covers the operator pages after the Ops hub was
// dissolved (B3 nav/IA): the retired /console/ops landing now
// 301-redirects, while /console/workers (placeholder), /console/kv
// inspector, and /console/streams (placeholder) carry the content.
// Each test asserts both a positive substring AND a boundary condition
// so the page can't drift silently.
//
// Methodology:
//   - In-memory fakeDataSource feeds page renders.
//   - httptest.Recorder asserts status + body substrings.
//   - Each test creates its own console.Mount; nothing is shared.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// TestOpsHubDissolved_redirectsToDashboard asserts the retired Ops
// landing 301-redirects to the dashboard (the B3 nav/IA pass removed
// the hub and promoted its children to top level).
func TestOpsHubDissolved_redirectsToDashboard(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops", nil))
	if rr.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want 301", rr.Code)
	}
	if got := rr.Header().Get("Location"); got != "/console/" {
		t.Fatalf("Location = %q, want /console/", got)
	}
}

// TestWorkersList_emptyStateNoWorkers asserts that with zero workers
// registered the page paints an honest empty state and no longer
// carries the retired "not yet wired" / wrong-bucket callout.
func TestWorkersList_emptyStateNoWorkers(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// Positive: honest empty state.
	if !strings.Contains(body, "No workers currently registered") {
		t.Fatalf("missing honest empty state: %s", body)
	}
	// Negative space: the retired stub copy + wrong bucket name are gone.
	if strings.Contains(body, "not yet wired") {
		t.Fatalf("workers page still carries retired not-wired callout")
	}
	if strings.Contains(body, "worker_heartbeats") {
		t.Fatalf("workers page still names the wrong KV bucket")
	}
	if !strings.Contains(body, `data-component="page-header"`) {
		t.Fatalf("workers page not using page-header partial: %s", body)
	}
}

// TestWorkersList_rendersRealWorkers feeds two worker registrations
// through the fake's ListWorkers seam and asserts the real id / task
// types / host / last-seen / status reach the page — and that no stub
// callout remains.
func TestWorkersList_rendersRealWorkers(t *testing.T) {
	fake := newFakeDS()
	now := time.Now()
	fake.configSnap.Workers = []worker.WorkerRegistration{
		{
			WorkerID:  "wk-alpha",
			TaskTypes: []string{"email", "render"},
			Hostname:  "host-1",
			LastSeen:  now,
		},
		{
			WorkerID:  "wk-stale",
			TaskTypes: []string{"crunch"},
			Hostname:  "host-2",
			LastSeen:  now.Add(-5 * time.Minute),
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// Positive: both worker ids + real metadata reach the table.
	for _, want := range []string{
		"wk-alpha", "wk-stale", "host-1", "host-2", "email", "render",
		"active", "stale",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in workers page", want)
		}
	}
	// Negative space: no stub callout, no fake empty state.
	if strings.Contains(body, "not yet wired") {
		t.Fatalf("workers page still carries retired not-wired callout")
	}
	if strings.Contains(body, "No workers currently registered") {
		t.Fatalf("workers page shows empty state despite real workers")
	}
}

// TestLeases_routeRemoved is the honesty assertion: the Leases surface
// had no engine feed and no mockup counterpart, so it was removed.
// /console/leases must now 404 (the route is gone) and the redirect of
// the legacy /console/ops/leases must land on /console/concurrency, the
// real admission-backed surface that owns lock / slot / rate-limit
// telemetry.
func TestLeases_routeRemoved(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/leases", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("GET /console/leases: status = %d, want 404", rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/leases", nil))
	if got := rr.Header().Get("Location"); got != "/console/concurrency" {
		t.Fatalf("ops/leases redirect Location = %q, want /console/concurrency",
			got)
	}
}

func TestKVList_renderBucketAndKeyList(t *testing.T) {
	fake := newFakeDS()
	fake.kvBuckets = []KVBucketInfo{
		{Name: "triggers", Description: "triggers", Keys: 2},
	}
	fake.kvKeys["triggers"] = []string{"cron-1", "hook-1"}
	fake.kvEntries["triggers/cron-1"] = []byte(`{"id":"cron-1"}`)
	h := mountWithFake(t, fake)
	// Without selection — defaults to first bucket.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/kv", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"KV inspector", "triggers", "cron-1", "hook-1",
		"Pick a key on the left",
		`data-component="page-header"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in KV inspector body", want)
		}
	}
	if strings.Contains(body, "← Ops") {
		t.Fatalf("KV page still carries back-to-Ops link")
	}
	// Internal bucket / key hrefs must point at the promoted path.
	if !strings.Contains(body, `href="/console/kv?bucket=triggers"`) {
		t.Errorf("missing bucket href under /console/kv: %s", body)
	}
}

func TestKVList_selectKeyRendersValuePane(t *testing.T) {
	fake := newFakeDS()
	fake.kvBuckets = []KVBucketInfo{
		{Name: "triggers", Description: "triggers", Keys: 1},
	}
	fake.kvKeys["triggers"] = []string{"cron-1"}
	fake.kvEntries["triggers/cron-1"] = []byte(`{"id":"cron-1","x":2}`)
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/kv?bucket=triggers&key=cron-1", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "kv-entry-json") {
		t.Fatalf("missing JSON value class: %s", body)
	}
	// Template escapes quotes — assert on the escaped form.
	if !strings.Contains(body, "&#34;x&#34;") &&
		!strings.Contains(body, "&quot;x&quot;") {
		t.Fatalf("missing JSON key content: %s", body)
	}
	if !strings.Contains(body, "rev 1") {
		t.Fatalf("missing revision label: %s", body)
	}
}

func TestKVList_missingKeyRendersNotFound(t *testing.T) {
	fake := newFakeDS()
	fake.kvBuckets = []KVBucketInfo{{Name: "triggers", Keys: 0}}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/kv?bucket=triggers&key=missing", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Key not found") {
		t.Fatalf("missing not-found message: %s", body)
	}
}

// TestStreamsList_rendersRealSnapshot feeds a ConfigSnapshot with live
// stream metadata and asserts the real Messages / Bytes / Consumers
// reach the page — and that the retired "not yet wired" callout is gone.
func TestStreamsList_rendersRealSnapshot(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap.Streams = []StreamSnapshot{
		{
			Name: "WORKFLOW_HISTORY", Subjects: []string{"history.>"},
			Messages: 1234, Bytes: 2048, Consumers: 3,
			Retention: "limits", Provisioned: true,
		},
		{Name: "TASK_QUEUES", Provisioned: false},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/streams", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// Positive: real stream names + counts reach the table. Bytes are
	// humanized (2048 → "2.0 KiB"); messages + consumers render raw.
	for _, want := range []string{
		"Streams", "WORKFLOW_HISTORY", "TASK_QUEUES", "history.&gt;",
		"1234", "2.0 KiB", `data-component="page-header"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in streams page", want)
		}
	}
	// Negative space: the retired stub callout is gone.
	if strings.Contains(body, "not yet wired") {
		t.Fatalf("streams page still carries retired not-wired callout")
	}
}

func TestOpsWorkersRedirect_308ToPromoted(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/workers", nil))
	if rr.Code != http.StatusPermanentRedirect {
		t.Fatalf("status = %d, want 308", rr.Code)
	}
	if got := rr.Header().Get("Location"); got != "/console/workers" {
		t.Fatalf("Location = %q, want /console/workers", got)
	}
}

func TestOpsKVRedirect_308PreservesQuery(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/kv?bucket=triggers&key=cron-1", nil))
	if rr.Code != http.StatusPermanentRedirect {
		t.Fatalf("status = %d, want 308", rr.Code)
	}
	want := "/console/kv?bucket=triggers&key=cron-1"
	if got := rr.Header().Get("Location"); got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
}

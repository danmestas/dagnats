// nav_counts_test.go covers the Batch-9 nav-badge count endpoint
// (/console/api/nav-counts) and the badge placeholders in the layout
// nav. The nav renders on every page, so the counts are fetched once
// by the client after load via this JSON endpoint rather than scanned
// synchronously on every page render.
//
// Methodology:
//   - Pure-handler tests against newFakeDS() seeded with per-source
//     fixture data, exercising the JSON endpoint end-to-end without NATS.
//   - Positive space: the Verdict-A nav items (Workflows, Functions,
//     Workers, Triggers, Runs, DLQ, Connections, Streams, Consumers, KV)
//     report their real counts.
//   - Negative space: Traces carries NO count (no data/route) and an
//     errored/unavailable source omits its key rather than reporting a
//     fabricated 0. (Services is now a real route + count.)
//   - Min 2 assertions per test.
package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
)

// navCountsBody fetches /console/api/nav-counts and decodes the map.
func navCountsBody(t *testing.T, fake *fakeDataSource) map[string]int {
	t.Helper()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/api/nav-counts", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("nav-counts status = %d, want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("nav-counts content-type = %q, want application/json", ct)
	}
	var out map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode nav-counts: %v", err)
	}
	return out
}

// TestNavCountsReportsRealAItemCounts seeds every A-item source and
// asserts the JSON endpoint reports each real count. Positive: the
// seeded counts come back. Negative: Services and Traces keys never
// appear (no data/route yet — out of scope, would be a fake item).
func TestNavCountsReportsRealAItemCounts(t *testing.T) {
	fake := newFakeDS()
	now := time.Now()
	fake.workflows = []dag.WorkflowDef{
		sampleWorkflow("a"), sampleWorkflow("b"),
	}
	fake.runs = []dag.WorkflowRun{
		{RunID: "r1", WorkflowID: "a", Status: dag.RunStatusRunning, CreatedAt: now},
		{RunID: "r2", WorkflowID: "a", Status: dag.RunStatusCompleted, CreatedAt: now},
		{RunID: "r3", WorkflowID: "b", Status: dag.RunStatusFailed, CreatedAt: now},
	}
	fake.triggers = []trigger.TriggerDef{
		{ID: "t1"}, {ID: "t2"}, {ID: "t3"},
	}
	fake.deadLetters = make([]api.DeadLetterView, 2)
	fake.consumers = make([]ConsumerRow, 4)
	fake.connections = make([]ConnRow, 5)
	fake.kvBuckets = make([]KVBucketInfo, 6)
	fake.configSnap = ConfigSnapshot{
		Streams: make([]StreamSnapshot, 8),
		Workers: []worker.WorkerRegistration{
			{WorkerID: "w1", TaskTypes: []string{"email", "image"}},
			{WorkerID: "w2", TaskTypes: []string{"email"}},
		},
	}
	counts := navCountsBody(t, fake)

	want := map[string]int{
		"workflows":   2,
		"runs":        3,
		"triggers":    3,
		"dlq":         2,
		"consumers":   4,
		"connections": 5,
		"kv":          6,
		"streams":     8,
		"workers":     2,
		"functions":   2, // distinct task types: email, image
	}
	for key, n := range want {
		got, ok := counts[key]
		if !ok {
			t.Errorf("nav-counts missing key %q", key)
			continue
		}
		if got != n {
			t.Errorf("nav-counts[%q] = %d, want %d", key, got, n)
		}
	}
	// Services now has a route + data; with none seeded its count is 0
	// (present-but-zero, not omitted — the read succeeded on an empty
	// bucket). Detailed services coverage lives in services_page_test.go.
	if _, ok := counts["traces"]; ok {
		t.Errorf("nav-counts must NOT include traces (no data/route)")
	}
}

// TestNavCountsOmitsUnavailableSource asserts that a source returning
// an error is omitted from the payload entirely — never reported as a
// fabricated 0. Positive: a working source (workflows) is present.
// Negative: the errored source (config snapshot → streams) is absent.
func TestNavCountsOmitsUnavailableSource(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("a")}
	fake.configSnapErr = errNotFound("config", "snapshot")
	counts := navCountsBody(t, fake)

	if got, ok := counts["workflows"]; !ok || got != 1 {
		t.Errorf("nav-counts[workflows] = %d ok=%v, want 1 true", got, ok)
	}
	if _, ok := counts["streams"]; ok {
		t.Errorf("streams must be omitted when ConfigSnapshot errors")
	}
}

// TestNavBadgePlaceholdersInLayout asserts the layout nav carries a
// badge placeholder span (with a stable data-nav-count key) for each
// A-item so the client script can fill it, and carries NO such span on
// Services / Traces. Positive: a workflows badge placeholder exists.
// Negative: no services/traces badge placeholder.
func TestNavBadgePlaceholdersInLayout(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, key := range []string{
		"workflows", "functions", "workers", "triggers", "runs",
		"dlq", "connections", "streams", "consumers", "kv",
	} {
		if !strings.Contains(body, `data-nav-count="`+key+`"`) {
			t.Errorf("layout missing nav badge placeholder for %q", key)
		}
	}
	// Services is now a real route + data source, so it carries a badge
	// placeholder like the other System-group items (verified in
	// services_page_test.go). Traces remains absent (no data/route yet).
	if strings.Contains(body, `data-nav-count="traces"`) {
		t.Errorf("layout must not carry a traces nav badge")
	}
}

// TestNavCountsAssetServed asserts the client fetch script is served
// and references the endpoint + the placeholder attribute.
func TestNavCountsAssetServed(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/console/assets/nav-counts.js", nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("nav-counts.js status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "/console/api/nav-counts") {
		t.Errorf("nav-counts.js missing the endpoint URL")
	}
	if !strings.Contains(body, "data-nav-count") {
		t.Errorf("nav-counts.js missing the placeholder attribute selector")
	}
}

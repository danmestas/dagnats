// ops_pages_test.go covers the operator pages after the Ops hub was
// dissolved (B3 nav/IA): the retired /console/ops landing now
// 301-redirects, while /console/workers (placeholder), /console/leases
// (placeholder), /console/kv inspector, and /console/streams
// (placeholder) carry the content. Each test asserts both a positive
// substring AND a boundary condition so the page can't drift silently.
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

func TestWorkersList_rendersPlaceholderBanner(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/workers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Worker telemetry is not yet wired") {
		t.Fatalf("missing telemetry-gap callout: %s", body)
	}
	if !strings.Contains(body, "no workers reporting") {
		t.Fatalf("missing zero-row label: %s", body)
	}
	// Workers must use the shared page_header tile partial post-#311.
	if !strings.Contains(body, `data-component="page-header"`) {
		t.Fatalf("workers page not using page-header partial: %s", body)
	}
	// The old "back to Ops" link must be gone — Workers is top-level.
	if strings.Contains(body, "← Ops") {
		t.Fatalf("workers page still carries back-to-Ops link")
	}
}

func TestOpsLeases_rendersPlaceholderBanner(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/leases", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Lease telemetry is not yet wired") {
		t.Fatalf("missing telemetry-gap callout: %s", body)
	}
	if !strings.Contains(body, "no leases reporting") {
		t.Fatalf("missing zero-row label: %s", body)
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

func TestStreamsList_rendersKnownEngineStreams(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/streams", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"Streams",
		"Stream metadata is not yet wired",
		"TASKS",
		"STICKY_TASKS",
		"TELEMETRY",
		"TRIGGER_HISTORY",
		"HISTORY",
		`data-component="page-header"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in streams page", want)
		}
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

// ops_pages_test.go covers the /console/ops surfaces PR 5b adds:
// index, workers list (placeholder), leases list (placeholder), KV
// inspector. Each test asserts both a positive substring AND a
// boundary condition so the page can't drift silently.
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

func TestOpsIndex_rendersFourTiles(t *testing.T) {
	fake := newFakeDS()
	fake.kvBuckets = []KVBucketInfo{
		{Name: "triggers", Description: "triggers"},
		{Name: "workflow_runs", Description: "runs"},
	}
	fake.auditEvents = []AuditEvent{{
		Actor: "operator", Action: "dlq.retry",
		Target: "1", Outcome: "success",
	}}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"Workers", "Leases", "KV inspector", "Audit log",
		"engine telemetry pending",
		`href="/console/ops/workers"`,
		`href="/console/ops/kv"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in ops index", want)
		}
	}
}

func TestOpsWorkers_rendersPlaceholderBanner(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/workers", nil))
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
}

func TestOpsLeases_rendersPlaceholderBanner(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/leases", nil))
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

func TestOpsKV_renderBucketAndKeyList(t *testing.T) {
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
		"/console/ops/kv", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"KV inspector", "triggers", "cron-1", "hook-1",
		"Pick a key on the left",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in KV inspector body", want)
		}
	}
}

func TestOpsKV_selectKeyRendersValuePane(t *testing.T) {
	fake := newFakeDS()
	fake.kvBuckets = []KVBucketInfo{
		{Name: "triggers", Description: "triggers", Keys: 1},
	}
	fake.kvKeys["triggers"] = []string{"cron-1"}
	fake.kvEntries["triggers/cron-1"] = []byte(`{"id":"cron-1","x":2}`)
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/kv?bucket=triggers&key=cron-1", nil))
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

func TestOpsKV_missingKeyRendersNotFound(t *testing.T) {
	fake := newFakeDS()
	fake.kvBuckets = []KVBucketInfo{{Name: "triggers", Keys: 0}}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/kv?bucket=triggers&key=missing", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Key not found") {
		t.Fatalf("missing not-found message: %s", body)
	}
}

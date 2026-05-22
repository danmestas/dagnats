// configuration_page_test.go exercises /console/config (#312, R3).
//
// Methodology:
//   - In-memory fakeDataSource feeds the page; tests own the
//     ConfigSnapshot they assert against. No NATS or JetStream.
//   - httptest.Recorder asserts status + body substrings.
//   - Each test creates its own console.Mount; nothing is shared.
//   - Positive AND negative assertions per the project rule:
//     at least one structural substring + one boundary check
//     (counts, ordering, or empty-state copy) so the page can't
//     drift silently.
package console

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
)

// TestConfigurationPage_RendersAllSections is the spec-compliance
// check: every one of the five sections + the build footer must show
// up in the DOM with its identifying substring. Adding a section to
// the page means adding a row here.
func TestConfigurationPage_RendersAllSections(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{
		sampleWorkflow("alpha"), sampleWorkflow("beta"),
	}
	fake.triggers = []trigger.TriggerDef{{
		ID: "cron-1", WorkflowID: "alpha", Enabled: true,
		Cron: &trigger.CronConfig{Expression: "*/5 * * * *"},
	}}
	fake.deadLetters = []api.DeadLetterView{{
		DeadLetter: api.DeadLetter{
			Sequence: 7, Subject: "dead.task.alpha.first",
			Timestamp: time.Now(),
		},
	}}
	fake.configSnap = ConfigSnapshot{
		NATSURL:           "nats://127.0.0.1:4222",
		NATSServerVersion: "2.12.0",
		OTLPEndpoint:      "http://localhost:4318",
		Streams: []StreamSnapshot{
			{Name: "WORKFLOW_HISTORY", Messages: 42, Bytes: 1024,
				Retention: "limits", Provisioned: true},
			{Name: "TASK_QUEUES", Provisioned: false},
		},
		KVBuckets: []KVBucketInfo{
			{Name: "workflow_runs", Description: "runs", Keys: 12},
		},
		Workers: []worker.WorkerRegistration{
			{WorkerID: "w1", TaskTypes: []string{"echo"},
				LastSeen: time.Now().Add(-30 * time.Second)},
			{WorkerID: "w2", TaskTypes: []string{"echo"},
				LastSeen: time.Now().Add(-10 * time.Second)},
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Positive substrings — one per section.
	wants := []string{
		// Page header / nav active state.
		`<title>Config`,
		`data-page="config"`,
		// Section 1: counts strip (workflow / trigger / worker etc).
		`page-header-tile`,
		`WORKFLOWS`, `TRIGGERS`, `WORKERS`,
		`STREAMS`, `KV BUCKETS`, `DLQ`,
		// Section 2: endpoints panel.
		`config-endpoints`,
		`nats://127.0.0.1:4222`,
		`http://localhost:4318`,
		// Section 3: streams + KV.
		`config-jetstream`,
		`WORKFLOW_HISTORY`,
		`workflow_runs`,
		// Section 4: worker pools.
		`config-worker-pools`,
		`echo`,
		// Section 5: trigger types.
		`config-trigger-types`,
		`cron`, `webhook`, `subject`, `http`,
		// Footer.
		`config-build-footer`,
		`2.12.0`,
		// YAML modal.
		`config-yaml-modal`,
		`View deployment YAML`,
	}
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("config page missing %q", w)
		}
	}

	// Boundary: counts strip must reflect the seeded inventory.
	// 2 workflows; 1 trigger; 2 workers; 2 streams; 1 KV bucket;
	// 1 DLQ entry. Scope the search to the page header so the
	// nav-link "DLQ" doesn't accidentally satisfy the assertion.
	pageHeaderMarker := `data-component="page-header"`
	phIdx := strings.Index(body, pageHeaderMarker)
	if phIdx < 0 {
		t.Fatalf("page header not rendered")
	}
	// Each tile's label is in <span class="page-header-tile-label">.
	// Find each tile-label and assert the count span just before it.
	phEnd := strings.Index(body[phIdx:], "</header>")
	if phEnd < 0 {
		t.Fatalf("page header has no closing </header>")
	}
	phRegion := body[phIdx : phIdx+phEnd]
	for _, pair := range []struct {
		label string
		count string
	}{
		{"WORKFLOWS", `<span class="page-header-tile-count">2</span>`},
		{"TRIGGERS", `<span class="page-header-tile-count">1</span>`},
		{"WORKERS", `<span class="page-header-tile-count">2</span>`},
		{"STREAMS", `<span class="page-header-tile-count">2</span>`},
		{"KV BUCKETS", `<span class="page-header-tile-count">1</span>`},
		{"DLQ", `<span class="page-header-tile-count">1</span>`},
	} {
		labelIdx := strings.Index(phRegion, pair.label)
		if labelIdx < 0 {
			t.Errorf("tile label %q not in page header", pair.label)
			continue
		}
		// Look back 200 bytes for this tile's count span.
		start := labelIdx - 200
		if start < 0 {
			start = 0
		}
		window := phRegion[start:labelIdx]
		if !strings.Contains(window, pair.count) {
			t.Errorf("tile %s: expected %q before label, "+
				"got window %q", pair.label, pair.count, window)
		}
	}
}

// TestConfigurationPage_BuildFooter asserts that the build footer
// line carries each of the five required fragments: dagnats build,
// NATS server version, Go runtime version. The build line is the
// "is this the right deployment?" answer and the audit explicitly
// called it out — easy to lose if a future refactor consolidates
// the footer markup.
func TestConfigurationPage_BuildFooter(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		NATSServerVersion: "2.12.6",
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Pull the footer out of the body so substring searches don't
	// match incidental occurrences elsewhere on the page.
	startMark := `class="config-build-footer"`
	startIdx := strings.Index(body, startMark)
	if startIdx < 0 {
		t.Fatalf("footer not rendered: %s", body)
	}
	endIdx := strings.Index(body[startIdx:], "</footer>")
	if endIdx < 0 {
		t.Fatalf("footer end tag missing")
	}
	footer := body[startIdx : startIdx+endIdx]

	wants := []string{
		// cfg.Build is "test" from mountWithFake.
		`dagnats test`,
		// NATS server version comes from the snapshot.
		`NATS server 2.12.6`,
		// Go version comes from runtime.Version().
		runtime.Version(),
	}
	for _, w := range wants {
		if !strings.Contains(footer, w) {
			t.Errorf("footer missing %q (was: %s)", w, footer)
		}
	}

	// Boundary: empty NATS server version must render the em-dash
	// placeholder, not literally "NATS server " with trailing space
	// (which would lie about reachability).
	fake.configSnap = ConfigSnapshot{NATSServerVersion: ""}
	h2 := mountWithFake(t, fake)
	rr2 := httptest.NewRecorder()
	h2.ServeHTTP(rr2, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))
	body2 := rr2.Body.String()
	startIdx2 := strings.Index(body2, startMark)
	endIdx2 := strings.Index(body2[startIdx2:], "</footer>")
	footer2 := body2[startIdx2 : startIdx2+endIdx2]
	if !strings.Contains(footer2, `NATS server —`) {
		t.Errorf("empty NATS version did not render em-dash: %s",
			footer2)
	}
}

// TestConfigurationPage_YAMLModal asserts the YAML export modal
// renders with the deployment shape. The YAML block must:
//  1. open via a <details>/<summary> primitive (works without JS),
//  2. carry every top-level key (build / endpoints / streams /
//     kv_buckets / worker_groups / trigger_types).
//  3. round-trip the seeded values into the YAML body (sample one
//     value per section so the test fails when the renderer drops
//     a section).
func TestConfigurationPage_YAMLModal(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		NATSURL:           "nats://10.0.0.1:4222",
		NATSServerVersion: "2.12.0",
		OTLPEndpoint:      "http://otel:4318",
		Streams: []StreamSnapshot{
			{Name: "EVENTS", Messages: 99, Bytes: 4096,
				Retention: "interest", Provisioned: true},
		},
		KVBuckets: []KVBucketInfo{
			{Name: "triggers", Keys: 3},
		},
		Workers: []worker.WorkerRegistration{
			{WorkerID: "w1", TaskTypes: []string{"render"},
				LastSeen: time.Now()},
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Modal structure.
	for _, w := range []string{
		`<details class="config-yaml-modal"`,
		`<summary class="config-yaml-toggle"`,
		`View deployment YAML`,
		`config-yaml-body`,
	} {
		if !strings.Contains(body, w) {
			t.Errorf("modal markup missing %q", w)
		}
	}

	// Pull the <code>...</code> YAML block out so substring matches
	// don't drift into the surrounding chrome.
	codeStart := strings.Index(body, `<code>`)
	codeEnd := strings.Index(body, `</code>`)
	if codeStart < 0 || codeEnd < 0 || codeEnd <= codeStart {
		t.Fatalf("YAML code block not found")
	}
	yaml := body[codeStart+len(`<code>`) : codeEnd]

	// Every top-level key must be present.
	for _, key := range []string{
		"build:", "endpoints:", "streams:",
		"kv_buckets:", "worker_groups:", "trigger_types:",
	} {
		if !strings.Contains(yaml, key) {
			t.Errorf("YAML missing top-level key %q (yaml=%q)",
				key, yaml)
		}
	}

	// Sample one value per section to confirm the seed values
	// reached the renderer. Note that html/template HTML-escapes
	// the body (the YAML is rendered inside <code>...</code>) — a
	// literal `"` becomes `&#34;`. Assert against the escaped form
	// because that's what reaches the browser.
	for _, w := range []string{
		`dagnats: test`,
		`nats: &#34;nats://10.0.0.1:4222&#34;`,
		`otlp: &#34;http://otel:4318&#34;`,
		`- name: EVENTS`,
		`messages: 99`,
		`retention: interest`,
		`- name: triggers`,
		`keys: 3`,
		`- name: render`,
		`- cron`,
		`- subject`,
		`- webhook`,
		`- http`,
	} {
		if !strings.Contains(yaml, w) {
			t.Errorf("YAML missing value %q (yaml=%q)", w, yaml)
		}
	}
}

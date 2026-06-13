// configuration_invariants_test.go exercises the Config page's
// engine-invariants table (#312 follow-on, ADR-015 R3). The table is
// static reference documentation of the engine's compile-time constants
// (consumer AckWait, KV TTLs, stream dedup windows) — not runtime data.
// These tests pin the honest values against the source-of-truth constants
// in internal/natsutil/conn.go and worker/, and guard the one row the
// mockup would have fabricated (a bare AckWait=30s) by requiring the
// AckWait rows to be scoped to the consumer they actually govern.
//
// Methodology:
//   - Pure handler test against fakeDataSource (no NATS).
//   - The table is static, so a single mount suffices.
//   - Positive assertions sample real constants from conn.go; the
//     negative-space assertion rejects an unscoped AckWait row.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func invariantsRegion(t *testing.T, body string) string {
	t.Helper()
	const marker = `class="config-invariants"`
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("engine-invariants section not rendered")
	}
	rest := body[start:]
	end := strings.Index(rest, "</section>")
	if end < 0 {
		t.Fatalf("engine-invariants section has no closing </section>")
	}
	return rest[:end]
}

func TestConfigPage_EngineInvariantsTable(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	region := invariantsRegion(t, rr.Body.String())

	// Section chrome + column headers.
	for _, want := range []string{
		"Engine invariants", "Constant", "Value", "Governs", "Source",
		"Compile-time constants, not config.",
	} {
		if !strings.Contains(region, want) {
			t.Errorf("invariants section missing %q", want)
		}
	}

	// Representative real values, each verified against conn.go / worker.
	for _, want := range []string{
		"MaxDeliver", "-1 (unlimited)", // worker/worker.go:577
		"5s",             // WORKFLOW_HISTORY + TELEMETRY dedup
		"24h",            // DEAD_LETTERS dedup / idempotency_keys TTL
		"60s",            // workers KV TTL
		"120s",           // worker_status KV TTL
		"168h (7d)",      // approval_tokens TTL
		"14d",            // debounce_state TTL
		"7 days / 1 GiB", // TELEMETRY retention
		"25h",            // sticky_bindings TTL
	} {
		if !strings.Contains(region, want) {
			t.Errorf("invariants table missing value %q", want)
		}
	}

	// Both AckWait rows must be present, each SCOPED to the consumer it
	// governs. The mockup listed a single bare "AckWait = 30s"; rendering
	// that verbatim would fabricate the worker task consumer's value
	// (really 5m, worker/consumer_naming.go:14). conn.go:53-54 confirms
	// 30s is the WORKFLOW_HISTORY consumer's default.
	if !strings.Contains(region, "AckWait (WORKFLOW_HISTORY consumer)") {
		t.Errorf("missing scoped WORKFLOW_HISTORY AckWait row")
	}
	if !strings.Contains(region, "AckWait (worker task consumer)") {
		t.Errorf("missing scoped worker-task AckWait row")
	}
	if !strings.Contains(region, "5m") {
		t.Errorf("worker task consumer AckWait (5m) not rendered")
	}

	// Negative space: no bare unscoped "AckWait" constant cell. Every
	// AckWait constant cell must carry a parenthetical scope.
	if strings.Contains(region, `<td class="mono">AckWait</td>`) {
		t.Errorf("unscoped AckWait row present — fabricates the value")
	}

	// Every row's source pill must read the honest "hardcoded".
	if strings.Contains(region, "Source") &&
		!strings.Contains(region, "hardcoded") {
		t.Errorf("invariants source pills missing 'hardcoded'")
	}
}

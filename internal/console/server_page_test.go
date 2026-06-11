// server_page_test.go exercises the /console/server page and the pure
// storePct helper without standing up NATS.
//
// Methodology:
//   - The page test reuses the fakeDataSource + mountWithFake helpers
//     from pages_test.go. Seeding fake.serverHealth drives the render so
//     the identity / account layout gets coverage without a live
//     nats.Conn. Assertions look for stable substrings the template
//     emits (positive space) and confirm cumulative API errors are shown
//     but never decorated with the danger class (negative space) — a
//     non-zero error tally since boot is normal, not an alarm.
//   - storePct is a pure function over (used, max); its test asserts the
//     rounding and the div-by-zero guard directly.
//   - Each page test creates its own console.Mount with the fake; tests
//     never share state.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServePageServer_rendersHealth(t *testing.T) {
	fake := newFakeDS()
	fake.serverHealth = ServerHealth{
		ServerName:    "dagnats-dev",
		ServerVersion: "2.12.6",
		NATSURL:       "nats://127.0.0.1:4222",
		StoreUsed:     2_000_000,
		StoreMax:      10 << 30,
		StorePct:      0,
		Streams:       5,
		StreamsMax:    -1,
		Consumers:     6,
		ConsumersMax:  -1,
		APITotal:      1234,
		APIErrors:     0,
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/server", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"dagnats-dev", "2.12.6", "nats://127.0.0.1:4222", ">5<", ">6<",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// Negative space: a server name we never seeded must not appear.
	if strings.Contains(body, "phantom-server") {
		t.Errorf("body unexpectedly contains a fabricated server name")
	}
}

func TestServePageServer_apiErrorsNotAlarmed(t *testing.T) {
	// Cumulative API errors since boot are normal (startup not-found
	// probes), so the page shows the tally but never decorates it with
	// the danger class — a snapshot count is not a health alarm.
	fake := newFakeDS()
	fake.serverHealth = ServerHealth{
		ServerName: "dagnats-dev", ServerVersion: "2.12.6",
		NATSURL: "nats://127.0.0.1:4222", APITotal: 100, APIErrors: 7,
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/server", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "7 errors") {
		t.Errorf("body missing the cumulative API error tally")
	}
	if strings.Contains(body, "status-failed") {
		t.Errorf("cumulative API errors must not apply the danger class")
	}
}

func TestServerHealthPct(t *testing.T) {
	// 2GiB of 10GiB rounds (integer-truncates) to 20%.
	if got := storePct(2<<30, 10<<30); got != 20 {
		t.Errorf("storePct(2GiB, 10GiB): got %d, want 20", got)
	}
	// Div-by-zero guard: a non-positive max yields 0, never a panic.
	if got := storePct(2<<30, 0); got != 0 {
		t.Errorf("storePct(_, 0): got %d, want 0", got)
	}
	if got := storePct(2<<30, -1); got != 0 {
		t.Errorf("storePct(_, -1 unlimited): got %d, want 0", got)
	}
}

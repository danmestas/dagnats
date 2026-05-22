package console

// Methodology:
//   - Mount() registers the /__fixtures__/ route only when
//     fixturesEnabled() returns true. We exercise that gate end-to-end
//     by booting Mount() against a fake DataSource with different
//     env-var combinations and probing /console/__fixtures__/tabs.
//   - Two positive/negative assertions per test:
//       prodGuard: 404 even with DAGNATS_FIXTURES=true (guard fires),
//                  and the body is not the fixture HTML.
//       devServes: 200 with DAGNATS_FIXTURES=true and no DAGNATS_ENV
//                  =production, and the body contains the fixture tag.
//   - Bounded: a single HTTP round-trip per case against an in-process
//     httptest.Server; no network, no goroutines, no timers.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestFixturesGate_productionRefuses asserts the explicit production
// guard fires even when DAGNATS_FIXTURES=true. A single misconfigured
// flag must not expose the fixture surface in prod.
func TestFixturesGate_productionRefuses(t *testing.T) {
	t.Setenv("DAGNATS_ENV", "production")
	t.Setenv("DAGNATS_FIXTURES", "true")

	fake := newFakeDS()
	srv := httptest.NewServer(mountWithFake(t, fake))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/console/__fixtures__/tabs")
	if err != nil {
		t.Fatalf("GET fixture: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("prod guard: status = %d, want 404", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	// Negative assertion: response must not contain the fixture
	// skeleton even by accident. If this fires, the gate leaked.
	if strings.Contains(string(body), `data-fixture="tabs"`) {
		t.Fatalf("prod guard: response leaked fixture body: %q", body)
	}
}

// TestFixturesGate_developmentServes asserts the dev path still works:
// DAGNATS_ENV=development + DAGNATS_FIXTURES=true mounts the route and
// serves the per-component skeleton. Mirrors the env shape that the
// browser smoke test relies on.
func TestFixturesGate_developmentServes(t *testing.T) {
	t.Setenv("DAGNATS_ENV", "development")
	t.Setenv("DAGNATS_FIXTURES", "true")

	fake := newFakeDS()
	srv := httptest.NewServer(mountWithFake(t, fake))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/console/__fixtures__/tabs")
	if err != nil {
		t.Fatalf("GET fixture: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dev path: status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	// Positive assertion: the tabs skeleton was rendered.
	if !strings.Contains(string(body), `data-fixture="tabs"`) {
		t.Fatalf("dev path: response missing fixture body: %q", body)
	}
}

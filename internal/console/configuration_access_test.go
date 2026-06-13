// configuration_access_test.go exercises the Config page's access-posture
// card (#312 follow-on, ADR-015 R3). The card surfaces the auth mode the
// console resolved at startup and the read-only flag — both read straight
// off the Config the handler already holds, so these tests assert that the
// rendered DOM reflects the mounted Config and nothing is fabricated.
//
// Methodology:
//   - Pure handler tests against fakeDataSource (no NATS).
//   - Each subtest mounts its own console; tests never share state.
//   - AuthBasic gates every request, so the BasicMode test supplies
//     credentials and asserts 200 BEFORE asserting card substrings —
//     otherwise the body would be the 401 page, not the card.
//   - Assertions look for stable substrings scoped to the card region
//     (data-card="access-posture") so a label colliding elsewhere on the
//     page can't satisfy the check.
//   - Each test carries a negative-space assertion (exactly-one-active
//     pill, the opposite read-only state) alongside the positive one.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// accessCardRegion returns the slice of body between the access-posture
// card marker and its closing region, so substring assertions don't match
// elsewhere on the page. Fails the test if the card is absent.
func accessCardRegion(t *testing.T, body string) string {
	t.Helper()
	const marker = `data-card="access-posture"`
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("access-posture card not rendered")
	}
	rest := body[start:]
	end := strings.Index(rest, "</section>")
	if end < 0 {
		t.Fatalf("access-posture card has no closing </section>")
	}
	return rest[:end]
}

func TestConfigPage_AccessPostureCard_BasicMode(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeAuth(t, fake, AuthBasic)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/console/config", nil)
	req.SetBasicAuth("console", "pw")
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (auth must not block the page)",
			rr.Code)
	}
	region := accessCardRegion(t, rr.Body.String())

	for _, label := range []string{
		"Access posture", "loopback", "forward-auth", "basic-auth",
		"disabled",
	} {
		if !strings.Contains(region, label) {
			t.Errorf("access card missing %q", label)
		}
	}
	// The active basic-auth pill must carry the on-class; exactly one
	// pill is active across the strip (negative space: not zero, not two).
	if !strings.Contains(region,
		`<span class="config-modepill is-active">basic-auth</span>`) {
		t.Errorf("basic-auth pill not marked active")
	}
	if got := strings.Count(region, "config-modepill is-active"); got != 1 {
		t.Errorf("active pill count = %d, want exactly 1", got)
	}
	// The quotes in the actor note are HTML-escaped (&#34;) in the
	// rendered DOM; assert against the escaped form.
	if !strings.Contains(region, "actor is &#34;console&#34;") {
		t.Errorf("access card missing basic-auth actor note")
	}
}

func TestConfigPage_AccessPostureCard_Loopback(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake) // defaults to AuthLoopback
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	region := accessCardRegion(t, rr.Body.String())

	if !strings.Contains(region,
		`<span class="config-modepill is-active">loopback</span>`) {
		t.Errorf("loopback pill not marked active under AuthLoopback")
	}
	// Negative space: the basic-auth pill must NOT be active here.
	if strings.Contains(region,
		`<span class="config-modepill is-active">basic-auth</span>`) {
		t.Errorf("basic-auth pill wrongly active under AuthLoopback")
	}
}

func TestConfigPage_ReadOnlyPill(t *testing.T) {
	// read-only ON → warn pill reads "on" + the env var name.
	fakeOn := newFakeDS()
	h := mountWithFakeRO(t, fakeOn, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	region := accessCardRegion(t, rr.Body.String())
	if !strings.Contains(region,
		`<span class="config-pill tile-tone-warning">on</span>`) {
		t.Errorf("read-only ON pill missing/incorrect")
	}
	if !strings.Contains(region, "CONSOLE_READ_ONLY") {
		t.Errorf("read-only env var name missing")
	}
	// Negative space: must not render the off pill when read-only is on.
	if strings.Contains(region,
		`<span class="config-pill tile-tone-success">off</span>`) {
		t.Errorf("off pill rendered while read-only is on")
	}

	// read-only OFF → success pill reads "off".
	fakeOff := newFakeDS()
	h2 := mountWithFakeRO(t, fakeOff, false)
	rr2 := httptest.NewRecorder()
	h2.ServeHTTP(rr2, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))
	if rr2.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr2.Code)
	}
	region2 := accessCardRegion(t, rr2.Body.String())
	if !strings.Contains(region2,
		`<span class="config-pill tile-tone-success">off</span>`) {
		t.Errorf("read-only OFF pill missing/incorrect")
	}
	if strings.Contains(region2,
		`<span class="config-pill tile-tone-warning">on</span>`) {
		t.Errorf("on pill rendered while read-only is off")
	}
}

func TestConfigPage_AuditLinkChip(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/config", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	region := accessCardRegion(t, rr.Body.String())
	// The audit chip links a real, navigable route — no dead affordance.
	if !strings.Contains(region, `href="/console/audit"`) {
		t.Errorf("audit link chip missing /console/audit href")
	}
}

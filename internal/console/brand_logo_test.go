// brand_logo_test.go pins the console brand wordmark: the green dot
// (.console-mark) is gone, and the header renders "dagnats://" set in
// the IoskeleyMono data face with "nats" accented and "://" as a muted
// scheme tail. A collapsed-rail glyph stands in when the name hides.
//
// Methodology:
//   - In-memory fakeDataSource feeds the layout via console.Mount.
//   - httptest.Recorder renders /console/ and asserts on the brand
//     markup (positive: the new spans; negative: the retired dot).
//   - Own Mount; nothing shared.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestBrandLogo_protocolWordmark asserts the header carries the new
// dagnats:// wordmark parts and no longer carries the green dot.
func TestBrandLogo_protocolWordmark(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /console/: status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Positive space: the wordmark renders as dag + accented nats +
	// the muted scheme tail, plus a collapsed-rail glyph.
	wantWordmark := `<span class="console-name">dag` +
		`<span class="console-name-nats">nats</span>` +
		`<span class="console-name-scheme">://</span></span>`
	for _, want := range []string{
		wantWordmark,
		`class="console-glyph"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("brand markup missing %q", want)
		}
	}

	// Negative space: the green dot is retired entirely.
	if strings.Contains(body, `class="console-mark"`) {
		t.Errorf("brand still renders the green dot (.console-mark)")
	}
}

// TestBrandLogo_styledByMonoFace asserts the served stylesheet sets the
// brand name in the mono face and colors the wordmark parts — the dot's
// fixed-size pill rule is gone.
func TestBrandLogo_styledByMonoFace(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET app.css: status = %d, want 200", rr.Code)
	}
	css := rr.Body.String()

	for _, want := range []string{
		".console-name-nats { color: var(--accent); }",
		".console-name-scheme { color: var(--text-secondary); }",
		".console-glyph {",
	} {
		if !strings.Contains(css, want) {
			t.Errorf("app.css missing brand rule %q", want)
		}
	}
	// The brand name must opt into the mono face, not the display face.
	idx := strings.Index(css, ".console-brand .console-name {")
	if idx < 0 {
		t.Fatalf("app.css missing .console-brand .console-name rule")
	}
	block := css[idx:min(idx+200, len(css))]
	if !strings.Contains(block, "var(--font-mono)") {
		t.Errorf("brand name not set in the mono face: %q", block)
	}
}

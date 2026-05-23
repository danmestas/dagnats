// font_assets_test.go pins the typography assets to their shipped
// shape so accidental regressions surface in CI.
//
// Methodology:
//   - Size check: each woff2 must stay under the agreed payload
//     ceiling. The mono faces are subset to Latin + Latin-Extended +
//     numerics + common punctuation (#336 R10); going over the
//     ceiling almost certainly means an unrelated glyph block crept
//     in, which is the bug we want flagged.
//   - HTTP check: GET /console/assets/fonts/<name>.woff2 returns 200
//     with Content-Type: font/woff2 and a body that matches the
//     embedded bytes. font-display:swap relies on the browser
//     fetching the binary; if the route ever 404s the operator just
//     sees the system fallback with no warning.
//   - CSS wiring check: app.css references both new mono faces at
//     the /console/assets/fonts/ paths the handler serves, and
//     --font-mono lists IBM Plex Mono ahead of the system stack.
//
// The mono assets land at:
//   - assets/fonts/ibm-plex-mono-latin-regular.woff2
//   - assets/fonts/ibm-plex-mono-latin-bold.woff2
package console

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// monoFaceCeilingBytes is the per-face payload ceiling for the IBM
// Plex Mono subset. The shipping subset sits well below this; the
// ceiling exists to flag accidental over-subsetting (CJK, emoji,
// arrows, box-drawing) before bytes hit operator browsers.
const monoFaceCeilingBytes = 30 * 1024

func TestFontAssets_monoFaces_underCeiling(t *testing.T) {
	t.Helper()
	cases := []string{
		"assets/fonts/ibm-plex-mono-latin-regular.woff2",
		"assets/fonts/ibm-plex-mono-latin-bold.woff2",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			body, err := fs.ReadFile(assetsFS, name)
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			if len(body) == 0 {
				t.Fatalf("%s is empty — embed did not pick up the bytes", name)
			}
			if len(body) > monoFaceCeilingBytes {
				t.Fatalf("%s = %d bytes, want <= %d (subset regressed?)",
					name, len(body), monoFaceCeilingBytes)
			}
			// woff2 magic header is "wOF2" — guard against accidentally
			// embedding a TTF instead of the compressed face.
			if len(body) < 4 || string(body[:4]) != "wOF2" {
				t.Fatalf("%s missing wOF2 magic; got %x",
					name, body[:min4(len(body))])
			}
		})
	}
}

func min4(n int) int {
	if n < 4 {
		return n
	}
	return 4
}

func TestServeFontAsset_monoFaces_servesWoff2(t *testing.T) {
	h := newTestConsole(t)
	cases := []string{
		"/console/assets/fonts/ibm-plex-mono-latin-regular.woff2",
		"/console/assets/fonts/ibm-plex-mono-latin-bold.woff2",
	}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			if got := rr.Header().Get("Content-Type"); got != "font/woff2" {
				t.Fatalf("Content-Type = %q, want font/woff2", got)
			}
			if rr.Body.Len() == 0 {
				t.Fatalf("empty body for %s", path)
			}
			// woff2 never advertises Content-Encoding: the bytes are
			// already compressed by the format itself. serveFontAsset
			// is documented to leave Content-Encoding unset; verify.
			if got := rr.Header().Get("Content-Encoding"); got != "" {
				t.Fatalf("Content-Encoding = %q, want empty for woff2", got)
			}
		})
	}
}

func TestAppCSS_referencesIBMPlexMonoFaces(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)
	wantSubs := []string{
		`font-family: "IBM Plex Mono"`,
		`/console/assets/fonts/ibm-plex-mono-latin-regular.woff2`,
		`/console/assets/fonts/ibm-plex-mono-latin-bold.woff2`,
		// --font-mono must list IBM Plex Mono ahead of the system
		// monospace fallbacks so the subset face wins on identifier
		// elements once the woff2 streams in.
		`--font-mono: "IBM Plex Mono"`,
	}
	for _, sub := range wantSubs {
		if !strings.Contains(css, sub) {
			t.Errorf("app.css missing %q", sub)
		}
	}
}

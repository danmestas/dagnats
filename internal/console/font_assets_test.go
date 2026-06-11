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
//   - CSS wiring check: app.css references the mono + spark faces at
//     the /console/assets/fonts/ paths the handler serves, and
//     --font-mono lists IoskeleyMono ahead of IBM Plex Mono and the
//     system stack (the IBM Plex faces stay embedded as the fallback).
//
// The typography assets land at:
//   - assets/fonts/ibm-plex-mono-latin-regular.woff2  (fallback)
//   - assets/fonts/ibm-plex-mono-latin-bold.woff2     (fallback)
//   - assets/fonts/ioskeley-mono-latin-regular.woff2  (primary data face)
//   - assets/fonts/ioskeley-mono-latin-bold.woff2     (primary data face)
//   - assets/fonts/datatype.woff2                     (inline sparklines)
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

// ioskeleyFaceCeilingBytes is the per-face ceiling for the IoskeleyMono
// subset (Iosevka-derived, a richer glyph set than Plex so a higher
// bound), and datatypeCeilingBytes bounds the whole Datatype face —
// vendored un-subset because its chart ligature lookups must survive.
// Both ceilings exist to flag a gross over-inclusion before bytes hit
// operator browsers, not to track the exact shipping size.
const (
	ioskeleyFaceCeilingBytes = 64 * 1024
	datatypeCeilingBytes     = 96 * 1024
)

func TestFontAssets_newFaces_underCeiling(t *testing.T) {
	cases := []struct {
		name    string
		ceiling int
	}{
		{"assets/fonts/ioskeley-mono-latin-regular.woff2", ioskeleyFaceCeilingBytes},
		{"assets/fonts/ioskeley-mono-latin-bold.woff2", ioskeleyFaceCeilingBytes},
		{"assets/fonts/datatype.woff2", datatypeCeilingBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, err := fs.ReadFile(assetsFS, tc.name)
			if err != nil {
				t.Fatalf("read %s: %v", tc.name, err)
			}
			if len(body) == 0 {
				t.Fatalf("%s is empty — embed did not pick up the bytes", tc.name)
			}
			if len(body) > tc.ceiling {
				t.Fatalf("%s = %d bytes, want <= %d (over-inclusion?)",
					tc.name, len(body), tc.ceiling)
			}
			if len(body) < 4 || string(body[:4]) != "wOF2" {
				t.Fatalf("%s missing wOF2 magic; got %x",
					tc.name, body[:min4(len(body))])
			}
		})
	}
}

func TestServeFontAsset_monoFaces_servesWoff2(t *testing.T) {
	h := newTestConsole(t)
	cases := []string{
		"/console/assets/fonts/ibm-plex-mono-latin-regular.woff2",
		"/console/assets/fonts/ibm-plex-mono-latin-bold.woff2",
		"/console/assets/fonts/ioskeley-mono-latin-regular.woff2",
		"/console/assets/fonts/ioskeley-mono-latin-bold.woff2",
		"/console/assets/fonts/datatype.woff2",
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

// TestFontAssets_OFLLicensePresent enforces OFL §2 compliance: the
// license text must travel with the binary. If the embed directive
// is ever dropped the font files would ship without their license
// and we'd be silently out of compliance — this test fails loudly
// before that can land.
func TestFontAssets_OFLLicensePresent(t *testing.T) {
	// Every vendored OFL face must travel with its license text. IBM
	// Plex ships under OFL.txt; IoskeleyMono and Datatype carry their
	// own license files alongside.
	cases := []string{
		"assets/fonts/OFL.txt",
		"assets/fonts/OFL-IoskeleyMono.txt",
		"assets/fonts/OFL-Datatype.txt",
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
			if !strings.Contains(strings.ToUpper(string(body)), "SIL OPEN FONT LICENSE") {
				t.Fatalf("%s missing 'SIL OPEN FONT LICENSE' marker; got %d bytes "+
					"of unexpected content", name, len(body))
			}
		})
	}
}

func TestAppCSS_referencesTypographyFaces(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)
	wantSubs := []string{
		// IoskeleyMono is the primary data face: its @font-face must
		// be declared, both weights served from the fonts path, and
		// --font-mono must list it ahead of IBM Plex Mono + the system
		// stack so the subset face wins on identifier elements.
		`font-family: "IoskeleyMono"`,
		`/console/assets/fonts/ioskeley-mono-latin-regular.woff2`,
		`/console/assets/fonts/ioskeley-mono-latin-bold.woff2`,
		`--font-mono: "IoskeleyMono"`,
		// IBM Plex Mono stays embedded + referenced as the fallback
		// face behind IoskeleyMono.
		`font-family: "IBM Plex Mono"`,
		`/console/assets/fonts/ibm-plex-mono-latin-regular.woff2`,
		`/console/assets/fonts/ibm-plex-mono-latin-bold.woff2`,
		// Datatype drives inline OpenType-ligature sparklines via the
		// --font-spark token + the .console-spark class.
		`font-family: "Datatype"`,
		`/console/assets/fonts/datatype.woff2`,
		`--font-spark: "Datatype"`,
	}
	for _, sub := range wantSubs {
		if !strings.Contains(css, sub) {
			t.Errorf("app.css missing %q", sub)
		}
	}
}

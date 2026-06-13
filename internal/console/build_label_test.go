// build_label_test.go pins the honest build-identity normalization the
// footer (R9, #320) and the configuration build line surface.
//
// Context: the build-info footer renders `dagnats {{.DagnatsBuild}}`
// verbatim from cfg.Build. The server stamps cfg.Build from the binary's
// ldflags revision (e.g. a `git describe` string that embeds the short
// commit). When a build is un-stamped, cfg.Build is empty — rendering it
// raw yields `dagnats ` with a trailing-space empty segment, which both
// looks broken and dishonestly implies an identity that isn't there. The
// honest contract: surface any real revision verbatim (it carries the
// commit), and degrade a blank build to the literal `dev` marker. We
// never fabricate a commit hash.
//
// Methodology:
//   - Pure table test over consoleBuildLabel covering blank, whitespace,
//     the literal "dev", and a real `git describe` revision (which the
//     footer must surface verbatim, commit and all).
//   - Rendered footer test (httptest + newFakeDS/Mount): a console built
//     with an empty Build must render `dagnats dev`, never `dagnats `
//     with a dangling segment.
//   - Negative space: the helper must NOT mangle a real revision — the
//     embedded commit substring survives verbatim.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"log/slog"
)

// TestConsoleBuildLabel covers the dev-fallback normalization. Only a
// blank/whitespace build maps to the "dev" marker; every other value is
// surfaced verbatim so a real revision (carrying the commit) is honest.
func TestConsoleBuildLabel(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty-falls-back-to-dev", "", "dev"},
		{"whitespace-falls-back-to-dev", "   ", "dev"},
		{"literal-dev-stays-dev", "dev", "dev"},
		{"describe-revision-verbatim",
			"v0.1.0-3-gabc1234", "v0.1.0-3-gabc1234"},
		{"bare-commit-verbatim", "abc1234", "abc1234"},
		{"clean-tag-verbatim", "v0.1.0", "v0.1.0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := consoleBuildLabel(tc.in); got != tc.want {
				t.Errorf("consoleBuildLabel(%q) = %q, want %q",
					tc.in, got, tc.want)
			}
		})
	}
}

// TestBuildInfoFooter_BlankBuildShowsDev asserts the rendered footer
// degrades a blank build identity to the honest "dev" marker rather than
// emitting a dangling empty `dagnats ` segment.
func TestBuildInfoFooter_BlankBuildShowsDev(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		NATSURL:      "nats://127.0.0.1:4222",
		NATSEmbedded: true,
	}
	h := Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "", // un-stamped local build
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     fake,
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	startIdx := strings.Index(body, `class="build-info-footer"`)
	if startIdx < 0 {
		t.Fatalf("footer not rendered")
	}
	endIdx := strings.Index(body[startIdx:], "</footer>")
	footer := body[startIdx : startIdx+endIdx]

	if !strings.Contains(footer, "dagnats dev") {
		t.Errorf("blank build must render honest \"dagnats dev\"; was: %s",
			footer)
	}
	// Negative space: no dangling empty identity segment.
	if strings.Contains(footer, "dagnats </span>") {
		t.Errorf("footer rendered empty build segment; was: %s", footer)
	}
}

// TestBuildInfoFooter_RevisionSurfacedVerbatim asserts a real stamped
// revision (carrying the commit) reaches the footer unmodified — the
// normalization must not strip or rewrite a genuine build identity.
func TestBuildInfoFooter_RevisionSurfacedVerbatim(t *testing.T) {
	const revision = "v0.1.0-3-g5824fe3"
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{NATSURL: "nats://127.0.0.1:4222"}
	h := Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    revision,
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     fake,
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	body := rr.Body.String()
	startIdx := strings.Index(body, `class="build-info-footer"`)
	if startIdx < 0 {
		t.Fatalf("footer not rendered")
	}
	footer := body[startIdx : startIdx+strings.Index(body[startIdx:], "</footer>")]
	if !strings.Contains(footer, "dagnats "+revision) {
		t.Errorf("revision must surface verbatim; was: %s", footer)
	}
}

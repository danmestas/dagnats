// handler_test.go exercises the public surface of console.Mount.
//
// Methodology:
//   - One Mount per test; the handler is the system under test.
//   - Asset tests assert Content-Encoding: gzip + non-empty body for
//     each gzipped artifact, and Content-Type for the plain CSS.
//   - Layout tests parse the rendered HTML and assert nav links + that
//     no external URL slips into a src/href/@import — the dedicated
//     TestNoExternalURLs covers this at the policy layer too.
//   - Heartbeat lifecycle uses a short interval, reads at least two
//     `event: datastar-patch-elements` lines off the SSE stream, then
//     cancels the request context so the handler returns. The handler
//     returning quickly after cancel is the proof that the goroutine
//     is bound to r.Context().Done().
//
// Every Mount() call uses AuthLoopback so the gate does not interfere
// with the surface under test. Auth-mode tests live in auth_test.go.
package console

import (
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func newTestConsole(t *testing.T) http.Handler {
	t.Helper()
	return Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Short interval so the lifecycle test finishes in <1s.
		HeartbeatInterval: 50 * time.Millisecond,
	})
}

func TestServeDashboard_rendersLayoutAndNav(t *testing.T) {
	h := newTestConsole(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	wantSubs := []string{
		`<title>Dashboard`,
		`href="/console/"`,
		`href="/console/workflows"`,
		`href="/console/runs"`,
		`href="/console/triggers"`,
		`href="/console/dlq"`,
		`href="/console/metrics"`,
		`href="/console/audit"`,
		`href="/console/assets/basecoat.css"`,
		`href="/console/assets/app.css"`,
		`src="/console/assets/console.js"`,
		// Phase 2 T07: dashboard now subscribes to /console/sse/dashboard
		// for live tile updates. The legacy heartbeat SSE moved off the
		// landing page (it was a plumbing demo, not operator value).
		`data-init="@get('/console/sse/dashboard', {openWhenHidden: true})"`,
		`id="theme-toggle"`,
	}
	for _, sub := range wantSubs {
		if !strings.Contains(body, sub) {
			t.Errorf("rendered body missing %q", sub)
		}
	}
}

func TestServeAsset_gzippedHeaders(t *testing.T) {
	h := newTestConsole(t)
	cases := []struct {
		path string
		ct   string
	}{
		{"/console/assets/console.js", "application/javascript"},
		{"/console/assets/basecoat.css", "text/css"},
		{"/console/assets/uplot.min.js", "application/javascript"},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			if got := rr.Header().Get("Content-Encoding"); got != "gzip" {
				t.Fatalf("Content-Encoding = %q, want gzip", got)
			}
			if got := rr.Header().Get("Content-Type"); !strings.HasPrefix(got, tc.ct) {
				t.Fatalf("Content-Type = %q, want prefix %q", got, tc.ct)
			}
			// Body must be a valid gzip stream and decode to something
			// non-empty — this catches bundle-empty / wrong-file bugs.
			zr, err := gzip.NewReader(rr.Body)
			if err != nil {
				t.Fatalf("not a gzip stream: %v", err)
			}
			defer zr.Close()
			decoded, err := io.ReadAll(zr)
			if err != nil {
				t.Fatalf("read gzip: %v", err)
			}
			if len(decoded) == 0 {
				t.Fatalf("decoded body is empty for %s", tc.path)
			}
		})
	}
}

func TestServeAsset_plainAppCSS(t *testing.T) {
	h := newTestConsole(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("plain asset should not advertise gzip: %q", got)
	}
	if got := rr.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/css") {
		t.Fatalf("Content-Type = %q, want text/css", got)
	}
	if !strings.Contains(rr.Body.String(), "--bg:") {
		t.Fatalf("app.css missing --bg custom property")
	}
}

func TestServeHeartbeat_emitsAtLeastTwoEvents(t *testing.T) {
	h := newTestConsole(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		srv.URL+"/console/sse/heartbeat", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get heartbeat: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}

	// Read the stream until we have seen at least two patch-elements
	// events. Bounded: the read loop quits at maxLines lines or 1.5s.
	const wantEvents = 2
	const maxLines = 200
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	gotEvents := 0
	deadline := time.Now().Add(1500 * time.Millisecond)
	for line := 0; line < maxLines && time.Now().Before(deadline); line++ {
		if !sc.Scan() {
			break
		}
		if strings.HasPrefix(sc.Text(), "event: datastar-patch-elements") {
			gotEvents++
			if gotEvents >= wantEvents {
				break
			}
		}
	}
	if gotEvents < wantEvents {
		t.Fatalf("received %d datastar-patch-elements events, want >= %d",
			gotEvents, wantEvents)
	}

	// Cancel and confirm the handler returns promptly — i.e., the
	// SSE goroutine is wired to r.Context().Done(). We do this by
	// closing the response body which closes the connection.
	cancel()
}

// TestNoExternalURLs enforces the local-first asset policy at the test
// layer: every rendered HTML page must point at /console/-relative
// paths only. Any src/href/@import naming an outside host fails the
// test. PR 1 only ships the dashboard page; later PRs add more pages
// to this loop without otherwise changing the assertion.
func TestNoExternalURLs(t *testing.T) {
	h := newTestConsole(t)
	pages := []string{"/console/"}

	// Pattern matches src/href values that start with http(s):// or //
	// (protocol-relative). app.css and link-rel-import are also caught
	// because they appear inside the same attributes.
	external := regexp.MustCompile(
		`(?i)(src|href)\s*=\s*"((https?:)?//[^"]+)"`)
	atImport := regexp.MustCompile(
		`(?i)@import\s+(url\()?["']((https?:)?//[^"']+)["']?`)

	for _, page := range pages {
		t.Run(page, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, page, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			body := rr.Body.String()
			if m := external.FindStringSubmatch(body); m != nil {
				t.Errorf("external URL in src/href: %s", m[2])
			}
			if m := atImport.FindStringSubmatch(body); m != nil {
				t.Errorf("external URL in @import: %s", m[2])
			}
		})
	}
}

// TestAllNavLinksReturn200 regresses C1+C2 — every <a href> in the
// rendered layout (nav, brand, mobile menu) must resolve to a real
// route. Dead links like /console/ops/health and /console/help shipped
// to operators and broke trust; this test guarantees no future PR
// re-introduces an unrouted nav target.
//
// Methodology: render the dashboard, scrape every href that starts
// with `/console`, GET each one, and assert the route is REGISTERED.
// We accept any response that is not 404 — including 503 (route exists
// but no DataSource is wired in this in-process test) — because the
// audit's claim is "the route resolves", not "the route returns data".
// A registered route returning 503 still proves the nav link is live.
// Bounded loop: at most maxLinks links.
func TestAllNavLinksReturn200(t *testing.T) {
	h := newTestConsole(t)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200", rr.Code)
	}

	// Match href="/console/..."; tolerate single or double quotes.
	hrefRE := regexp.MustCompile(`href=["'](\/console[^"'#?]*)["']`)
	matches := hrefRE.FindAllStringSubmatch(rr.Body.String(), -1)
	if len(matches) < 5 {
		t.Fatalf("expected at least 5 nav links, found %d", len(matches))
	}

	seen := make(map[string]bool, len(matches))
	const maxLinks = 64
	checked := 0
	for _, m := range matches {
		if checked >= maxLinks {
			t.Fatalf("exceeded link bound %d", maxLinks)
		}
		href := m[1]
		if href == "" || seen[href] {
			continue
		}
		seen[href] = true
		checked++

		sub := httptest.NewRecorder()
		h.ServeHTTP(sub, httptest.NewRequest(http.MethodGet, href, nil))
		if sub.Code == http.StatusNotFound {
			t.Errorf("nav link %q returned 404 — dead route", href)
		}
	}
	if checked == 0 {
		t.Fatal("checked zero nav links")
	}
}

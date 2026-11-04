// region_header_test.go checks the console header names the node's
// region (ADR-031) so an operator with two regions open can tell their
// consoles apart.
//
// Methodology: pure handler tests against fakeDataSource, no NATS. Mount
// the console with and without Config.Region, GET a page, and assert the
// header carries the region pill only when a region is set.
package console

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegionHeader_ShownWhenSet(t *testing.T) {
	const pill = `class="console-mode-pill console-region-pill"`
	for _, region := range []string{"", "us"} {
		h := Mount(Config{
			HTTPAddr: "127.0.0.1:0",
			AuthMode: AuthLoopback,
			Build:    "test",
			Region:   region,
			Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
			Data:     newFakeDS(),
		})
		for _, path := range []string{"/console/", "/console/workflows"} {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("region %q %s: status = %d", region, path, rr.Code)
			}
			body := rr.Body.String()
			header := body[:strings.Index(body, "</header>")]
			if region == "" {
				if strings.Contains(header, pill) {
					t.Errorf("%s: region pill rendered without a region", path)
				}
				continue
			}
			if !strings.Contains(header, pill) ||
				!strings.Contains(header, "Region us") {
				t.Errorf("%s: header lacks the region pill for us", path)
			}
		}
	}
}

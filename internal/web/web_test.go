package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/nats-io/nats.go"
)

// newTestUI builds a UI handler with non-nil dependencies. The handler
// under test no longer touches svc or nc — it redirects unconditionally
// — so an empty Service and a zero-value *nats.Conn satisfy the New
// guards without needing a live NATS connection.
func newTestUI(t *testing.T) *UI {
	t.Helper()
	return New(&api.Service{}, &nats.Conn{})
}

// TestHandlerRedirectsToConsole asserts the retired /ui stub now issues a
// permanent (301) redirect to /console/ instead of serving the legacy
// "coming soon" body. Old bookmarks under /ui resolve forward to the
// real console (issue #365).
func TestHandlerRedirectsToConsole(t *testing.T) {
	h := newTestUI(t).Handler()

	for _, path := range []string{"/ui", "/ui/"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if got := rec.Code; got != http.StatusMovedPermanently {
				t.Fatalf("status: got %d, want %d",
					got, http.StatusMovedPermanently)
			}
			if got := rec.Header().Get("Location"); got != "/console/" {
				t.Fatalf("Location: got %q, want %q", got, "/console/")
			}
			if body := rec.Body.String(); body == "DagNats UI — coming soon" {
				t.Fatalf("body still serves the retired coming-soon stub")
			}
		})
	}
}

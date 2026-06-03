// Package web provides the UI handler for the DagNats server.
package web

import (
	"net/http"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/nats-io/nats.go"
)

// UI serves the legacy /ui/ route, which now permanently redirects to
// the operator console at /console/. The route stays registered so old
// bookmarks resolve forward (issue #365).
type UI struct {
	svc *api.Service
	nc  *nats.Conn
}

// New creates a UI handler backed by the given service
// and NATS connection.
func New(svc *api.Service, nc *nats.Conn) *UI {
	if svc == nil {
		panic("web.New: svc is nil")
	}
	if nc == nil {
		panic("web.New: nc is nil")
	}
	return &UI{svc: svc, nc: nc}
}

// Handler returns an http.Handler that serves the web UI.
func (u *UI) Handler() http.Handler {
	if u == nil {
		panic("Handler: u is nil")
	}
	if u.svc == nil {
		panic("Handler: svc is nil")
	}
	return http.StripPrefix(
		"/ui",
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				// The real console lives at /console/. /ui is a
				// retired stub: permanently redirect so cached
				// bookmarks land on the live UI (issue #365).
				http.Redirect(
					w, r,
					"/console/",
					http.StatusMovedPermanently,
				)
			}),
	)
}

// Package web provides the UI handler for the DagNats server.
package web

import (
	"net/http"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/nats-io/nats.go"
)

// UI serves the web interface under /ui/.
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
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(
					"Content-Type",
					"text/html; charset=utf-8",
				)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(
					[]byte("DagNats UI — coming soon"),
				)
			}),
	)
}

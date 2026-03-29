package api

import "net/http"

// RESTHandler exposes the Service over HTTP. Routes and full implementation
// are added in a later task; this stub satisfies cmd/dagnats-api compilation.
type RESTHandler struct {
	svc *Service
}

// NewRESTHandler wraps a Service as an http.Handler. The handler panics if
// svc is nil — callers must always provide a fully initialised Service.
func NewRESTHandler(svc *Service) http.Handler {
	if svc == nil {
		panic("NewRESTHandler: svc must not be nil")
	}
	return &RESTHandler{svc: svc}
}

// ServeHTTP responds with 501 Not Implemented for all routes until the full
// REST layer is wired up in the api task.
func (h *RESTHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

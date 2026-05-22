package trigger

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/nats-io/nats.go"
)

// httpRegistrar owns the HTTPHandler routes keyed by "METHOD PATH".
// ADR-016. Map shared by reference with TriggerService for symmetry
// with the other registrars.
type httpRegistrar struct {
	nc     *nats.Conn
	routes map[string]*HTTPHandler
	mu     *sync.RWMutex // shared with TriggerService
}

func newHTTPRegistrar(
	nc *nats.Conn,
	routes map[string]*HTTPHandler,
	mu *sync.RWMutex,
) *httpRegistrar {
	if nc == nil {
		panic("newHTTPRegistrar: nc must not be nil")
	}
	if routes == nil {
		panic("newHTTPRegistrar: routes map must not be nil")
	}
	if mu == nil {
		panic("newHTTPRegistrar: mu must not be nil")
	}
	return &httpRegistrar{nc: nc, routes: routes, mu: mu}
}

// Activate registers the HTTP route. Idempotent: a second call with
// the same def.ID is a no-op. A different trigger trying to claim
// the same (method, path) is a registration-time error.
func (r *httpRegistrar) Activate(_ context.Context, def TriggerDef) error {
	if def.HTTP == nil {
		panic("httpRegistrar.Activate: def.HTTP must not be nil")
	}
	key := httpRouteKey(def.HTTP.Method, def.HTTP.Path)
	if existing, ok := r.routes[key]; ok {
		if existing.def.ID == def.ID {
			return nil
		}
		return fmt.Errorf(
			"http trigger %q: route %s already registered",
			def.ID, key,
		)
	}
	r.routes[key] = NewHTTPHandler(r.nc, def)
	return nil
}

// Deactivate removes the route whose def.ID matches. Idempotent.
func (r *httpRegistrar) Deactivate(_ context.Context, def TriggerDef) error {
	if def.ID == "" {
		panic("httpRegistrar.Deactivate: def.ID must not be empty")
	}
	for key, h := range r.routes {
		if h.def.ID == def.ID {
			delete(r.routes, key)
			return nil
		}
	}
	return nil
}

// ValidateConfig delegates to HTTPConfig.Validate. Wraps the error
// to match the surface produced by the package-level Validate().
func (r *httpRegistrar) ValidateConfig(def TriggerDef) error {
	if def.HTTP == nil {
		return fmt.Errorf("trigger %q: http config missing", def.ID)
	}
	if err := def.HTTP.Validate(); err != nil {
		return fmt.Errorf("trigger %q: http config: %w", def.ID, err)
	}
	return nil
}

// Router returns an http.Handler that dispatches by (method, path).
// 404 when path is unknown, 405 when path is known but method is not.
func (r *httpRegistrar) Router() http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter, req *http.Request,
	) {
		r.serveRoute(w, req)
	})
}

func (r *httpRegistrar) serveRoute(
	w http.ResponseWriter, req *http.Request,
) {
	r.mu.RLock()
	pathMethods := make(map[string]*HTTPHandler, 4)
	for _, handler := range r.routes {
		if handler.def.HTTP == nil {
			continue
		}
		if handler.def.HTTP.Path != req.URL.Path {
			continue
		}
		pathMethods[handler.def.HTTP.Method] = handler
	}
	r.mu.RUnlock()

	if len(pathMethods) == 0 {
		http.NotFound(w, req)
		return
	}
	handler, methodOK := pathMethods[req.Method]
	if !methodOK {
		http.Error(w, "method not allowed",
			http.StatusMethodNotAllowed)
		return
	}
	handler.ServeHTTP(w, req)
}

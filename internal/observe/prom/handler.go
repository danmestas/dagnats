package prom

import (
	"log/slog"
	"net/http"

	"github.com/danmestas/dagnats/internal/observe/metrics"
)

// Handler returns an http.HandlerFunc that renders the aggregator's
// current state in Prometheus text format. The handler:
//
//   - Rejects everything but GET (responds 405).
//   - Sets Content-Type to the canonical Prometheus exposition value.
//   - Streams output directly to the response writer; no intermediate
//     buffer so a slow scraper backpressures the renderer naturally.
//
// Auth is the caller's responsibility — see server.go's
// /metrics mount for the loopback-default policy. The exporter itself
// is auth-agnostic.
func Handler(agg *metrics.Aggregator, logger *slog.Logger) http.HandlerFunc {
	if agg == nil {
		panic("prom.Handler: agg is nil")
	}
	if logger == nil {
		panic("prom.Handler: logger is nil")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			panic("prom.Handler: r is nil")
		}
		if w == nil {
			panic("prom.Handler: w is nil")
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed",
				http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", ContentType)
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		if err := Render(w, agg); err != nil {
			logger.Warn("prom: render failed", "err", err)
		}
	}
}

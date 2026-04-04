package bridge

import (
	"net/http"
	"os"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// Bridge is an HTTP-to-NATS gateway that lets non-Go workers
// interact with DagNats over HTTP. Three deep endpoints expose the
// full worker lifecycle: connect, poll, and resolve.
//
// Authentication: when DAGNATS_BRIDGE_TOKEN env var is set, all
// requests must include Authorization: Bearer <token>. When unset,
// all requests are allowed (development mode).
type Bridge struct {
	nc           *nats.Conn
	js           nats.JetStreamContext
	ackMap       *AckMap
	checkpointKV nats.KeyValue
	signalKV     nats.KeyValue
	token        string
	tel          *observe.Telemetry

	// Pre-allocated metric instruments — created once in constructor.
	requestCount    observe.Counter
	requestDuration observe.Histogram
	ackMapSize      observe.Gauge
}

// NewBridge creates a Bridge. Panics on nil nc — a programmer error.
// Binds optional KV buckets for checkpoints (nil if not present).
// If tel is nil, uses a noop telemetry provider.
func NewBridge(nc *nats.Conn, tel *observe.Telemetry) *Bridge {
	if nc == nil {
		panic("NewBridge: nc must not be nil")
	}
	if tel == nil {
		tel = observe.NewNoopTelemetry()
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewBridge: JetStream init failed: " + err.Error())
	}
	checkpointKV, _ := js.KeyValue("checkpoints")
	signalKV, _ := js.KeyValue("signals")
	token := os.Getenv("DAGNATS_BRIDGE_TOKEN")
	return &Bridge{
		nc:           nc,
		js:           js,
		ackMap:       NewAckMap(),
		checkpointKV: checkpointKV,
		signalKV:     signalKV,
		token:        token,
		tel:          tel,
		requestCount: tel.Metrics.Counter(
			"bridge.requests", nil,
		),
		requestDuration: tel.Metrics.Histogram(
			"bridge.request.duration_ms", nil,
		),
		ackMapSize: tel.Metrics.Gauge(
			"bridge.ackmap.size", nil,
		),
	}
}

// Handler returns an http.Handler with the three bridge routes.
// The mux routes are:
//   - POST /v1/workers/connect
//   - POST /v1/tasks/poll
//   - POST /v1/tasks/ (resolve, path includes task ID)
func (b *Bridge) Handler() http.Handler {
	if b.nc == nil {
		panic("Bridge.Handler: nc must not be nil")
	}
	if b.ackMap == nil {
		panic("Bridge.Handler: ackMap must not be nil")
	}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"POST /v1/workers/connect", b.handleConnect,
	)
	mux.HandleFunc(
		"POST /v1/tasks/poll", b.handlePoll,
	)
	mux.HandleFunc(
		"POST /v1/tasks/{id}/resolve", b.handleResolve,
	)
	return b.authMiddleware(mux)
}

// authMiddleware checks the Authorization header when a bridge
// token is configured. Returns 401 on mismatch.
func (b *Bridge) authMiddleware(
	next http.Handler,
) http.Handler {
	if next == nil {
		panic("authMiddleware: next must not be nil")
	}
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if b.token != "" {
			auth := r.Header.Get("Authorization")
			expected := "Bearer " + b.token
			if auth != expected {
				http.Error(
					w, "unauthorized", http.StatusUnauthorized,
				)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

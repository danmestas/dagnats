package bridge

import (
	"context"
	"net/http"
	"os"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Bridge is an HTTP-to-NATS gateway that lets non-Go workers
// interact with DagNats over HTTP. Three deep endpoints expose the
// full worker lifecycle: connect, poll, and resolve.
//
// Authentication: when DAGNATS_BRIDGE_TOKEN env var is set, all
// requests must include Authorization: Bearer <token>. When unset,
// all requests are allowed (development mode).
//
// Every outbound NATS publish goes through *natsutil.TracingPublisher
// so W3C trace context (traceparent / tracestate) is auto-injected
// onto the outgoing message. This continues distributed traces from
// the inbound HTTP request into the NATS plane — without it, the
// trace ID would terminate at the HTTP boundary for non-Go workers.
type Bridge struct {
	pub          *natsutil.TracingPublisher
	nc           *nats.Conn
	js           jetstream.JetStream
	ackMap       *AckMap
	checkpointKV jetstream.KeyValue
	signalKV     jetstream.KeyValue
	token        string
	tracer       trace.Tracer

	// Pre-allocated metric instruments — created once in constructor.
	requestCount    metric.Int64Counter
	requestDuration metric.Float64Histogram
	ackMapSize      metric.Int64UpDownCounter
}

// NewBridge creates a Bridge. Panics on nil pub — a programmer
// error at startup. The TracingPublisher wraps both *nats.Conn
// and jetstream.JetStream and is the only legal publish surface
// inside this package (CI lint enforces this).
//
// Binds optional KV buckets for checkpoints and signals (nil if
// not present).
func NewBridge(pub *natsutil.TracingPublisher) *Bridge {
	if pub == nil {
		panic("NewBridge: pub must not be nil")
	}
	nc := pub.NC()
	js := pub.JS()
	if nc == nil {
		panic("NewBridge: pub.NC must not be nil")
	}
	if js == nil {
		panic("NewBridge: pub.JS must not be nil")
	}
	ctx := context.Background()
	checkpointKV, _ := js.KeyValue(ctx, "checkpoints")
	signalKV, _ := js.KeyValue(ctx, "signals")
	token := os.Getenv("DAGNATS_BRIDGE_TOKEN")
	m := otel.Meter("dagnats/bridge")
	reqCount, _ := m.Int64Counter("bridge.requests")
	reqDur, _ := m.Float64Histogram(
		"bridge.request.duration_ms",
	)
	ackSize, _ := m.Int64UpDownCounter("bridge.ackmap.size")
	return &Bridge{
		pub:             pub,
		nc:              nc,
		js:              js,
		ackMap:          NewAckMap(),
		checkpointKV:    checkpointKV,
		signalKV:        signalKV,
		token:           token,
		tracer:          otel.Tracer("dagnats/bridge"),
		requestCount:    reqCount,
		requestDuration: reqDur,
		ackMapSize:      ackSize,
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

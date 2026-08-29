package bridge

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Bridge is an HTTP-to-NATS gateway that lets non-Go workers
// interact with DagNats over HTTP. Three deep endpoints expose the
// full worker lifecycle: connect, poll, and resolve.
//
// Authentication (#627): DAGNATS_BRIDGE_TOKEN, when set, is now the
// admin/root credential -- it authenticates as workertoken.Claims{
// Admin: true}, bypassing task-type scoping entirely. A configured
// workertoken.Store (SetTokenStore) additionally accepts minted,
// revocable, scoped worker tokens ("dgn_{id}_{secret}" bearers) --
// checked whenever the admin bearer does not match, regardless of
// whether DAGNATS_BRIDGE_TOKEN is set. With neither an admin token nor
// a Store configured, every request is allowed (development mode,
// unchanged from before #627).
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
	tokenStore   *workertoken.Store
	tracer       trace.Tracer

	// Pre-allocated metric instruments — created once in constructor.
	requestCount    metric.Int64Counter
	requestDuration metric.Float64Histogram
	// metricsReg holds the observable-gauge registration. Bridge has no
	// shutdown path today, so nothing unregisters it; it is kept so a
	// future Close can, matching internal/trigger's scheduler.
	metricsReg metric.Registration
}

// routePoll / routeResolve tag the shared bridge.requests counter,
// which both endpoints increment. Without them the two collapse into
// one series and neither endpoint's request rate can be read on its
// own. bridge.request.duration_ms is recorded only on the poll path,
// and carries routePoll for consistency.
var (
	routePoll = metric.WithAttributes(
		attribute.String("route", "poll"),
	)
	routeResolve = metric.WithAttributes(
		attribute.String("route", "resolve"),
	)
)

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
	b := &Bridge{
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
	}
	// Registered after the Bridge exists because the callback closes
	// over it. A failure here costs one gauge, so it is logged rather
	// than fatal: losing observability must not stop the bridge from
	// serving workers.
	reg, err := RegisterBridgeMetrics(m, b)
	if err != nil {
		slog.Warn("bridge ackmap size gauge not registered",
			"error", err)
	}
	b.metricsReg = reg
	return b
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

// SetTokenStore wires an optional workertoken.Store into the bridge so
// authMiddleware accepts minted worker tokens alongside the single
// env-configured admin bearer. nil is a valid value (clears it); the
// zero-value Bridge has no Store, matching pre-#627 behavior exactly.
func (b *Bridge) SetTokenStore(store *workertoken.Store) {
	if b == nil {
		panic("SetTokenStore: b must not be nil")
	}
	b.tokenStore = store
}

// claimsContextKey is the unexported type for the Claims value
// authMiddleware attaches to each request's context. Unexported so no
// other package can inject or read forged claims.
type claimsContextKey struct{}

// claimsFromContext returns the Claims authMiddleware attached to ctx.
// Always present by the time a route handler runs -- authMiddleware
// wraps every route and returns before calling next on any rejection.
func claimsFromContext(ctx context.Context) workertoken.Claims {
	if ctx == nil {
		panic("claimsFromContext: ctx must not be nil")
	}
	claims, _ := ctx.Value(claimsContextKey{}).(workertoken.Claims)
	return claims
}

// authMiddleware resolves the Authorization header into Claims and
// rejects the request with 401 when none of the three modes documented
// on Bridge apply. On success it attaches Claims to the request
// context so downstream handlers (poll for scoping, connect for
// TokenID) can read them via claimsFromContext.
func (b *Bridge) authMiddleware(
	next http.Handler,
) http.Handler {
	if next == nil {
		panic("authMiddleware: next must not be nil")
	}
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		claims, ok := b.authorize(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), claimsContextKey{}, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authorize resolves one Authorization header value into Claims,
// trying each configured mode in order: the env admin bearer, then a
// configured Store, then dev-mode allow-all when neither is
// configured. See the Bridge doc comment for the full contract.
func (b *Bridge) authorize(header string) (workertoken.Claims, bool) {
	bearer, hasBearer := strings.CutPrefix(header, "Bearer ")
	if b.token != "" && hasBearer &&
		subtle.ConstantTimeCompare([]byte(bearer), []byte(b.token)) == 1 {
		return workertoken.Claims{Admin: true}, true
	}
	if b.tokenStore != nil {
		if !hasBearer {
			return workertoken.Claims{}, false
		}
		claims, err := b.tokenStore.Authorize(bearer)
		if err != nil {
			return workertoken.Claims{}, false
		}
		return claims, true
	}
	if b.token == "" {
		// Dev mode: no admin token, no Store configured. Unscoped
		// zero-value Claims is fine -- nothing downstream checks
		// AllowsTaskType unless a Store is configured.
		return workertoken.Claims{}, true
	}
	return workertoken.Claims{}, false
}

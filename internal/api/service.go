// api/service.go
// Control plane service: register workflow definitions, start runs, query state.
// This layer is shared by REST and NATS request/reply handlers -- it owns no
// transport concerns, only business logic backed by NATS KV and JetStream.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Service is the control plane for DagNats. It writes workflow
// definitions to KV and publishes WorkflowStarted events to the
// history stream. Run state is owned exclusively by the
// orchestrator -- the service only reads snapshots.
type Service struct {
	nc *nats.Conn
	js jetstream.JetStream
	// tp wraps publish operations so every workflow.started /
	// task re-enqueue / scheduled run dispatch carries W3C trace
	// context. Constructed once in NewService and shared with
	// helpers (timer.go, scheduled.go, bulk_run.go). #334.
	tp            *natsutil.TracingPublisher
	defKV         jetstream.KeyValue
	store         *engine.SnapshotStore
	tracer        trace.Tracer
	meter         metric.Meter
	triggerKV     jetstream.KeyValue
	signalKV      jetstream.KeyValue
	scheduledKV   jetstream.KeyValue
	idempotencyKV jetstream.KeyValue

	// Pre-allocated metric instruments -- created once.
	requestCount    metric.Int64Counter
	requestDuration metric.Float64Histogram
	errorCount      metric.Int64Counter

	// Per-runtime safety bounds (ADR-021 Phase A, #378). limits holds the
	// resolved quota/depth values; registerLimiter throttles runtime def
	// registration per tree-root (reuses engine.RateLimiter — nil-safe when
	// the rate_limits bucket is absent). Both are set in NewServiceWithLimits.
	limits          RuntimeLimits
	registerLimiter *engine.RateLimiter

	// grantPolicy holds the hot-reloadable capability-grant policy (#380).
	// Used to authorize promotion (AllowsPromote). nil → deny-by-default.
	grantPolicy *engine.GrantPolicyHolder
	// auditKV is the console_audit bucket the control plane emits action
	// rows into (best-effort; nil-tolerant). logger backs the best-effort
	// audit warn path.
	auditKV jetstream.KeyValue
	logger  *slog.Logger
}

// ServiceOption configures optional Service behavior (#380). Additive: a
// caller passing no options gets today's behavior with deny-by-default grant.
type ServiceOption func(*Service)

// WithGrantPolicyHolder wires the capability-grant policy holder so the
// service authorizes promotion via the same policy the engine grants the
// control-plane handle with (#380).
func WithGrantPolicyHolder(holder *engine.GrantPolicyHolder) ServiceOption {
	return func(s *Service) { s.grantPolicy = holder }
}

// WithAuditKV wires the console_audit KV bucket so the control plane emits
// audit rows (#380). nil is tolerated — auditkv.Emit warns and continues.
func WithAuditKV(kv jetstream.KeyValue) ServiceOption {
	return func(s *Service) { s.auditKV = kv }
}

// RuntimeLimits carries the resolved per-runtime safety bounds the control
// plane enforces (#378). Zero fields resolve to the default consts in
// runtimes.go at construction, so an old caller (or a zero-value struct)
// inherits the safe defaults rather than disabling the caps.
type RuntimeLimits struct {
	MaxActiveRunsPerRoot         int
	MaxDefsPerRoot               int
	MaxGenerationDepth           int
	MaxRegistersPerMinutePerRoot int
}

// NewService binds the control plane to an active NATS connection.
// Panics if JetStream init fails or the workflow_defs bucket does
// not exist -- callers must call natsutil.SetupAll first.
func NewService(nc *nats.Conn) *Service {
	return NewServiceWithLimits(nc, RuntimeLimits{})
}

// NewServiceWithLimits is NewService with explicit per-runtime safety
// bounds (#378). Zero fields in limits resolve to the default consts so a
// caller passing RuntimeLimits{} gets the safe defaults. The register
// rate-limiter is constructed once here via engine.NewRateLimiter — nil
// (an honest no-op) when the rate_limits bucket is absent.
func NewServiceWithLimits(
	nc *nats.Conn, limits RuntimeLimits, opts ...ServiceOption,
) *Service {
	if nc == nil {
		panic("NewServiceWithLimits: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(
			"NewServiceWithLimits: jetstream.New: " + err.Error(),
		)
	}
	ctx := context.Background()
	defKV, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		panic(
			"NewServiceWithLimits: workflow_defs bucket not found: " +
				err.Error(),
		)
	}
	triggerKV, _ := js.KeyValue(ctx, "triggers")
	signalKV, _ := js.KeyValue(ctx, "signals")
	scheduledKV, _ := js.KeyValue(ctx, "scheduled_runs")
	idempotencyKV, _ := js.KeyValue(
		ctx, "idempotency_keys",
	)
	m := otel.Meter("dagnats/api")
	reqCount, _ := m.Int64Counter("api.requests")
	reqDur, _ := m.Float64Histogram(
		"api.request.duration_ms",
	)
	errCount, _ := m.Int64Counter("api.errors")
	svc := &Service{
		nc:              nc,
		js:              js,
		limits:          resolveRuntimeLimits(limits),
		registerLimiter: engine.NewRateLimiter(js),
		tp:              natsutil.NewTracingPublisher(nc, js),
		defKV:           defKV,
		store:           engine.NewSnapshotStore(js),
		tracer:          otel.Tracer("dagnats/api"),
		meter:           m,
		triggerKV:       triggerKV,
		signalKV:        signalKV,
		scheduledKV:     scheduledKV,
		idempotencyKV:   idempotencyKV,
		requestCount:    reqCount,
		requestDuration: reqDur,
		errorCount:      errCount,
		logger:          slog.Default(),
	}
	for _, opt := range opts {
		opt(svc)
	}
	return svc
}

// checkIdempotency extracts the idempotency key from input, hashes it,
// and checks the KV for an existing run. Returns the existing run ID
// if found, empty string if not, or error on extraction/KV failure.
func (s *Service) checkIdempotency(
	ctx context.Context,
	workflowName string, keyPath string, input []byte,
) (string, error) {
	if workflowName == "" {
		panic("checkIdempotency: workflowName must not be empty")
	}
	if keyPath == "" {
		panic("checkIdempotency: keyPath must not be empty")
	}
	val, err := dag.ExtractDotPath(keyPath, input)
	if err != nil {
		return "", fmt.Errorf("extract key %q: %w", keyPath, err)
	}
	kvKey := idempotencyHash(workflowName, fmt.Sprintf("%v", val))

	entry, err := s.idempotencyKV.Get(
		ctx, kvKey,
	)
	if err == nil {
		return string(entry.Value()), nil
	}
	return "", nil
}

// storeIdempotencyKey stores the idempotency key -> run ID mapping.
// Uses Create for atomicity — if another request raced and won, this
// is a no-op (the winner's mapping stands).
func (s *Service) storeIdempotencyKey(
	ctx context.Context,
	workflowName string, keyPath string,
	input []byte, runID string,
) {
	val, err := dag.ExtractDotPath(keyPath, input)
	if err != nil {
		return // extraction failed — skip silently
	}
	kvKey := idempotencyHash(workflowName, fmt.Sprintf("%v", val))
	// Create fails if key exists (race loser) — that's fine.
	_, _ = s.idempotencyKV.Create(
		ctx, kvKey, []byte(runID),
	)
}

// idempotencyHash produces a deterministic KV key from workflow name
// and extracted key value using SHA-256.
func idempotencyHash(workflowName string, keyValue string) string {
	h := sha256.Sum256(
		[]byte(workflowName + "." + keyValue),
	)
	return hex.EncodeToString(h[:])
}

package natsutil

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// provisionAttemptTimeout bounds one create/update call. A JetStream API
// request sent while a clustered meta group is still settling can be
// dropped without any reply, so a single call with the whole setup budget
// would sit on it until the budget is gone. Short attempts get re-sent.
// The most common silent drop, a concurrent update of a stream still
// being created, is avoided altogether by ensureStream's create-first
// order; this bound is for the rest.
const provisionAttemptTimeout = 8 * time.Second

// provisionBudget bounds a whole clustered provisioning sweep. A fixed
// 30s was shorter than three cold nodes creating ~20 replicated resources
// against each other; each resource is still capped by
// provisionAttemptsMax x provisionAttemptTimeout.
const provisionBudget = 3 * time.Minute

// provisionBudgetStandalone is the sweep bound without a cluster: nothing
// is waiting on a meta leader, so a server that does not answer is broken
// and startup should fail fast.
const provisionBudgetStandalone = 30 * time.Second

// provisionContext returns the single budget context for one provisioning
// sweep. SetupAll shares it across every Setup* call so the worst case is
// one budget, not one per call.
func provisionContext(clustered bool) (context.Context, context.CancelFunc) {
	budget := provisionBudgetStandalone
	if clustered {
		budget = provisionBudget
	}
	return context.WithTimeout(context.Background(), budget)
}

// provisionAttemptsMax bounds retries per resource (fixed upper bound).
const provisionAttemptsMax = 5

// provisionRetryPauseBase and provisionRetryPauseMax shape the pause
// between attempts. It doubles so a re-election or a peer still joining
// gets seconds, not a fixed 500ms that five attempts spend in 2.5s.
const (
	provisionRetryPauseBase = 500 * time.Millisecond
	provisionRetryPauseMax  = 4 * time.Second
)

// provisionRetryPause is the pause after attempt (0-based).
func provisionRetryPause(attempt int) time.Duration {
	if attempt < 0 {
		panic("provisionRetryPause: attempt must not be negative")
	}
	pause := provisionRetryPauseBase
	for step := 0; step < attempt && pause < provisionRetryPauseMax; step++ {
		pause *= 2
	}
	if pause > provisionRetryPauseMax {
		pause = provisionRetryPauseMax
	}
	return pause
}

// isRetryableProvisionError reports whether err is the "no answer in
// time" class that a re-sent create/update can fix. Real configuration
// errors (bad config, insufficient storage) are returned immediately.
func isRetryableProvisionError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) {
		return true
	}
	// 10008 "JetStream system temporarily unavailable" (no meta leader
	// yet, or a re-election) and 10005 "no suitable peers for placement"
	// (a meta leader exists but has not seen enough peers for R3 yet) are
	// transient on a forming cluster. Other errors -- insufficient
	// resources, JetStream not enabled -- will not fix themselves. A
	// replica count the cluster can never meet still fails, just after
	// the bounded attempts instead of at once.
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == jsErrCodeClusterNotAvailable ||
		apiErr.ErrorCode == jsErrCodeNoSuitablePeers
}

// jsErrCodeClusterNotAvailable is JetStream error 10008, "JetStream system
// temporarily unavailable"; nats.go does not export a constant for it.
const jsErrCodeClusterNotAvailable jetstream.ErrorCode = 10008

// jsErrCodeNoSuitablePeers is JetStream error 10005, "no suitable peers
// for placement"; nats.go does not export a constant for it.
const jsErrCodeNoSuitablePeers jetstream.ErrorCode = 10005

// createStreamWithRetry is ensureStream under provisionWithRetry.
func createStreamWithRetry(
	parent context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig,
) error {
	return provisionWithRetry(parent, func(c context.Context) error {
		return ensureStream(c, js, cfg)
	})
}

// createKeyValueWithRetry is ensureKeyValue under provisionWithRetry.
func createKeyValueWithRetry(
	parent context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig,
) error {
	return provisionWithRetry(parent, func(c context.Context) error {
		return ensureKeyValue(c, js, cfg)
	})
}

// EnsureKeyValue creates cfg, or upgrades an existing bucket to it in
// place, with the create-first order and retry budget SetupAll uses. It is
// for buckets owned outside SetupAll (the console's audit bucket), so they
// reach the cluster's replication factor the same way.
func EnsureKeyValue(
	ctx context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig,
) (jetstream.KeyValue, error) {
	if js == nil {
		panic("EnsureKeyValue: js must not be nil")
	}
	if cfg.Replicas < 1 {
		panic("EnsureKeyValue: cfg.Replicas must be positive")
	}
	if err := createKeyValueWithRetry(ctx, js, cfg); err != nil {
		return nil, err
	}
	return js.KeyValue(ctx, cfg.Bucket)
}

// ensureStream creates or updates cfg. A replicated stream is created
// first and updated only if it exists with a different config, instead
// of CreateOrUpdateStream's update-first order: on nats-server 2.15 an
// update of an R3 stream whose create is still waiting for its stream
// leader gets no reply, and neither does that create. Nodes booting
// together hit that window on nearly every resource. A create of an
// identical replicated stream is answered at once. R1 keeps update-first:
// it has no stream leader to wait for, and a standalone server counts an
// existing stream's MaxBytes twice when it is "created" again.
//
// An update never lowers an existing stream's replica count (#784): a
// process started without cluster settings asks for one replica, and
// shrinking an R3 stream to R1 silently removes its failover.
func ensureStream(
	ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig,
) error {
	if ctx == nil {
		panic("ensureStream: ctx must not be nil")
	}
	if cfg.Name == "" {
		panic("ensureStream: stream name must not be empty")
	}
	if cfg.Replicas <= 1 {
		replicas, err := keepExistingReplicas(ctx, js, cfg.Name, cfg.Replicas)
		if err != nil {
			return err
		}
		if replicas <= 1 {
			_, err = js.CreateOrUpdateStream(ctx, cfg)
			return err
		}
		cfg.Replicas = replicas
		_, err = js.UpdateStream(ctx, cfg)
		return err
	}
	_, err := js.CreateStream(ctx, cfg)
	if !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return err
	}
	cfg.Replicas, err = keepExistingReplicas(ctx, js, cfg.Name, cfg.Replicas)
	if err != nil {
		return err
	}
	_, err = js.UpdateStream(ctx, cfg)
	return err
}

// ensureKeyValue is ensureStream for a KV bucket (CreateOrUpdateKeyValue
// is update-first too, and loses the same replies). It keeps an existing
// bucket's higher replica count the same way.
func ensureKeyValue(
	ctx context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig,
) error {
	if ctx == nil {
		panic("ensureKeyValue: ctx must not be nil")
	}
	if cfg.Bucket == "" {
		panic("ensureKeyValue: bucket must not be empty")
	}
	streamName := "KV_" + cfg.Bucket
	if cfg.Replicas <= 1 {
		replicas, err := keepExistingReplicas(ctx, js, streamName, cfg.Replicas)
		if err != nil {
			return err
		}
		if replicas <= 1 {
			_, err = js.CreateOrUpdateKeyValue(ctx, cfg)
			return err
		}
		cfg.Replicas = replicas
		_, err = js.UpdateKeyValue(ctx, cfg)
		return err
	}
	_, err := js.CreateKeyValue(ctx, cfg)
	if !errors.Is(err, jetstream.ErrBucketExists) {
		return err
	}
	cfg.Replicas, err = keepExistingReplicas(ctx, js, streamName, cfg.Replicas)
	if err != nil {
		return err
	}
	_, err = js.UpdateKeyValue(ctx, cfg)
	return err
}

// keepExistingReplicas returns the replica count an update of streamName
// should carry: requested, or the existing stream's count when that is
// higher. Provisioning never scales a resource down; an operator who
// wants fewer replicas edits the stream directly. A stream that does not
// exist yet takes requested as-is.
func keepExistingReplicas(
	ctx context.Context, js jetstream.JetStream, streamName string, requested int,
) (int, error) {
	if streamName == "" {
		panic("keepExistingReplicas: streamName must not be empty")
	}
	if requested < 0 {
		panic("keepExistingReplicas: requested must not be negative")
	}
	stream, err := js.Stream(ctx, streamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return requested, nil
	}
	if err != nil {
		return 0, err
	}
	existing := stream.CachedInfo().Config.Replicas
	if existing <= max(requested, 1) {
		return requested, nil
	}
	slog.Warn("natsutil: keeping existing replica count; provisioning "+
		"never scales a stream down",
		"stream", streamName, "existing_replicas", existing,
		"requested_replicas", requested)
	return existing, nil
}

// provisionWithRetry runs create with per-attempt timeouts inside parent,
// retrying only no-answer errors. create must be idempotent
// (ensureStream, ensureKeyValue). Returns the last error when attempts
// run out or parent ends.
func provisionWithRetry(
	parent context.Context, create func(context.Context) error,
) error {
	if parent == nil {
		panic("provisionWithRetry: parent must not be nil")
	}
	if create == nil {
		panic("provisionWithRetry: create must not be nil")
	}
	var err error
	for attempt := 0; attempt < provisionAttemptsMax; attempt++ {
		attemptCtx, cancel := context.WithTimeout(
			parent, provisionAttemptTimeout,
		)
		err = create(attemptCtx)
		cancel()
		if err == nil || !isRetryableProvisionError(err) {
			return err
		}
		if parent.Err() != nil {
			return err
		}
		select {
		case <-parent.Done():
			return err
		case <-time.After(provisionRetryPause(attempt)):
		}
	}
	return err
}

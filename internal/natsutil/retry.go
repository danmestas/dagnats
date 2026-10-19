package natsutil

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// provisionAttemptTimeout bounds one create/update call. A JetStream API
// request sent while a clustered meta group is still settling can be
// dropped without any reply, so a single call with the whole setup budget
// would sit on it until the budget is gone. Short attempts get re-sent.
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

// createStreamWithRetry is CreateOrUpdateStream under provisionWithRetry.
func createStreamWithRetry(
	parent context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig,
) error {
	return provisionWithRetry(parent, func(c context.Context) error {
		_, err := js.CreateOrUpdateStream(c, cfg)
		return err
	})
}

// provisionWithRetry runs create with per-attempt timeouts inside parent,
// retrying only no-answer errors. create must be idempotent
// (CreateOrUpdate*). Returns the last error when attempts run out or
// parent ends.
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

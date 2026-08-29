// api/queue_snapshot.go
// Periodic event.queue.snapshot publisher (#632). Runs on its own
// ticker at DAGNATS_QUEUE_SNAPSHOT_INTERVAL cadence -- never
// per-enqueue -- and publishes protocol.QueueSnapshot to the EVENTS
// stream ONLY when the queue state changed since the last publish
// (change-suppression), plus an unconditional heartbeat publish every
// 60s so a consumer can distinguish "still nothing pending" from "the
// publisher died". See docs/wire-protocol.md "Consumer contract: run
// lifecycle events" for the event.queue.snapshot schema/cadence.
//
// Mirrors server.startMetricsAggregator's shape (server/server.go):
// a setup step that can fail synchronously, then a goroutine bounded
// by a stop function the caller invokes on shutdown.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// queueSnapshotIntervalEnv is the operator-facing env var resolving
// the snapshot-check ticker interval.
const queueSnapshotIntervalEnv = "DAGNATS_QUEUE_SNAPSHOT_INTERVAL"

// queueSnapshotIntervalDefault/Min/Max bound
// DAGNATS_QUEUE_SNAPSHOT_INTERVAL. Min keeps a misconfigured operator
// from turning this into a per-enqueue publisher (defeats
// change-suppression's whole purpose); max keeps a consumer's
// "the publisher died" heuristic meaningful -- an interval near the
// 60s heartbeat would make the two signals indistinguishable.
const (
	queueSnapshotIntervalDefault = 5 * time.Second
	queueSnapshotIntervalMin     = 1 * time.Second
	queueSnapshotIntervalMax     = 5 * time.Minute
)

// queueSnapshotHeartbeatInterval is the unconditional publish cadence
// regardless of state change, so a consumer with no message for over
// a minute can conclude the publisher (or its NATS connection) is
// down rather than that the queue has simply been idle.
const queueSnapshotHeartbeatInterval = 60 * time.Second

// queueSnapshotSubject is the EVENTS-stream subject
// event.run.*-sibling events publish to -- reliable, durable,
// consumer-subscribable, matching the run-lifecycle event convention
// documented in docs/wire-protocol.md.
const queueSnapshotSubject = "event.queue.snapshot"

// ParseQueueSnapshotInterval resolves DAGNATS_QUEUE_SNAPSHOT_INTERVAL.
// Empty resolves to queueSnapshotIntervalDefault. Any parse failure or
// a value outside [queueSnapshotIntervalMin, queueSnapshotIntervalMax]
// is a hard error -- server startup must refuse to start on a
// misconfigured interval rather than silently clamping to a value the
// operator didn't ask for.
func ParseQueueSnapshotInterval(val string) (time.Duration, error) {
	if val == "" {
		return queueSnapshotIntervalDefault, nil
	}
	dur, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", queueSnapshotIntervalEnv, val, err)
	}
	if dur < queueSnapshotIntervalMin || dur > queueSnapshotIntervalMax {
		return 0, fmt.Errorf(
			"invalid %s %q: must be between %s and %s",
			queueSnapshotIntervalEnv, val,
			queueSnapshotIntervalMin, queueSnapshotIntervalMax,
		)
	}
	return dur, nil
}

// queueSnapshotPublisherConfig holds the publisher's tunables.
// heartbeatInterval and now are overridable only via unexported
// options (below) -- production callers always get the real 60s
// heartbeat and wall-clock time; tests shrink both to keep runs fast
// and deterministic.
type queueSnapshotPublisherConfig struct {
	heartbeatInterval time.Duration
	now               func() time.Time
}

// queueSnapshotPublisherOption configures a queueSnapshotPublisherConfig.
type queueSnapshotPublisherOption func(*queueSnapshotPublisherConfig)

// withQueueSnapshotHeartbeatInterval overrides the heartbeat cadence.
// Test-only: production always uses queueSnapshotHeartbeatInterval.
func withQueueSnapshotHeartbeatInterval(d time.Duration) queueSnapshotPublisherOption {
	return func(c *queueSnapshotPublisherConfig) { c.heartbeatInterval = d }
}

// withQueueSnapshotNow overrides the clock used to stamp SnapshotAt
// and compute oldest_wait_ms. Test-only: it freezes "now" so a
// change-suppression test can assert on real queue-state changes
// (enqueue/dequeue) without every tick's naturally-growing
// oldest_wait_ms also registering as a "change" once it crosses a
// rounded-second boundary.
func withQueueSnapshotNow(now func() time.Time) queueSnapshotPublisherOption {
	return func(c *queueSnapshotPublisherConfig) { c.now = now }
}

// StartQueueSnapshotPublisher starts the periodic event.queue.snapshot
// publisher goroutine. Returns a stop function that cancels the
// goroutine and blocks until it exits, and a synchronous setup error
// (currently none -- reserved so a future preflight check, e.g.
// verifying the EVENTS stream exists, has somewhere to fail before the
// goroutine starts).
func StartQueueSnapshotPublisher(
	ctx context.Context, tp *natsutil.TracingPublisher, js jetstream.JetStream,
	interval time.Duration, logger *slog.Logger,
	opts ...queueSnapshotPublisherOption,
) (func(), error) {
	if tp == nil {
		panic("StartQueueSnapshotPublisher: tp must not be nil")
	}
	if js == nil {
		panic("StartQueueSnapshotPublisher: js must not be nil")
	}
	if interval < queueSnapshotIntervalMin || interval > queueSnapshotIntervalMax {
		panic("StartQueueSnapshotPublisher: interval out of bounds")
	}
	cfg := queueSnapshotPublisherConfig{
		heartbeatInterval: queueSnapshotHeartbeatInterval,
		now:               time.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if logger == nil {
		logger = slog.Default()
	}
	pubCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go runQueueSnapshotPublisher(pubCtx, tp, js, interval, cfg, logger, done)
	return func() { cancel(); <-done }, nil
}

// runQueueSnapshotPublisher is the steady-state loop: two independent
// tickers (change-check at interval, heartbeat at
// cfg.heartbeatInterval) so a slow operator-configured snapshot
// interval never delays the heartbeat and vice versa.
func runQueueSnapshotPublisher(
	ctx context.Context, tp *natsutil.TracingPublisher, js jetstream.JetStream,
	interval time.Duration, cfg queueSnapshotPublisherConfig,
	logger *slog.Logger, done chan struct{},
) {
	defer close(done)
	snapshotTicker := time.NewTicker(interval)
	defer snapshotTicker.Stop()
	heartbeatTicker := time.NewTicker(cfg.heartbeatInterval)
	defer heartbeatTicker.Stop()

	var lastHash string
	for {
		select {
		case <-ctx.Done():
			return
		case <-snapshotTicker.C:
			lastHash = publishIfChanged(ctx, tp, js, cfg, logger, lastHash)
		case <-heartbeatTicker.C:
			publishQueueSnapshot(ctx, tp, js, cfg, logger)
		}
	}
}

// publishIfChanged builds the current snapshot and publishes it only
// when its normalized hash differs from lastHash, returning the hash
// to carry into the next tick either way (so an unpublished tick still
// advances the comparison baseline correctly on the next change).
func publishIfChanged(
	ctx context.Context, tp *natsutil.TracingPublisher, js jetstream.JetStream,
	cfg queueSnapshotPublisherConfig, logger *slog.Logger, lastHash string,
) string {
	snap, err := buildQueueSnapshot(ctx, js, cfg.now(), logger)
	if err != nil {
		logger.Warn("queue snapshot: build failed", "error", err)
		return lastHash
	}
	hash := hashQueueGroups(snap.Groups)
	if hash == lastHash {
		return lastHash
	}
	publishSnapshot(ctx, tp, snap, logger)
	return hash
}

// publishQueueSnapshot builds and unconditionally publishes the
// current snapshot -- the heartbeat path, which must fire regardless
// of change-suppression state.
func publishQueueSnapshot(
	ctx context.Context, tp *natsutil.TracingPublisher, js jetstream.JetStream,
	cfg queueSnapshotPublisherConfig, logger *slog.Logger,
) {
	snap, err := buildQueueSnapshot(ctx, js, cfg.now(), logger)
	if err != nil {
		logger.Warn("queue snapshot: heartbeat build failed", "error", err)
		return
	}
	publishSnapshot(ctx, tp, snap, logger)
}

// publishSnapshot marshals and publishes snap to
// event.queue.snapshot. Best-effort: a failure is logged, not
// returned -- this is a periodic observability signal, not a
// durability-critical write, matching publishRunEvent's failure
// policy (internal/engine/run_event.go).
func publishSnapshot(
	ctx context.Context, tp *natsutil.TracingPublisher,
	snap protocol.QueueSnapshot, logger *slog.Logger,
) {
	data, err := json.Marshal(snap)
	if err != nil {
		logger.Warn("queue snapshot: marshal failed", "error", err)
		return
	}
	msgID := "queue-snapshot-" + snap.SnapshotAt.Format(time.RFC3339Nano)
	_, err = tp.JSPublish(
		ctx, queueSnapshotSubject, data, jetstream.WithMsgID(msgID),
	)
	if err != nil {
		logger.Warn("queue snapshot: publish failed", "error", err)
	}
}

// hashQueueGroups builds a stable comparison key over groups, ignoring
// SnapshotAt (never part of "did the queue state change") and
// smoothing OldestWaitMs to the nearest second -- otherwise every tick
// would report a "changed" hash purely from millisecond clock drift on
// an otherwise-static queue, defeating change-suppression entirely.
// The precise (unrounded) value is still what gets published; only the
// comparison is coarsened.
func hashQueueGroups(groups []protocol.QueueGroup) string {
	h := sha256.New()
	for _, g := range groups {
		fmt.Fprintf(h, "%s|%d|", g.TaskType, g.Pending)
		if g.OldestWaitMs == nil {
			fmt.Fprint(h, "-;")
			continue
		}
		roundedSec := int64(math.Round(float64(*g.OldestWaitMs) / 1000))
		fmt.Fprintf(h, "%d;", roundedSec)
	}
	return hex.EncodeToString(h.Sum(nil))
}

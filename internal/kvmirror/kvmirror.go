// Package kvmirror keeps an in-memory view of a KV bucket current: Load
// replays every live key once, and Watch feeds every later change to the
// same callback, reconnecting with bounded backoff when the watch
// drops. The caller owns the view (its map, lock and decoding); this
// package owns enumeration and the reconnect loop.
//
// Watch-loss behavior: while the watch is down the caller keeps serving
// its last-known view, so a change committed elsewhere is invisible
// until the watch reconnects, bounded by BackoffMax. This trades bounded
// staleness for availability: failing every read during a brief NATS
// blip would be worse than serving a view at most ~30s old.
package kvmirror

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
)

// BackoffInitial / BackoffMax bound the watch reconnect loop's
// exponential backoff. Capped at 30s so a prolonged NATS outage does not
// busy-loop, while a brief blip recovers in well under a second.
const (
	BackoffInitial = 250 * time.Millisecond
	BackoffMax     = 30 * time.Second
)

// Bind returns the bucket and its backing stream, creating the bucket
// (History 1) only when absent. natsutil.SetupAll provisions every
// mirrored bucket at the cluster's replication factor; a blind
// CreateOrUpdate here would reset replicas to 1 on every node start,
// silently dropping HA for the bucket. Creating when absent keeps a
// store usable outside the standard server bootstrap, such as a test.
func Bind(
	ctx context.Context, js jetstream.JetStream, bucket string,
) (jetstream.KeyValue, jetstream.Stream, error) {
	if js == nil || ctx == nil {
		panic("kvmirror.Bind: js and ctx must not be nil")
	}
	if bucket == "" {
		panic("kvmirror.Bind: bucket must not be empty")
	}
	kv, err := js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:  bucket,
			History: 1,
		})
	}
	if err != nil {
		return nil, nil, fmt.Errorf("ensure %s bucket: %w", bucket, err)
	}
	stream, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve %s bucket stream: %w", bucket, err)
	}
	return kv, stream, nil
}

// Load calls apply with the current entry of every live key in kv,
// whose backing stream is stream.
//
// Keys come from natsutil.ListKeys (#698), not kv.ListKeys: a Put that
// replaces a history=1 key's only revision inside the listing watcher's
// setup window can drop the key (#699). Because ListKeys enumerates
// subject state, a subject whose last message is a delete marker also
// appears; Get tells the two apart by error. Any other Get failure
// fails Load, since swallowing it would silently drop a live record.
func Load(
	ctx context.Context, kv jetstream.KeyValue, stream jetstream.Stream,
	apply func(jetstream.KeyValueEntry),
) error {
	if kv == nil || stream == nil {
		panic("kvmirror.Load: kv and stream must not be nil")
	}
	if apply == nil {
		panic("kvmirror.Load: apply must not be nil")
	}
	keys, err := natsutil.ListKeys(ctx, stream, kv.Bucket())
	if err != nil {
		return err
	}
	for _, key := range keys {
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		apply(entry)
	}
	return nil
}

// Watch calls apply with every change to kv until ctx is cancelled,
// reconnecting the underlying watch whenever it drops. The initial
// replay is applied too, so a caller that ran Load first sees each key
// again and must order by revision. failures, when non-nil, counts
// failed watch attempts so a dead watch shows on a dashboard and not
// only in the paired warn log.
func Watch(
	ctx context.Context, kv jetstream.KeyValue, failures metric.Int64Counter,
	apply func(jetstream.KeyValueEntry),
) {
	if kv == nil || ctx == nil {
		panic("kvmirror.Watch: kv and ctx must not be nil")
	}
	if apply == nil {
		panic("kvmirror.Watch: apply must not be nil")
	}
	bucket := kv.Bucket()
	backoff := BackoffInitial
	for ctx.Err() == nil {
		watcher, err := kv.Watch(ctx, ">")
		if err != nil {
			slog.Warn("kvmirror: watch failed, retrying",
				"bucket", bucket, "error", err, "backoff", backoff)
			if failures != nil {
				failures.Add(context.Background(), 1)
			}
			if !sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = BackoffInitial
		consume(ctx, watcher, apply)
		shuttingDown := ctx.Err() != nil
		if stopErr := watcher.Stop(); stopErr != nil && !shuttingDown {
			// A Stop failure during shutdown is expected: the connection
			// underlying the subscription is already tearing down.
			slog.Warn("kvmirror: watch stop failed",
				"bucket", bucket, "error", stopErr)
		}
		if shuttingDown {
			return
		}
		slog.Warn("kvmirror: watch closed, reconnecting", "bucket", bucket)
	}
}

// consume drains watcher's updates into apply until the channel closes
// or ctx is cancelled.
func consume(
	ctx context.Context, watcher jetstream.KeyWatcher,
	apply func(jetstream.KeyValueEntry),
) {
	if watcher == nil {
		panic("kvmirror.consume: watcher must not be nil")
	}
	if apply == nil {
		panic("kvmirror.consume: apply must not be nil")
	}
	updates := watcher.Updates()
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-updates:
			if !ok {
				return
			}
			if entry == nil {
				continue // nil marks "initial replay complete"
			}
			apply(entry)
		}
	}
}

// sleep blocks for d or until ctx is cancelled, reporting false in the
// latter case so the caller stops retrying.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		panic("kvmirror.sleep: d must be positive")
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// nextBackoff doubles d, capped at BackoffMax.
func nextBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		panic("kvmirror.nextBackoff: d must be positive")
	}
	next := d * 2
	if next > BackoffMax || next <= 0 {
		return BackoffMax
	}
	return next
}

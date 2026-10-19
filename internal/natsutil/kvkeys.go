package natsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// kvKeysAttemptsMax bounds how many fresh watchers KVKeys opens.
const kvKeysAttemptsMax = 3

// kvKeysIdleTimeout is how long a watcher may deliver nothing before
// the attempt is abandoned. It is an idle timer, reset on every entry,
// not a wall-clock cap: a large bucket legitimately takes a while to
// list, and restarting it from zero would make every retry fail too.
// A var so a test can shrink it.
var kvKeysIdleTimeout = 10 * time.Second

// kvKeysDrainBound bounds the post-Stop drain goroutine. A var so a
// test can shrink it.
var kvKeysDrainBound = 5 * time.Second

// kvWatchAll is the seam tests swap to stall or observe the watcher.
var kvWatchAll = func(
	ctx context.Context, kv jetstream.KeyValue, opts ...jetstream.WatchOpt,
) (jetstream.KeyWatcher, error) {
	return kv.WatchAll(ctx, opts...)
}

// errKVKeysIdle marks an attempt abandoned for lack of progress. It is
// distinct from the caller's own deadline so only a stall is retried.
var errKVKeysIdle = errors.New("kv keys watcher idle")

// KVKeys lists a bucket's live keys, sorted and de-duplicated, like
// jetstream.KeyValue.Keys but with every wait bounded. nats.go's Keys
// uses ctx only while setting up the watcher, then blocks on the
// watcher's update channel with no deadline; a stalled watcher (seen
// in CI) hangs the caller forever. Here a watcher that goes
// kvKeysIdleTimeout without delivering anything is abandoned and
// retried with a fresh one; ctx remains the overall cap. Every other
// error, including jetstream.ErrNoKeysFound, is returned unchanged.
func KVKeys(
	ctx context.Context, kv jetstream.KeyValue, opts ...jetstream.WatchOpt,
) ([]string, error) {
	if ctx == nil || kv == nil {
		panic("natsutil.KVKeys: nil ctx or kv")
	}
	if kvKeysAttemptsMax < 1 {
		panic("natsutil.KVKeys: kvKeysAttemptsMax must be positive")
	}
	for attempt := 1; attempt <= kvKeysAttemptsMax; attempt++ {
		keys, err := watchKeysOnce(ctx, kv, opts)
		if err == nil {
			return keys, nil
		}
		if !errors.Is(err, errKVKeysIdle) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("list keys of bucket %q: %d attempts idle for %s each: %w",
		kv.Bucket(), kvKeysAttemptsMax, kvKeysIdleTimeout, context.DeadlineExceeded)
}

// watchKeysOnce mirrors nats.go's Keys with an idle timer and ctx in
// the select, so a stalled watcher cannot block the caller.
func watchKeysOnce(
	ctx context.Context, kv jetstream.KeyValue, opts []jetstream.WatchOpt,
) ([]string, error) {
	if ctx == nil || kvKeysIdleTimeout <= 0 {
		panic("natsutil.watchKeysOnce: nil ctx or non-positive idle timeout")
	}
	opts = append(slices.Clone(opts), jetstream.IgnoreDeletes(), jetstream.MetaOnly())
	watcher, err := kvWatchAll(ctx, kv, opts...)
	if err != nil {
		return nil, err
	}
	defer stopAndDrain(watcher, kv.Bucket())
	idle := time.NewTimer(kvKeysIdleTimeout)
	defer idle.Stop()
	var keys []string
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-idle.C:
			return nil, errKVKeysIdle
		case entry, open := <-watcher.Updates():
			if entry == nil || !open {
				return finishKeys(keys)
			}
			keys = append(keys, entry.Key())
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(kvKeysIdleTimeout)
		}
	}
}

func finishKeys(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, jetstream.ErrNoKeysFound
	}
	slices.Sort(keys)
	return slices.Compact(keys), nil
}

// stopAndDrain stops the watcher, then drains its channel in the
// background. nats.go's subscription callback blocks on a full update
// channel while holding the watcher mutex, and Stop's close handler
// needs that mutex, so an abandoned mid-stream watcher leaks its
// goroutine unless someone keeps reading. The drain is bounded.
func stopAndDrain(watcher jetstream.KeyWatcher, bucket string) {
	if watcher == nil {
		panic("natsutil.stopAndDrain: nil watcher")
	}
	updates := watcher.Updates()
	drainBound := kvKeysDrainBound
	go func() {
		bound := time.NewTimer(drainBound)
		defer bound.Stop()
		for {
			select {
			case _, open := <-updates:
				if !open {
					return
				}
			case <-bound.C:
				return
			}
		}
	}()
	if err := watcher.Stop(); err != nil {
		slog.Debug("kv keys watcher stop", "bucket", bucket, "err", err)
	}
}

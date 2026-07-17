// natsutil/parallel.go
// Concurrent KV fetching with bounded parallelism. Used across engine,
// API, and trigger packages to parallelize Keys()+Get() patterns.
package natsutil

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"
)

// DefaultParallelism is the standard concurrency limit for parallel
// KV operations. Balances throughput against NATS connection pressure.
const DefaultParallelism = 16

// ParallelGet fetches multiple KV entries concurrently with the given
// concurrency limit. Keys that no longer exist (deleted between
// Keys() and Get()) are silently skipped — callers receive a clean
// slice with no nils. Panics if kv is nil or limit is not positive.
func ParallelGet(
	kv nats.KeyValue, keys []string, limit int,
) ([]nats.KeyValueEntry, error) {
	if kv == nil {
		panic("ParallelGet: kv must not be nil")
	}
	if limit <= 0 {
		panic("ParallelGet: limit must be positive")
	}
	if len(keys) == 0 {
		return []nats.KeyValueEntry{}, nil
	}

	// Each goroutine writes to a distinct index — no mutex needed.
	raw := make([]nats.KeyValueEntry, len(keys))
	var g errgroup.Group
	g.SetLimit(limit)

	for i, key := range keys {
		g.Go(func() error {
			entry, err := kv.Get(key)
			if err != nil {
				if errors.Is(err, nats.ErrKeyNotFound) {
					return nil
				}
				return fmt.Errorf("get %q: %w", key, err)
			}
			raw[i] = entry
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Compact: remove nil entries from deleted keys.
	entries := make([]nats.KeyValueEntry, 0, len(keys))
	for _, e := range raw {
		if e != nil {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// ParallelGetJS fetches multiple KV entries concurrently using the
// new jetstream.KeyValue API. Same bounded-parallelism approach as
// ParallelGet but with context-aware operations.
func ParallelGetJS(
	kv jetstream.KeyValue, keys []string, limit int,
) ([]jetstream.KeyValueEntry, error) {
	if kv == nil {
		panic("ParallelGetJS: kv must not be nil")
	}
	if limit <= 0 {
		panic("ParallelGetJS: limit must be positive")
	}
	if len(keys) == 0 {
		return []jetstream.KeyValueEntry{}, nil
	}

	raw := make([]jetstream.KeyValueEntry, len(keys))
	var g errgroup.Group
	g.SetLimit(limit)

	for i, key := range keys {
		g.Go(func() error {
			entry, err := kv.Get(
				context.Background(), key,
			)
			if err != nil {
				if errors.Is(err, jetstream.ErrKeyNotFound) {
					return nil
				}
				return fmt.Errorf("get %q: %w", key, err)
			}
			raw[i] = entry
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	entries := make([]jetstream.KeyValueEntry, 0, len(keys))
	for _, e := range raw {
		if e != nil {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// ParallelGetJSBestEffort fetches KV entries concurrently, bounding each
// GET with perKeyTimeout and DEGRADING rather than aborting on a slow key.
// A per-key timeout, deleted key, or transient class error SKIPS that key
// (counted in skipped) and the batch continues — this is the #523 fix, so
// one slow GET on a huge bucket can no longer discard every entry the caller
// could have used. Only a caller-ctx cancellation aborts the whole batch.
// Returns the fetched values keyed by run key, the skipped count, and an
// error only on caller cancellation. Distinct from ParallelGetJS, which is
// all-or-nothing and kept intact for its other callers.
func ParallelGetJSBestEffort(
	ctx context.Context, kv jetstream.KeyValue, keys []string,
	parallelism int, perKeyTimeout time.Duration,
) (map[string][]byte, int, error) {
	if kv == nil {
		panic("ParallelGetJSBestEffort: kv must not be nil")
	}
	if parallelism <= 0 {
		panic("ParallelGetJSBestEffort: parallelism must be positive")
	}
	if perKeyTimeout <= 0 {
		panic("ParallelGetJSBestEffort: perKeyTimeout must be positive")
	}
	entries := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return entries, 0, nil
	}
	var mu sync.Mutex
	skipped := 0
	// WithContext so a caller cancellation (or the first hard error, which
	// only ever fires ON that cancellation) short-circuits pending GETs.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)
	for _, key := range keys {
		g.Go(func() error {
			return getKeyBestEffort(
				gctx, kv, key, perKeyTimeout, &mu, entries, &skipped,
			)
		})
	}
	if err := g.Wait(); err != nil {
		return entries, skipped, err
	}
	return entries, skipped, nil
}

// getKeyBestEffort fetches one key under a perKeyTimeout derived from ctx.
// A caller cancellation (ctx.Err() != nil) is returned as a hard error so
// the batch aborts; any other GET failure (per-key deadline, not-found,
// transient) is swallowed as a skip so the batch proceeds.
func getKeyBestEffort(
	ctx context.Context, kv jetstream.KeyValue, key string,
	perKeyTimeout time.Duration, mu *sync.Mutex,
	entries map[string][]byte, skipped *int,
) error {
	if key == "" {
		panic("getKeyBestEffort: key must not be empty")
	}
	if mu == nil {
		panic("getKeyBestEffort: mu must not be nil")
	}
	getCtx, cancel := context.WithTimeout(ctx, perKeyTimeout)
	defer cancel()
	entry, err := kv.Get(getCtx, key)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // caller cancelled → abort the batch
		}
		mu.Lock()
		*skipped++
		mu.Unlock()
		return nil // slow/absent/transient key → skip, keep going
	}
	mu.Lock()
	entries[key] = entry.Value()
	mu.Unlock()
	return nil
}

// natsutil/parallel.go
// Concurrent KV fetching with bounded parallelism. Used across engine,
// API, and trigger packages to parallelize Keys()+Get() patterns.
package natsutil

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
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
		i, key := i, key
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

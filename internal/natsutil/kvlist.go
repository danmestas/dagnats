package natsutil

import (
	"context"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// ListKeys enumerates a KV bucket's keys from its backing stream's
// server-side subject state instead of kv.ListKeys/ListKeysFiltered.
//
// kv.ListKeys builds its answer from a watcher: a consumer is created
// and its initial delivery is treated as the set of live keys. That
// snapshot is not atomic with respect to concurrent writers. On a
// history=1 bucket, a Put immediately replaces a key's only revision;
// when that lands inside the watcher's setup window the key can be
// omitted from the listing entirely, even though a plain Get for it
// still succeeds (#699: worker.Directory.List() reported a live,
// actively-heartbeating worker as absent roughly once per 2000 calls).
//
// A filtered stream Info resolves server-side under the store lock, so
// a concurrent Put cannot hide an existing key. That atomicity holds as
// long as the answer is one response: a bucket with more than the
// server's JSMaxSubjectDetails (100_000) subjects is assembled from
// several paginated requests and is then only eventually consistent.
//
// Callers whose writes are ever Put (not just Create/Delete) MUST treat
// this as the primitive to enumerate over, not the final key set: a
// subject whose last message is a delete marker still appears here, so
// a caller that needs "live keys only" must Get each returned key and
// skip jetstream.ErrKeyNotFound -- exactly as worker.Directory.List()
// does -- while still propagating any OTHER Get error. Swallowing a
// non-NotFound Get error is indistinguishable, from the result, to
// silently dropping a live key, which is the exact failure mode this
// function exists to eliminate.
//
// stream must be the bucket's own backing stream ("KV_<bucket>"), not a
// higher-level jetstream.KeyValue -- ListKeys reads its subject state
// directly. A hot-path caller should resolve that stream once (e.g. at
// construction, the way worker.Directory does) rather than re-resolve
// it on every call.
func ListKeys(
	ctx context.Context, stream jetstream.Stream, bucket string,
) ([]string, error) {
	if stream == nil {
		panic("ListKeys: stream must not be nil")
	}
	if bucket == "" {
		panic("ListKeys: bucket must not be empty")
	}
	prefix := "$KV." + bucket + "."
	info, err := stream.Info(
		ctx, jetstream.WithSubjectFilter(prefix+">"),
	)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(info.State.Subjects))
	for subject := range info.State.Subjects {
		keys = append(keys, strings.TrimPrefix(subject, prefix))
	}
	// SORTED BY CONTRACT, not as a convenience. info.State.Subjects is a
	// Go map, so ranging it yields a different order on every call --
	// whereas kv.ListKeys, which callers are migrating away from,
	// happened to deliver watcher-replay (creation) order. Any caller
	// that reversed, truncated to first-N, paginated, or simply printed
	// the result would silently start flapping between calls, with
	// nothing failing to reveal it. Making order a property of this
	// helper kills that whole class once instead of asking six call
	// sites to remember it.
	//
	// Lexicographic, NOT creation order: a caller that genuinely needs
	// creation order (see internal/engine's listRunIndexKeys, which
	// ScanNewestFirst depends on) cannot use this helper at all and must
	// keep ListKeysFiltered. Timestamp-prefixed key spaces such as the
	// audit bucket get chronological order from this for free, since
	// their keys sort that way by construction.
	sort.Strings(keys)
	return keys, nil
}

// cli/clean_521_test.go
// Tests for issue #521 clean-command fixes. Methodology: embedded NATS,
// populate a stream/bucket, run the clean primitive, assert the wire-level
// outcome. Fix 2 guards that age-based purge SKIPS work-queue streams
// (un-acked messages are live tasks; the ordered consumer used to find the
// age boundary is rejected on a work-queue with err 10084) while still
// age-purging a limits-policy stream. Fix 3 guards the O(1) full-clear via
// the KV backing-stream purge and the per-key bounded contexts on the
// --older-than path. Bounded timeouts on all ops.
package cli

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// makeStream creates a stream with the given retention and publishes one
// message to it, returning the live jetstream.Stream handle.
func makeStream(
	t *testing.T,
	js jetstream.JetStream,
	name, subject string,
	retention jetstream.RetentionPolicy,
) jetstream.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Retention: retention,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		t.Fatalf("CreateStream(%s): %v", name, err)
	}
	if _, err := js.Publish(ctx, subject, []byte("task")); err != nil {
		t.Fatalf("Publish(%s): %v", subject, err)
	}
	return stream
}

// TestPurgeStreamBefore_SkipsWorkQueue is the Fix 2 guard. On a work-queue
// stream whose one message is older than the cutoff, the old code took the
// "newest older than cutoff → purge everything" branch and DELETED the live
// task (or, on the boundary branch, hit err 10084). The message is a live
// pending task; age-purge must skip the stream entirely and preserve it.
func TestPurgeStreamBefore_SkipsWorkQueue(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream := makeStream(t, js, "WQ_521", "wq521.a",
		jetstream.WorkQueuePolicy)

	// Age the message so its timestamp is well before the cutoff.
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	purged := purgeStreamBefore(ctx, stream, time.Millisecond)

	// Positive: the work-queue stream is skipped, not purged.
	if purged {
		t.Fatal("work-queue stream should be skipped, not purged by age")
	}

	// Negative: the live task must still be present.
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("expected 1 live task preserved, got %d", info.State.Msgs)
	}
}

// TestPurgeStreamBefore_LimitsPolicyStillPurges is the negative space for
// Fix 2: a non-work-queue (limits) stream whose messages are all older than
// the cutoff must still be age-purged normally.
func TestPurgeStreamBefore_LimitsPolicyStillPurges(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream := makeStream(t, js, "LIM_521", "lim521.a",
		jetstream.LimitsPolicy)

	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	purged := purgeStreamBefore(ctx, stream, time.Millisecond)

	// Positive: a limits stream is still age-purged.
	if !purged {
		t.Fatal("limits stream should be age-purged")
	}

	// Negative: no messages remain.
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("expected 0 msgs after purge, got %d", info.State.Msgs)
	}
}

// TestPurgeKVBucket_LargeBucketFastClear is the Fix 3 full-clear guard: a
// bucket with many keys is emptied in one round trip via the backing-stream
// purge (O(1)), where the old per-key Delete loop scaled with key count and
// blew the shared clean deadline. The whole clear runs under a tight
// deadline that a per-key loop over this many keys would strain.
func TestPurgeKVBucket_LargeBucketFastClear(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	seedCtx := context.Background()
	kv, err := js.KeyValue(seedCtx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	const keyCount = 500
	for i := 0; i < keyCount; i++ {
		if _, err := kv.Put(seedCtx,
			fmt.Sprintf("run-%d", i), []byte("snapshot")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// A tight deadline the O(1) purge clears trivially.
	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	if err := purgeKVBucket(ctx, js, "workflow_runs"); err != nil {
		t.Fatalf("purgeKVBucket: %v", err)
	}

	// Positive: the bucket is empty.
	keys, err := kv.Keys(seedCtx)
	if err != nil && err != jetstream.ErrNoKeysFound {
		t.Fatalf("Keys after purge: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected 0 keys after purge, got %d", len(keys))
	}

	// Negative: the bucket itself survives (still usable).
	if _, err := kv.Put(seedCtx, "after", []byte("x")); err != nil {
		t.Fatalf("bucket unusable after purge: %v", err)
	}
}

// TestPurgeKVBucketBefore_PerKeyContexts guards the Fix 3 --older-than path:
// the per-key bounded contexts must still delete old keys and keep recent
// ones over a multi-key bucket, running under a short caller deadline without
// a shared-deadline failure (each Get/Delete gets its own budget).
func TestPurgeKVBucketBefore_PerKeyContexts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	seedCtx := context.Background()
	kv, err := js.KeyValue(seedCtx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}

	const oldCount = 20
	for i := 0; i < oldCount; i++ {
		if _, err := kv.Put(seedCtx,
			fmt.Sprintf("old-%d", i), []byte("old")); err != nil {
			t.Fatalf("Put old %d: %v", i, err)
		}
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := kv.Put(seedCtx, "fresh", []byte("new")); err != nil {
		t.Fatalf("Put fresh: %v", err)
	}

	// A short caller deadline: the per-key contexts are independent of it.
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	if err := purgeKVBucketBefore(ctx, kv, 30*time.Millisecond); err != nil {
		t.Fatalf("purgeKVBucketBefore: %v", err)
	}

	// Positive: the fresh key survives.
	entry, err := kv.Get(seedCtx, "fresh")
	if err != nil {
		t.Fatalf("fresh key should survive: %v", err)
	}
	if string(entry.Value()) != "new" {
		t.Fatalf("fresh value = %q, want new", string(entry.Value()))
	}

	// Negative: an old key was deleted.
	if _, err := kv.Get(seedCtx, "old-0"); err == nil {
		t.Fatal("old-0 should have been deleted")
	}
}

// internal/natsutil/kvkeys_test.go
// Tests for KVKeys, the bounded replacement for jetstream's Keys.
// Methodology: integration tests against a real embedded NATS server
// (one per test) for the sorted/empty/deleted/cancel contracts; the
// stall and retry contracts swap the kvWatchAll seam for a fake watcher
// that delivers on a schedule the test controls, since a real watcher
// cannot be stalled on demand. Timers are shrunk so tests finish in
// milliseconds.
// Bounded timeouts throughout.
package natsutil

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func kvKeysTestBucket(t *testing.T, name string) (context.Context, jetstream.KeyValue) {
	t.Helper()
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name, History: 1})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	return ctx, kv
}

func shrinkKVKeysTimers(t *testing.T, idle, drain time.Duration) {
	t.Helper()
	prevIdle, prevDrain, prevWatch := kvKeysIdleTimeout, kvKeysDrainBound, kvWatchAll
	kvKeysIdleTimeout, kvKeysDrainBound = idle, drain
	t.Cleanup(func() {
		kvKeysIdleTimeout, kvKeysDrainBound, kvWatchAll = prevIdle, prevDrain, prevWatch
	})
}

type fakeEntry struct {
	jetstream.KeyValueEntry
	key string
}

func (e fakeEntry) Key() string { return e.key }

// fakeWatcher delivers entries on a schedule it controls; it never
// closes its channel, so a stall is "no more sends".
type fakeWatcher struct{ updates chan jetstream.KeyValueEntry }

func (w *fakeWatcher) Context() context.Context                { return context.Background() }
func (w *fakeWatcher) Updates() <-chan jetstream.KeyValueEntry { return w.updates }
func (w *fakeWatcher) Stop() error                             { return nil }

// stallingWatcher relays the first passN entries of a real watcher,
// then stops reading it until Stop, leaving the real producer blocked
// mid-stream exactly as an abandoned attempt does. After Stop it
// relays freely, so only a consumer that keeps draining frees it.
type stallingWatcher struct {
	jetstream.KeyWatcher
	out     chan jetstream.KeyValueEntry
	stopped chan struct{}
}

func newStallingWatcher(real jetstream.KeyWatcher, passN int) *stallingWatcher {
	w := &stallingWatcher{
		KeyWatcher: real,
		out:        make(chan jetstream.KeyValueEntry),
		stopped:    make(chan struct{}),
	}
	go func() {
		defer close(w.out)
		for n := 0; ; n++ {
			if n >= passN {
				<-w.stopped
			}
			entry, open := <-real.Updates()
			if !open {
				return
			}
			w.out <- entry
		}
	}()
	return w
}

func (w *stallingWatcher) Updates() <-chan jetstream.KeyValueEntry { return w.out }

func (w *stallingWatcher) Stop() error {
	close(w.stopped)
	return w.KeyWatcher.Stop()
}

func stalledWatcher() *fakeWatcher {
	return &fakeWatcher{updates: make(chan jetstream.KeyValueEntry)}
}

func TestKVKeys_ReturnsSortedKeys(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_sorted")
	for _, key := range []string{"c", "a", "b"} {
		if _, err := kv.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	keys, err := KVKeys(ctx, kv)
	if err != nil {
		t.Fatalf("KVKeys: %v", err)
	}
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Fatalf("keys = %v, want [a b c]", keys)
	}
}

func TestKVKeys_EmptyBucketReturnsErrNoKeysFound(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_empty")
	keys, err := KVKeys(ctx, kv)
	if !errors.Is(err, jetstream.ErrNoKeysFound) {
		t.Fatalf("err = %v, want ErrNoKeysFound", err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys = %v, want none", keys)
	}
}

func TestKVKeys_ExcludesDeletedKeys(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_deleted")
	for _, key := range []string{"a", "b"} {
		if _, err := kv.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	if err := kv.Delete(ctx, "b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	keys, err := KVKeys(ctx, kv)
	if err != nil {
		t.Fatalf("KVKeys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "a" {
		t.Fatalf("keys = %v, want [a]", keys)
	}
}

func TestKVKeys_CancelledParentReturnsWithoutRetry(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_cancel")
	shrinkKVKeysTimers(t, 5*time.Second, time.Second)
	calls := 0
	kvWatchAll = func(context.Context, jetstream.KeyValue, ...jetstream.WatchOpt,
	) (jetstream.KeyWatcher, error) {
		calls++
		return stalledWatcher(), nil
	}
	parent, cancel := context.WithCancel(ctx)
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	_, err := KVKeys(parent, kv)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("calls = %d after %s, want 1 call, prompt return", calls, time.Since(start))
	}
}

func TestKVKeys_ExpiredParentDeadlineDoesNotRetry(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_parent_deadline")
	shrinkKVKeysTimers(t, 5*time.Second, time.Second)
	calls := 0
	kvWatchAll = func(context.Context, jetstream.KeyValue, ...jetstream.WatchOpt,
	) (jetstream.KeyWatcher, error) {
		calls++
		return stalledWatcher(), nil
	}
	parent, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err := KVKeys(parent, kv)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (parent deadline must not retry)", calls)
	}
}

func TestKVKeys_RetriesIdleWatcherThenSucceeds(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_retry")
	if _, err := kv.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	shrinkKVKeysTimers(t, 100*time.Millisecond, time.Second)
	calls := 0
	real := kvWatchAll
	kvWatchAll = func(c context.Context, k jetstream.KeyValue, o ...jetstream.WatchOpt,
	) (jetstream.KeyWatcher, error) {
		calls++
		if calls == 1 {
			return stalledWatcher(), nil
		}
		return real(c, k, o...)
	}
	keys, err := KVKeys(ctx, kv)
	if err != nil {
		t.Fatalf("KVKeys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "k" || calls != 2 {
		t.Fatalf("keys = %v calls = %d, want [k] after 2 calls", keys, calls)
	}
}

func TestKVKeys_GivesUpAfterAttemptsMax(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_giveup")
	shrinkKVKeysTimers(t, 50*time.Millisecond, time.Second)
	calls := 0
	kvWatchAll = func(context.Context, jetstream.KeyValue, ...jetstream.WatchOpt,
	) (jetstream.KeyWatcher, error) {
		calls++
		return stalledWatcher(), nil
	}
	_, err := KVKeys(ctx, kv)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if calls != kvKeysAttemptsMax {
		t.Fatalf("calls = %d, want %d", calls, kvKeysAttemptsMax)
	}
	if got := err.Error(); !strings.Contains(got, "kvkeys_giveup") {
		t.Fatalf("err %q does not name the bucket", got)
	}
}

// A slow but steady watcher must finish: total time exceeds the idle
// timeout, but no single gap does.
func TestKVKeys_IdleTimerResetsOnEachEntry(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_steady")
	shrinkKVKeysTimers(t, 150*time.Millisecond, time.Second)
	calls := 0
	kvWatchAll = func(context.Context, jetstream.KeyValue, ...jetstream.WatchOpt,
	) (jetstream.KeyWatcher, error) {
		calls++
		w := &fakeWatcher{updates: make(chan jetstream.KeyValueEntry)}
		go func() {
			for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
				time.Sleep(60 * time.Millisecond)
				w.updates <- fakeEntry{key: key}
			}
			w.updates <- nil
		}()
		return w, nil
	}
	keys, err := KVKeys(ctx, kv)
	if err != nil {
		t.Fatalf("KVKeys: %v", err)
	}
	if len(keys) != 6 || calls != 1 {
		t.Fatalf("keys = %v calls = %d, want 6 keys in 1 call", keys, calls)
	}
}

// Abandoning a real watcher mid-stream must not strand its goroutines.
func TestKVKeys_AbandonedAttemptDoesNotLeakGoroutines(t *testing.T) {
	ctx, kv := kvKeysTestBucket(t, "kvkeys_leak")
	for i := 0; i < 2000; i++ {
		if _, err := kv.Put(ctx, fmt.Sprintf("key-%04d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	shrinkKVKeysTimers(t, 100*time.Millisecond, 500*time.Millisecond)
	real := kvWatchAll
	kvWatchAll = func(c context.Context, k jetstream.KeyValue, o ...jetstream.WatchOpt,
	) (jetstream.KeyWatcher, error) {
		w, err := real(c, k, o...)
		if err != nil {
			return nil, err
		}
		return newStallingWatcher(w, 3), nil
	}
	baseline := runtime.NumGoroutine()
	_, err := KVKeys(ctx, kv)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline {
		t.Fatalf("goroutines = %d, baseline %d: abandoned watcher leaked", got, baseline)
	}
}

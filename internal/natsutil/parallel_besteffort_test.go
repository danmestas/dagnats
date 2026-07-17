// natsutil/parallel_besteffort_test.go
// Tests for ParallelGetJSBestEffort: bounded, best-effort concurrent KV
// fetching. Methodology: red-green TDD against a real embedded NATS KV.
// Each test asserts a positive outcome (successful entries returned) and a
// negative property (slow keys skipped without aborting the batch, or a
// caller cancellation aborting early). This is the #523 regression surface:
// unlike the all-or-nothing ParallelGetJS, a single slow/timed-out GET must
// NOT discard every other successfully-fetched entry.
package natsutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func newBestEffortKV(
	t *testing.T, bucket string,
) (jetstream.KeyValue, func()) {
	t.Helper()
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, err := js.CreateKeyValue(
		context.Background(), jetstream.KeyValueConfig{Bucket: bucket},
	)
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	return kv, func() { nc.Close() }
}

func TestParallelGetJSBestEffortReturnsAllOnSuccess(t *testing.T) {
	kv, done := newBestEffortKV(t, "be_success")
	defer done()
	ctx := context.Background()

	keys := make([]string, 12)
	for i := range keys {
		key := fmt.Sprintf("run.%02d", i)
		keys[i] = key
		if _, err := kv.Put(ctx, key,
			[]byte(fmt.Sprintf("val-%02d", i))); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	entries, skipped, err := ParallelGetJSBestEffort(
		ctx, kv, keys, 8, 5*time.Second,
	)
	if err != nil {
		t.Fatalf("ParallelGetJSBestEffort: %v", err)
	}
	// Positive: every key fetched.
	if len(entries) != 12 {
		t.Fatalf("expected 12 entries, got %d", len(entries))
	}
	// Negative: nothing was skipped on the happy path.
	if skipped != 0 {
		t.Fatalf("expected 0 skipped, got %d", skipped)
	}
	if got := string(entries["run.00"]); got != "val-00" {
		t.Errorf("run.00 = %q, want %q", got, "val-00")
	}
}

func TestParallelGetJSBestEffortSkipsTimeoutsWithoutBatchError(t *testing.T) {
	kv, done := newBestEffortKV(t, "be_timeout")
	defer done()
	ctx := context.Background()

	keys := make([]string, 10)
	for i := range keys {
		key := fmt.Sprintf("run.%02d", i)
		keys[i] = key
		if _, err := kv.Put(ctx, key, []byte("data")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	// A 1ns per-key deadline forces every GET to exceed its bound. The
	// all-or-nothing ParallelGetJS returns the first error and discards
	// the batch; best-effort must skip and return (partial, skipped, nil).
	entries, skipped, err := ParallelGetJSBestEffort(
		ctx, kv, keys, 8, time.Nanosecond,
	)
	// Positive: a slow batch is NOT a batch error — the tick can proceed.
	if err != nil {
		t.Fatalf("best-effort must not surface a batch error, got %v", err)
	}
	// Negative: the timed-out keys were counted as skipped, not fetched.
	if skipped != len(keys) {
		t.Fatalf("expected %d skipped, got %d (entries=%d)",
			len(keys), skipped, len(entries))
	}
	if len(entries) != len(keys)-skipped {
		t.Fatalf("entries(%d)+skipped(%d) != total(%d)",
			len(entries), skipped, len(keys))
	}
}

// slowKV wraps a real jetstream.KeyValue and makes designated keys hang
// until their per-key deadline fires, so a test can prove that fast,
// successfully-fetched entries SURVIVE in the same batch as timed-out ones.
type slowKV struct {
	jetstream.KeyValue
	slow  map[string]bool
	delay time.Duration
}

func (s slowKV) Get(
	ctx context.Context, key string,
) (jetstream.KeyValueEntry, error) {
	if s.slow[key] {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.KeyValue.Get(ctx, key)
}

func TestParallelGetJSBestEffortMixedBatchKeepsFastEntries(t *testing.T) {
	kv, done := newBestEffortKV(t, "be_mixed")
	defer done()
	ctx := context.Background()

	fast := []string{"run.f0", "run.f1", "run.f2", "run.f3"}
	slow := []string{"run.s0", "run.s1"}
	all := append(append([]string{}, fast...), slow...)
	for _, key := range all {
		if _, err := kv.Put(ctx, key, []byte("v-"+key)); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	slowSet := map[string]bool{"run.s0": true, "run.s1": true}
	wrapped := slowKV{KeyValue: kv, slow: slowSet, delay: 10 * time.Second}

	// perKeyTimeout well under the 10s stall forces ONLY the slow keys to
	// skip; the fast keys must all come back intact (not clobbered).
	entries, skipped, err := ParallelGetJSBestEffort(
		ctx, wrapped, all, 8, 150*time.Millisecond,
	)
	// Positive: a partial-slow batch is not a batch error.
	if err != nil {
		t.Fatalf("mixed batch must not surface a batch error, got %v", err)
	}
	// Negative: exactly the slow keys were skipped.
	if skipped != len(slow) {
		t.Fatalf("expected %d skipped, got %d", len(slow), skipped)
	}
	if len(entries) != len(fast) {
		t.Fatalf("expected %d fast entries, got %d", len(fast), len(entries))
	}
	// Every fast key survived with its own value — none were clobbered.
	for _, key := range fast {
		got, ok := entries[key]
		if !ok {
			t.Fatalf("fast key %q missing from best-effort result", key)
		}
		if string(got) != "v-"+key {
			t.Errorf("%q = %q, want %q", key, string(got), "v-"+key)
		}
	}
	// The slow keys must be absent, not present-with-stale-data.
	for _, key := range slow {
		if _, ok := entries[key]; ok {
			t.Errorf("timed-out key %q must not appear in result", key)
		}
	}
}

func TestParallelGetJSBestEffortCallerCancelAborts(t *testing.T) {
	kv, done := newBestEffortKV(t, "be_cancel")
	defer done()

	keys := make([]string, 6)
	for i := range keys {
		key := fmt.Sprintf("run.%02d", i)
		keys[i] = key
		if _, err := kv.Put(context.Background(), key,
			[]byte("data")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // caller cancellation must abort, unlike a per-key timeout.

	_, _, err := ParallelGetJSBestEffort(ctx, kv, keys, 8, 5*time.Second)
	// Positive: a caller cancel is a hard abort — it is NOT swallowed.
	if err == nil {
		t.Fatal("expected error on caller-ctx cancel, got nil")
	}
	// Negative: the surfaced error is the cancellation, not a get error.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestParallelGetJSBestEffortEmptyKeys(t *testing.T) {
	kv, done := newBestEffortKV(t, "be_empty")
	defer done()

	entries, skipped, err := ParallelGetJSBestEffort(
		context.Background(), kv, nil, 8, time.Second,
	)
	if err != nil {
		t.Fatalf("ParallelGetJSBestEffort: %v", err)
	}
	// Positive: an empty key set returns a usable empty map.
	if entries == nil {
		t.Fatal("expected non-nil empty map")
	}
	// Negative: nothing fetched, nothing skipped.
	if len(entries) != 0 || skipped != 0 {
		t.Fatalf("expected 0/0, got entries=%d skipped=%d",
			len(entries), skipped)
	}
}

package engine

// Methodology: integration tests for KV-based concurrency limits.
// Each test uses its own embedded NATS server.

import (
	"context"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestConcurrencyAcquireAndRelease(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cm := NewConcurrencyManager(jsNew)

	// Positive: first acquire succeeds
	ok, err := cm.AcquireRun(context.Background(), "wf-1", "run-1", 2)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if !ok {
		t.Fatalf("acquire 1 should succeed")
	}

	// Positive: second acquire (a DIFFERENT run) succeeds (limit 2)
	ok2, err := cm.AcquireRun(context.Background(), "wf-1", "run-2", 2)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if !ok2 {
		t.Fatalf("acquire 2 should succeed")
	}

	// Negative: a third, distinct run fails (at limit)
	ok3, err := cm.AcquireRun(context.Background(), "wf-1", "run-3", 2)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if ok3 {
		t.Fatalf("acquire 3 should fail (limit 2)")
	}

	// Release run-1's slot.
	if err := cm.ReleaseRun(context.Background(), "wf-1", "run-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Positive: run-3 now acquires the slot run-1 freed.
	ok4, err := cm.AcquireRun(context.Background(), "wf-1", "run-3", 2)
	if err != nil {
		t.Fatalf("acquire 4: %v", err)
	}
	if !ok4 {
		t.Fatalf("acquire 4 should succeed after release")
	}

	// Positive: re-acquiring an already-held runID is an idempotent
	// no-op success, not a second slot -- run-2 still holds its own
	// slot from acquire 2 above.
	ok5, err := cm.AcquireRun(context.Background(), "wf-1", "run-2", 2)
	if err != nil {
		t.Fatalf("acquire 5 (re-acquire run-2): %v", err)
	}
	if !ok5 {
		t.Fatalf("re-acquiring an already-held runID should succeed (idempotent)")
	}
}

func TestConcurrencyReleaseWhenZero(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cm := NewConcurrencyManager(jsNew)

	// Positive: release with no prior acquire is safe (never a member).
	err = cm.ReleaseRun(context.Background(), "wf-zero", "run-never-held")
	if err != nil {
		t.Fatalf("release at zero should not error: %v", err)
	}

	// Acquire one, release it, then release again.
	ok, err := cm.AcquireRun(context.Background(), "wf-zero", "run-x", 5)
	if err != nil || !ok {
		t.Fatalf("acquire should succeed: ok=%v err=%v", ok, err)
	}
	if err := cm.ReleaseRun(context.Background(), "wf-zero", "run-x"); err != nil {
		t.Fatalf("release should succeed: %v", err)
	}
	// Positive: second release of the SAME already-released run is
	// safe -- run-x is no longer a member, so this is a no-op
	// (#648 PR review round 3: a replayed release must never act on
	// a run that already isn't holding a slot).
	if err := cm.ReleaseRun(context.Background(), "wf-zero", "run-x"); err != nil {
		t.Fatalf("release at zero should not error: %v", err)
	}
}

// TestConcurrencyReleaseDoesNotStealADifferentRunsSlot is the exact
// scenario from the PR #661 review round-3 BLOCKER: a release replayed
// for a run that already released must never free a DIFFERENT run's
// slot. MaxRuns=1; run1 acquires and releases; run2 acquires the freed
// slot; run1's release is replayed (simulating the reconciler retrying
// a ReleasePending debt whose flag-clear save failed after the first,
// already-successful release); run3 must still be refused while run2
// holds the only slot.
func TestConcurrencyReleaseDoesNotStealADifferentRunsSlot(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cm := NewConcurrencyManager(jsNew)
	ctx := context.Background()
	const wf = "replay-wf"

	ok1, err := cm.AcquireRun(ctx, wf, "run1", 1)
	if err != nil || !ok1 {
		t.Fatalf("run1 acquire: ok=%v err=%v", ok1, err)
	}
	if err := cm.ReleaseRun(ctx, wf, "run1"); err != nil {
		t.Fatalf("run1 release: %v", err)
	}
	ok2, err := cm.AcquireRun(ctx, wf, "run2", 1)
	if err != nil || !ok2 {
		t.Fatalf("run2 acquire: ok=%v err=%v", ok2, err)
	}

	// The replay: run1's release fires AGAIN, long after run2 took
	// the slot run1 vacated.
	if err := cm.ReleaseRun(ctx, wf, "run1"); err != nil {
		t.Fatalf("run1 replayed release: %v", err)
	}

	// Negative: run3 must NOT be admitted -- run2's slot must survive
	// the replay untouched. A bare-counter implementation would have
	// decremented the counter to 0 here and wrongly admitted run3.
	ok3, err := cm.AcquireRun(ctx, wf, "run3", 1)
	if err != nil {
		t.Fatalf("run3 acquire: %v", err)
	}
	if ok3 {
		t.Fatal(
			"run3 was admitted -- run1's replayed release stole " +
				"run2's slot",
		)
	}
}

func TestConcurrencyManagerSafeNoBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	// Do NOT create concurrency_runs bucket.
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Positive: NewConcurrencyManagerSafe returns nil, not panic.
	cm, err := NewConcurrencyManagerSafe(jsNew)
	if cm != nil {
		t.Fatal("expected nil manager when bucket missing")
	}
	// Positive: error is returned.
	if err == nil {
		t.Fatal("expected error when bucket missing")
	}
}

func TestConcurrencyReadMembersNonJSON(t *testing.T) {
	// Methodology: manually write a non-JSON value to the concurrency
	// KV, then acquire. readMembers should treat the unmarshal error
	// gracefully -- same code path the legacy plain-integer counter
	// format falls into (see TestConcurrencyLegacyCounterMigrates).
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, _ := jsNew.KeyValue(
		context.Background(), "concurrency_runs",
	)

	// Write non-JSON value directly.
	mustPutJS(t, context.Background(), kv,
		"workflow.bad-value", []byte("not-json"))

	cm := NewConcurrencyManager(jsNew)

	// Positive: acquire treats the corrupt value as an empty member
	// set and succeeds. readMembers returns (nil, rev, nil) when
	// Unmarshal fails.
	ok, err := cm.AcquireRun(context.Background(), "bad-value", "run-1", 2)
	if err != nil {
		t.Fatalf("acquire with corrupt value: %v", err)
	}
	if !ok {
		t.Fatal("acquire should succeed on corrupt value")
	}

	// Positive: release on the same workflow is safe.
	if err := cm.ReleaseRun(context.Background(), "bad-value", "run-1"); err != nil {
		t.Fatalf("release with corrupt value: %v", err)
	}
}

// TestConcurrencyLegacyCounterMigrates documents and verifies the
// migration decision for #648 PR review round-3: a legacy plain-
// integer counter value (the format this ConcurrencyManager used
// before member sets) is treated as an empty member set on first
// touch, and the very next CAS write replaces it with the new JSON
// shape in place -- there is no separate migration step or flag.
//
// This is a deliberate, documented one-time under-count: a bare
// integer has no record of WHICH runs it was counting, so an
// in-flight workflow may briefly admit more concurrent runs than its
// limit until the runs the old counter was tracking finish (seen
// here as run-2 succeeding with limit=1, right after run-1 already
// occupies the set the legacy "1" never named). See readMembers's doc
// comment for the full reasoning.
func TestConcurrencyLegacyCounterMigrates(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, _ := jsNew.KeyValue(
		context.Background(), "concurrency_runs",
	)

	// Write the LEGACY plain-integer format directly, simulating a
	// pre-upgrade snapshot with one run already counted.
	mustPutJS(t, context.Background(), kv,
		"workflow.legacy-wf", []byte("1"))

	cm := NewConcurrencyManager(jsNew)

	// Positive: the legacy value is treated as an empty set, so
	// run-1 acquires successfully even though the old counter said
	// "1" was already held (the under-count this migration accepts).
	ok1, err := cm.AcquireRun(context.Background(), "legacy-wf", "run-1", 1)
	if err != nil || !ok1 {
		t.Fatalf("run-1 acquire against legacy value: ok=%v err=%v", ok1, err)
	}

	// Positive: the key is now migrated to the JSON member-set shape
	// -- the raw KV value is no longer the bare integer.
	entry, getErr := kv.Get(context.Background(), "workflow.legacy-wf")
	if getErr != nil {
		t.Fatalf("get migrated key: %v", getErr)
	}
	if string(entry.Value()) == "1" {
		t.Fatal("key was not migrated off the legacy plain-integer format")
	}

	// Positive: going forward, the limit is enforced normally against
	// the now-correct member set -- run-2 is refused at limit=1.
	ok2, err := cm.AcquireRun(context.Background(), "legacy-wf", "run-2", 1)
	if err != nil {
		t.Fatalf("run-2 acquire: %v", err)
	}
	if ok2 {
		t.Fatal("run-2 should be refused -- limit=1 already held by run-1")
	}
}

// TestConcurrencyLegacyValueWarnsOnceAndCounts is the PR #661 review
// round-4 fix: a limiter silently resetting itself (the legacy-value
// fallback readMembers takes -- see TestConcurrencyLegacyCounterMigrates)
// must be visible, not just safe. Reading the SAME still-legacy key
// twice (before anything migrates it) must log the WARN only once
// (bounded per-key dedup) even though the counter increments on every
// occurrence.
func TestConcurrencyLegacyValueWarnsOnceAndCounts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, _ := jsNew.KeyValue(
		context.Background(), "concurrency_runs",
	)
	// A unique key per test run avoids collisions with the
	// process-lifetime warned-keys dedup set other tests populate.
	key := "workflow.dedup-wf-" + t.Name()
	mustPutJS(t, context.Background(), kv, key, []byte("3"))

	cm := NewConcurrencyManager(jsNew)

	buf, restore := captureSlog(t)
	defer restore()

	// Two reads of the SAME still-legacy value (readMembers is
	// read-only -- neither call migrates the key).
	if _, _, err := cm.readMembers(context.Background(), key); err != nil {
		t.Fatalf("readMembers 1: %v", err)
	}
	if _, _, err := cm.readMembers(context.Background(), key); err != nil {
		t.Fatalf("readMembers 2: %v", err)
	}

	logs := buf.String()
	occurrences := strings.Count(logs, "stored value was not a member set")
	if occurrences != 1 {
		t.Fatalf(
			"legacy-reset WARN logged %d times for the same key, want exactly 1 (bounded dedup): %s",
			occurrences, logs,
		)
	}
	if !strings.Contains(logs, key) {
		t.Fatalf("legacy-reset WARN did not name the key %q: %s", key, logs)
	}
}

func TestConcurrencyUnlimitedWhenZero(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cm := NewConcurrencyManager(jsNew)

	// Positive: limit 0 means unlimited
	ok, err := cm.AcquireRun(context.Background(), "wf-2", "run-1", 0)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !ok {
		t.Fatalf("limit 0 should always succeed")
	}
}

func TestTaskConcurrencyAcquireAndRelease(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
			natsutil.KVConfig{Bucket: "concurrency_tasks"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cm := NewConcurrencyManager(jsNew)

	// Positive: first acquire under limit succeeds
	ok, err := cm.AcquireTask(context.Background(), "call-claude", 2)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if !ok {
		t.Fatal("acquire 1 should succeed")
	}

	// Positive: second acquire under limit succeeds
	ok2, err := cm.AcquireTask(context.Background(), "call-claude", 2)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if !ok2 {
		t.Fatal("acquire 2 should succeed")
	}

	// Negative: third acquire at limit fails
	ok3, err := cm.AcquireTask(context.Background(), "call-claude", 2)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if ok3 {
		t.Fatal("acquire 3 should fail (at limit)")
	}

	// Release one and retry
	if err := cm.ReleaseTask(context.Background(), "call-claude"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Positive: acquire succeeds after release
	ok4, err := cm.AcquireTask(context.Background(), "call-claude", 2)
	if err != nil {
		t.Fatalf("acquire 4: %v", err)
	}
	if !ok4 {
		t.Fatal("acquire 4 should succeed after release")
	}
}

func TestTaskConcurrencyReleaseAtZero(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
			natsutil.KVConfig{Bucket: "concurrency_tasks"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cm := NewConcurrencyManager(jsNew)

	// Positive: release with no prior acquire is safe
	err = cm.ReleaseTask(context.Background(), "no-prior")
	if err != nil {
		t.Fatalf("release at zero should not error: %v", err)
	}

	// Acquire one, release it, release again
	ok, err := cm.AcquireTask(context.Background(), "no-prior", 5)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if err := cm.ReleaseTask(context.Background(), "no-prior"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// Positive: double release at zero is safe
	if err := cm.ReleaseTask(context.Background(), "no-prior"); err != nil {
		t.Fatalf("release at zero: %v", err)
	}
}

func TestTaskConcurrencyUnlimitedWhenZero(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
			natsutil.KVConfig{Bucket: "concurrency_tasks"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cm := NewConcurrencyManager(jsNew)

	// Positive: limit 0 means unlimited
	ok, err := cm.AcquireTask(context.Background(), "any-task", 0)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !ok {
		t.Fatal("limit 0 should always succeed")
	}
}

func TestTaskConcurrencyNoTaskBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	// Safe variant — tasks bucket missing
	cm, err := NewConcurrencyManagerSafe(jsNew)
	if err != nil {
		t.Fatalf("safe constructor: %v", err)
	}
	if cm == nil {
		t.Fatal("cm should not be nil when runs bucket exists")
	}

	// Positive: acquire succeeds even without task bucket
	ok, err := cm.AcquireTask(context.Background(), "call-claude", 2)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !ok {
		t.Fatal("acquire should succeed without task bucket")
	}

	// Positive: release is no-op without task bucket
	if err := cm.ReleaseTask(context.Background(), "call-claude"); err != nil {
		t.Fatalf("release: %v", err)
	}
}

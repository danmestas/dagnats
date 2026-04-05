package engine

// Methodology: integration tests for KV-based concurrency limits.
// Each test uses its own embedded NATS server.

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
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

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: first acquire succeeds
	ok, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if !ok {
		t.Fatalf("acquire 1 should succeed")
	}

	// Positive: second acquire succeeds (limit 2)
	ok2, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if !ok2 {
		t.Fatalf("acquire 2 should succeed")
	}

	// Negative: third acquire fails (at limit)
	ok3, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if ok3 {
		t.Fatalf("acquire 3 should fail (limit 2)")
	}

	// Release one
	if err := cm.ReleaseRun("wf-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Positive: acquire succeeds again after release
	ok4, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 4: %v", err)
	}
	if !ok4 {
		t.Fatalf("acquire 4 should succeed after release")
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

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: release with no prior acquire is safe (already 0).
	err := cm.ReleaseRun("wf-zero")
	if err != nil {
		t.Fatalf("release at zero should not error: %v", err)
	}

	// Acquire one, release it, then release again.
	ok, err := cm.AcquireRun("wf-zero", 5)
	if err != nil || !ok {
		t.Fatalf("acquire should succeed: ok=%v err=%v", ok, err)
	}
	if err := cm.ReleaseRun("wf-zero"); err != nil {
		t.Fatalf("release should succeed: %v", err)
	}
	// Positive: second release when counter is 0 is safe.
	if err := cm.ReleaseRun("wf-zero"); err != nil {
		t.Fatalf("release at zero should not error: %v", err)
	}
}

func TestConcurrencyManagerSafeNoBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	// Do NOT create concurrency_runs bucket.
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()

	// Positive: NewConcurrencyManagerSafe returns nil, not panic.
	cm, err := NewConcurrencyManagerSafe(js)
	if cm != nil {
		t.Fatal("expected nil manager when bucket missing")
	}
	// Positive: error is returned.
	if err == nil {
		t.Fatal("expected error when bucket missing")
	}
}

func TestConcurrencyReadCounterNonNumeric(t *testing.T) {
	// Methodology: manually write a non-numeric value to the
	// concurrency KV, then acquire. readCounter should treat
	// the parse error gracefully.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	kv, _ := js.KeyValue("concurrency_runs")

	// Write non-numeric value directly.
	kv.Put("workflow.bad-counter", []byte("not-a-number"))

	cm := NewConcurrencyManager(js)

	// Positive: acquire treats corrupted counter as 0 and
	// succeeds. The readCounter returns (0, rev, nil) when
	// Atoi fails.
	ok, err := cm.AcquireRun("bad-counter", 2)
	if err != nil {
		t.Fatalf("acquire with corrupt counter: %v", err)
	}
	if !ok {
		t.Fatal("acquire should succeed on corrupt counter")
	}

	// Positive: release on the same workflow is safe.
	if err := cm.ReleaseRun("bad-counter"); err != nil {
		t.Fatalf("release with corrupt counter: %v", err)
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

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: limit 0 means unlimited
	ok, err := cm.AcquireRun("wf-2", 0)
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

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: first acquire under limit succeeds
	ok, err := cm.AcquireTask("call-claude", 2)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if !ok {
		t.Fatal("acquire 1 should succeed")
	}

	// Positive: second acquire under limit succeeds
	ok2, err := cm.AcquireTask("call-claude", 2)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if !ok2 {
		t.Fatal("acquire 2 should succeed")
	}

	// Negative: third acquire at limit fails
	ok3, err := cm.AcquireTask("call-claude", 2)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if ok3 {
		t.Fatal("acquire 3 should fail (at limit)")
	}

	// Release one and retry
	if err := cm.ReleaseTask("call-claude"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Positive: acquire succeeds after release
	ok4, err := cm.AcquireTask("call-claude", 2)
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

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: release with no prior acquire is safe
	err := cm.ReleaseTask("no-prior")
	if err != nil {
		t.Fatalf("release at zero should not error: %v", err)
	}

	// Acquire one, release it, release again
	ok, err := cm.AcquireTask("no-prior", 5)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if err := cm.ReleaseTask("no-prior"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// Positive: double release at zero is safe
	if err := cm.ReleaseTask("no-prior"); err != nil {
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

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: limit 0 means unlimited
	ok, err := cm.AcquireTask("any-task", 0)
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

	js, _ := nc.JetStream()
	// Safe variant — tasks bucket missing
	cm, err := NewConcurrencyManagerSafe(js)
	if err != nil {
		t.Fatalf("safe constructor: %v", err)
	}
	if cm == nil {
		t.Fatal("cm should not be nil when runs bucket exists")
	}

	// Positive: acquire succeeds even without task bucket
	ok, err := cm.AcquireTask("call-claude", 2)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !ok {
		t.Fatal("acquire should succeed without task bucket")
	}

	// Positive: release is no-op without task bucket
	if err := cm.ReleaseTask("call-claude"); err != nil {
		t.Fatalf("release: %v", err)
	}
}

package engine

// Methodology: integration tests for KV-based concurrency limits.
// Each test uses its own embedded NATS server.

import (
	"testing"

	"github.com/danmestas/dagnats/natsutil"
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

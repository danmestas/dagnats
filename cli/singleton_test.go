// cli/singleton_test.go
// Tests for singleton list and release commands. Methodology:
// integration tests with embedded NATS — populate singleton_locks
// KV, verify list and release behavior.
package cli

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestListSingletonLocks_Empty(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	locks := listSingletonLocks(js, "")

	// Positive: returns empty slice.
	if len(locks) != 0 {
		t.Fatalf("expected 0 locks, got %d", len(locks))
	}

	// Negative: no panic on empty bucket.
}

func TestListSingletonLocks_WithEntries(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, "singleton_locks")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}

	if _, err := kv.Put(
		ctx, "deploy", []byte(`{"run_id":"run-1"}`),
	); err != nil {
		t.Fatalf("Put deploy: %v", err)
	}
	if _, err := kv.Put(
		ctx, "sync.user-42",
		[]byte(`{"run_id":"run-2"}`),
	); err != nil {
		t.Fatalf("Put sync: %v", err)
	}

	// Positive: lists all locks.
	locks := listSingletonLocks(js, "")
	if len(locks) != 2 {
		t.Fatalf("expected 2 locks, got %d", len(locks))
	}

	// Positive: filter by workflow.
	filtered := listSingletonLocks(js, "sync")
	if len(filtered) != 1 {
		t.Fatalf("expected 1 filtered lock, got %d",
			len(filtered))
	}
	if filtered[0].Key != "sync.user-42" {
		t.Fatalf("expected key sync.user-42, got %s",
			filtered[0].Key)
	}
	if filtered[0].RunID != "run-2" {
		t.Fatalf("expected run-2, got %s", filtered[0].RunID)
	}

	// Negative: filter with no match.
	none := listSingletonLocks(js, "nonexistent")
	if len(none) != 0 {
		t.Fatalf("expected 0 locks, got %d", len(none))
	}
}

func TestReleaseSingletonLock(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, "singleton_locks")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if _, err := kv.Put(
		ctx, "deploy", []byte(`{"run_id":"run-1"}`),
	); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Positive: release existing lock succeeds.
	released := releaseSingletonLock(js, "deploy")
	if !released {
		t.Fatal("expected release to succeed")
	}

	// Positive: lock is gone.
	_, err = kv.Get(ctx, "deploy")
	if err == nil {
		t.Fatal("lock should be deleted after release")
	}

	// Negative: release on already-deleted key still succeeds
	// (NATS KV Delete is idempotent via tombstones).
	released2 := releaseSingletonLock(js, "deploy")
	if !released2 {
		t.Fatal("re-release should succeed (idempotent)")
	}
}

func TestKeyMatchesWorkflow(t *testing.T) {
	// Positive: exact match.
	if !keyMatchesWorkflow("deploy", "deploy") {
		t.Fatal("exact match should return true")
	}

	// Positive: prefix match with entity key.
	if !keyMatchesWorkflow("deploy.production", "deploy") {
		t.Fatal("prefix match should return true")
	}

	// Negative: different workflow.
	if keyMatchesWorkflow("sync.user-42", "deploy") {
		t.Fatal("different workflow should return false")
	}

	// Negative: partial name match is not a match.
	if keyMatchesWorkflow("deploy-v2", "deploy") {
		t.Fatal("partial name match should return false")
	}
}

func TestParseLockEntry(t *testing.T) {
	// Positive: parses valid JSON.
	lock := parseLockEntry(
		"deploy", []byte(`{"run_id":"abc"}`),
	)
	if lock.Key != "deploy" {
		t.Fatalf("expected key deploy, got %s", lock.Key)
	}
	if lock.RunID != "abc" {
		t.Fatalf("expected run_id abc, got %s", lock.RunID)
	}

	// Negative: invalid JSON doesn't panic.
	lock2 := parseLockEntry("deploy", []byte("not-json"))
	if lock2.RunID != "" {
		t.Fatal("expected empty run_id for invalid JSON")
	}
}

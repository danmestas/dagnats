// engine/prune_ticker_test.go
// Tests for the orchestrator-hosted run-retention prune ticker (#453).
// Methodology: real embedded NATS server, one per test. The prune interval
// is lowered via the test-injection var so the background pass fires quickly
// without sleeping for the production 10m default. Asserts BOTH that an
// age window deletes an old terminal run AND that a live run survives; that
// with no age window an old run that fits its share survives; and that size
// pruning, always on, trims an over-share bucket to its oldest-first
// low-water mark under a tiny store budget.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPruneTicker_EnabledDropsOldTerminalKeepsLive(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := NewSnapshotStore(jsNew)

	old := terminalRun("old-terminal", 48*time.Hour)
	live := runningRun("live-running", 200*time.Hour)
	saveAll(t, store, old, live)

	prev := prunePassInterval
	prunePassInterval = 50 * time.Millisecond
	defer func() { prunePassInterval = prev }()

	orch := NewOrchestrator(nc, WithRunsMaxAge(24*time.Hour))
	orch.Start()
	defer orch.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !exists(t, store, "old-terminal") {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if exists(t, store, "old-terminal") {
		t.Fatal("enabled sweeper should have pruned old-terminal")
	}
	if !exists(t, store, "live-running") {
		t.Fatal("enabled sweeper must NOT delete a live run")
	}
}

// With runs_max_age unset there is no age pruning: a run far older than 30
// days survives while the bucket fits its share of the store budget.
func TestPruneTicker_AgeUnsetKeepsOldRunThatFits(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := NewSnapshotStore(jsNew)
	saveAll(t, store, terminalRun("old-terminal", 1000*time.Hour))

	prev := prunePassInterval
	prunePassInterval = 50 * time.Millisecond
	defer func() { prunePassInterval = prev }()

	// No WithRunsMaxAge: size pruning only, against the server budget.
	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	time.Sleep(300 * time.Millisecond)

	count, err := store.CountAll(context.Background())
	if err != nil {
		t.Fatalf("CountAll failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1 (no age pruning when unset)", count)
	}
	if !exists(t, store, "old-terminal") {
		t.Fatal("a run that fits its share was pruned with runs_max_age unset")
	}
}

// Size pruning is always on: under a tiny store budget the ticker deletes
// the oldest terminal runs until workflow_runs fits its share, with no age
// window configured, and never touches a live run.
func TestPruneTicker_SizePruningWithTinyBudget(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := NewSnapshotStore(jsNew)
	ids := seedSizedRuns(t, store, 5)
	saveAll(t, store, runningRun("live-running", 200*time.Hour))

	prev := prunePassInterval
	prunePassInterval = 50 * time.Millisecond
	defer func() { prunePassInterval = prev }()

	const budget = 100 << 10 // share = 8% of 100 KiB, about one run
	orch := NewOrchestrator(nc, WithStoreBudget(budget))
	orch.Start()
	defer orch.Stop()

	newest := ids[len(ids)-1]
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && exists(t, store, ids[0]) {
		time.Sleep(25 * time.Millisecond)
	}
	if exists(t, store, ids[0]) {
		t.Fatal("size pruning should have dropped the oldest terminal run")
	}
	if !exists(t, store, newest) {
		t.Fatal("size pruning must keep the newest terminal run")
	}
	if !exists(t, store, "live-running") {
		t.Fatal("size pruning must NOT delete a live run")
	}
}

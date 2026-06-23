// engine/prune_ticker_test.go
// Tests for the orchestrator-hosted run-retention prune ticker (#453).
// Methodology: real embedded NATS server, one per test. The prune interval
// is lowered via the test-injection var so the background pass fires quickly
// without sleeping for the production 10m default. Asserts BOTH that an
// enabled sweeper deletes an old terminal run AND that a live run survives;
// and (invariant 3) that the disabled default never starts the ticker.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPruneTicker_EnabledDropsOldTerminalKeepsLive(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
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

// Invariant 3: disabled by default — the ticker never runs, nothing is pruned.
func TestPruneTicker_DisabledByDefaultPrunesNothing(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
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

	// No WithRunsMaxAge → RunsMaxAge stays zero → disabled.
	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	time.Sleep(300 * time.Millisecond)

	count, err := store.CountAll(context.Background())
	if err != nil {
		t.Fatalf("CountAll failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1 (disabled sweeper must not prune)", count)
	}
	if !exists(t, store, "old-terminal") {
		t.Fatal("disabled sweeper deleted a run — safety invariant 3 violated")
	}
}

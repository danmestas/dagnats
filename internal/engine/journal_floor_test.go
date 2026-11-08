// engine/journal_floor_test.go
// The engine publishes AGENT_JOURNAL's protected floor over the runs
// its runactive index holds: a live run's agent step pins the journal
// from where its next Open reads, a terminal run (no runactive marker)
// pins nothing, and only the node chosen as the writer publishes.
//
// Methodology: one embedded server per test. Journals are written
// through journal.Open/Append/Commit; liveness is the real runactive
// marker (createActiveEntry / deleteActiveEntry). A pass runs with an
// injected clock, and the result is read back from the stream's
// metadata by its literal key. Each test asserts both the published
// floor and what must not be published.
package engine

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go/jetstream"
)

const floorTestTimeout = 10 * time.Second

// floorFixture is an orchestrator (not started) whose writer flag the
// test flips, with the journal stream provisioned.
type floorFixture struct {
	orch   *Orchestrator
	js     jetstream.JetStream
	pub    *natsutil.TracingPublisher
	writer *atomic.Bool
}

func newFloorFixture(t *testing.T) floorFixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	writer := &atomic.Bool{}
	orch := NewOrchestrator(nc, WithJournalFloorWriter(writer.Load))
	return floorFixture{orch: orch, js: js, pub: natsutil.NewTracingPublisher(nc, js), writer: writer}
}

// iterate writes iterations 0..n-1 of runID's step "agent" as a claim
// and a continue commit each, then claims iteration n. It returns the
// commit of iteration n-1, where the next Open of iteration n reads
// from.
func (f floorFixture) iterate(t *testing.T, runID string, n int) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), floorTestTimeout)
	defer cancel()
	var commit uint64
	for iteration := 0; iteration <= n; iteration++ {
		j, err := journal.Open(ctx, f.js, f.pub, journal.Task{
			RunID: runID, StepID: "agent", Iteration: iteration, Attempt: 1,
			DispatchNonce: "n-" + runID, Delivered: 1, StreamSeq: 7,
		})
		if err != nil {
			t.Fatalf("Open %s/%d: %v", runID, iteration, err)
		}
		if iteration == n {
			break
		}
		commit, err = j.Commit(ctx, journal.Commit{Action: journal.ActionContinue, State: []byte(`1`)})
		if err != nil {
			t.Fatalf("Commit %s/%d: %v", runID, iteration, err)
		}
	}
	return commit
}

func (f floorFixture) live(t *testing.T, runID string) {
	t.Helper()
	if err := f.orch.store.createActiveEntry(context.Background(), runID); err != nil {
		t.Fatalf("createActiveEntry %s: %v", runID, err)
	}
}

func (f floorFixture) pass(t *testing.T, now time.Time) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), floorTestTimeout)
	defer cancel()
	if err := f.orch.publishJournalFloor(ctx, now); err != nil {
		t.Fatalf("publishJournalFloor: %v", err)
	}
	stream, err := f.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	return stream.CachedInfo().Config.Metadata
}

func TestJournalFloor_LiveRunsPinTerminalRunsDoNot(t *testing.T) {
	f := newFloorFixture(t)
	f.writer.Store(true)
	f.iterate(t, "done", 3)
	liveA := f.iterate(t, "live-a", 2)
	liveB := f.iterate(t, "live-b", 2)
	f.live(t, "live-a")
	f.live(t, "live-b")
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	meta := f.pass(t, now)
	if got := meta["dagnats.protected_floor"]; got != strconv.FormatUint(liveA, 10) {
		t.Fatalf("floor = %q, want live-a's %d (live-b's is %d)", got, liveA, liveB)
	}
	if meta["dagnats.protected_floor_at"] != "2026-11-08T12:00:00Z" {
		t.Fatalf("floor_at = %q", meta["dagnats.protected_floor_at"])
	}
	// live-a finishes: its marker goes, and the floor moves up to
	// live-b's once a minute has passed.
	f.orch.store.deleteActiveEntry(context.Background(), "live-a")
	meta = f.pass(t, now.Add(2*time.Minute))
	if got := meta["dagnats.protected_floor"]; got != strconv.FormatUint(liveB, 10) {
		t.Fatalf("floor after live-a ended = %q, want live-b's %d", got, liveB)
	}
}

func TestJournalFloor_NoLiveRunsProtectsNothing(t *testing.T) {
	f := newFloorFixture(t)
	f.writer.Store(true)
	f.iterate(t, "done", 2)
	ctx, cancel := context.WithTimeout(context.Background(), floorTestTimeout)
	defer cancel()
	stream, err := f.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	last := stream.CachedInfo().State.LastSeq
	meta := f.pass(t, time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC))
	if got := meta[natsutil.MetaProtectedFloor]; got != strconv.FormatUint(last+1, 10) {
		t.Fatalf("floor = %q, want LastSeq+1 = %d", got, last+1)
	}
	if last == 0 {
		t.Fatal("the terminal run's journal must have records")
	}
}

func TestJournalFloor_OnlyTheWriterPublishes(t *testing.T) {
	f := newFloorFixture(t)
	f.iterate(t, "live", 1)
	f.live(t, "live")
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if meta := f.pass(t, now); meta[natsutil.MetaProtectedFloor] != "" {
		t.Fatalf("a node that is not the writer published %v", meta)
	}
	f.writer.Store(true)
	if meta := f.pass(t, now); meta[natsutil.MetaProtectedFloor] == "" {
		t.Fatalf("the writer did not publish: %v", meta)
	}
}

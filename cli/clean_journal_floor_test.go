// cli/clean_journal_floor_test.go
// Pins #1030: dagnats clean never purges AGENT_JOURNAL at or above its
// protected floor, the contract every archiver of the stream keeps. Each
// bounded purge (--older-than, the --max-keep ceiling, --keep and
// --before-seq) is capped at a fresh floor, and with a missing or stale
// floor the journal is skipped.
//
// Methodology: an embedded server with every stream provisioned. Ten
// journal records are published and a floor is written as stream
// metadata. Each purge path runs, and the stream's first sequence shows
// what was purged. Negative space: WORKFLOW_HISTORY, which has no floor,
// is purged by the same call, so the cap is a guard, not a dead path.
package cli

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// journalFloorFixture provisions the streams, publishes ten journal
// records and five history records, and sets the journal's floor
// metadata as given (floor 0 leaves it unset).
func journalFloorFixture(t *testing.T, floor uint64, floorAt time.Time) jetstream.JetStream {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := js.Publish(ctx, "journal.run-1.step-1", []byte("r")); err != nil {
			t.Fatalf("publish journal: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := js.Publish(ctx, "history.floor-test", []byte("h")); err != nil {
			t.Fatalf("publish history: %v", err)
		}
	}
	if floor > 0 {
		stream, err := js.Stream(ctx, natsutil.AgentJournalStream)
		if err != nil {
			t.Fatalf("journal stream: %v", err)
		}
		cfg := stream.CachedInfo().Config
		cfg.Metadata = map[string]string{
			natsutil.MetaProtectedFloor:   strconv.FormatUint(floor, 10),
			natsutil.MetaProtectedFloorAt: floorAt.UTC().Format(time.RFC3339),
		}
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			t.Fatalf("set floor: %v", err)
		}
	}
	return js
}

// firstSeqAndMsgs reports a stream's first sequence and message count.
func firstSeqAndMsgs(t *testing.T, js jetstream.JetStream, name string) (uint64, uint64) {
	t.Helper()
	stream, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatalf("stream %s: %v", name, err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatalf("info %s: %v", name, err)
	}
	return info.State.FirstSeq, info.State.Msgs
}

func TestCleanOlderThanStopsAtTheJournalFloor(t *testing.T) {
	js := journalFloorFixture(t, 4, time.Now())
	time.Sleep(50 * time.Millisecond)
	streams, _ := collectTargets([]string{"runs"})
	purgeStreams(context.Background(), js, streams, time.Millisecond)

	if first, msgs := firstSeqAndMsgs(t, js, natsutil.AgentJournalStream); first != 4 || msgs != 7 {
		t.Fatalf("journal first seq %d, %d msgs; want 4 and 7: nothing at or above the floor goes", first, msgs)
	}
	if _, msgs := firstSeqAndMsgs(t, js, "WORKFLOW_HISTORY"); msgs != 0 {
		t.Fatalf("history kept %d msgs; the same purge must still clear it", msgs)
	}
}

func TestCleanSkipsTheJournalWithAStaleOrMissingFloor(t *testing.T) {
	for name, js := range map[string]jetstream.JetStream{
		"stale":   journalFloorFixture(t, 4, time.Now().Add(-natsutil.ProtectedFloorStaleAfter-time.Minute)),
		"missing": journalFloorFixture(t, 0, time.Time{}),
	} {
		time.Sleep(50 * time.Millisecond)
		streams, _ := collectTargets([]string{"runs"})
		purgeStreams(context.Background(), js, streams, time.Millisecond)
		if _, msgs := firstSeqAndMsgs(t, js, natsutil.AgentJournalStream); msgs != 10 {
			t.Fatalf("%s floor: journal kept %d of 10; with no usable floor it is skipped", name, msgs)
		}
	}
}

func TestCleanKeepAndBeforeSeqStopAtTheJournalFloor(t *testing.T) {
	js := journalFloorFixture(t, 4, time.Now())
	streams, buckets := collectTargets([]string{"runs"})
	executeSeqPurge(context.Background(), js, streams, buckets, 1, 0)
	if first, msgs := firstSeqAndMsgs(t, js, natsutil.AgentJournalStream); first != 4 || msgs != 7 {
		t.Fatalf("--keep 1: journal first seq %d, %d msgs; want 4 and 7", first, msgs)
	}
	if _, msgs := firstSeqAndMsgs(t, js, "WORKFLOW_HISTORY"); msgs != 1 {
		t.Fatalf("--keep 1: history kept %d msgs, want 1", msgs)
	}

	js = journalFloorFixture(t, 4, time.Now())
	executeSeqPurge(context.Background(), js, streams, buckets, 0, 9)
	if first, _ := firstSeqAndMsgs(t, js, natsutil.AgentJournalStream); first != 4 {
		t.Fatalf("--before-seq 9: journal first seq %d, want the floor 4", first)
	}
	js = journalFloorFixture(t, 6, time.Now())
	executeSeqPurge(context.Background(), js, streams, buckets, 0, 3)
	if first, _ := firstSeqAndMsgs(t, js, natsutil.AgentJournalStream); first != 3 {
		t.Fatalf("--before-seq 3 under floor 6: journal first seq %d, want 3", first)
	}
}

func TestCleanCeilingStopsAtTheJournalFloor(t *testing.T) {
	js := journalFloorFixture(t, 4, time.Now())
	enforceRunCeiling(context.Background(), js, []string{"runs"}, 2)
	if first, msgs := firstSeqAndMsgs(t, js, natsutil.AgentJournalStream); first != 4 || msgs != 7 {
		t.Fatalf("--max-keep 2: journal first seq %d, %d msgs; want 4 and 7", first, msgs)
	}
	if _, msgs := firstSeqAndMsgs(t, js, "WORKFLOW_HISTORY"); msgs != 2 {
		t.Fatalf("--max-keep 2: history kept %d msgs, want 2", msgs)
	}
}

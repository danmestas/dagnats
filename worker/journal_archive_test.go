// worker/journal_archive_test.go
// Endless JetStream for a Go worker: tc.Journal() on a step whose
// journal was partly purged from AGENT_JOURNAL replays the purged
// records from the archive (the earlier iteration's committed state and
// this iteration's records), and fails with journal.ErrArchive when no
// archive answers, instead of replaying with records missing.
//
// Methodology: a taskContext built as in journal_test.go against an
// embedded server. Iteration 0 commits "s0"; a crashed holder of
// iteration 1 leaves a claim and an entry; the stream is snapshotted and
// purged below a later record, and the fake archive of
// internal/archivetest serves the snapshot.
package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/danmestas/dagnats/internal/archivetest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// purgedWorkerJournal writes the two iterations, purges everything but
// the last record, and returns the archive starter.
func purgedWorkerJournal(
	t *testing.T, runID string,
) (*nats.Conn, func() *archivetest.Responder) {
	t.Helper()
	nc, js := journalServer(t)
	pub := natsutil.NewTracingPublisher(nc, js)
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	first, err := journal.Open(ctx, js, pub, journal.Task{
		RunID: runID, StepID: "s", Attempt: 1, DispatchNonce: "nonce", Delivered: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.Commit(ctx, journal.Commit{
		Action: journal.ActionContinue, State: []byte("s0"),
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	crashed, err := journal.Open(ctx, js, pub, journal.Task{
		RunID: runID, StepID: "s", Iteration: 1, Attempt: 1, DispatchNonce: "nonce",
		Delivered: 1,
	})
	if err != nil {
		t.Fatalf("Open iteration 1: %v", err)
	}
	if _, err := crashed.Append(ctx, journal.TypeEntry, []byte(`{"e":1}`)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cut, err := crashed.Append(ctx, journal.TypeEntry, []byte(`{"e":2}`))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return nc, purgeBelow(t, nc, js, cut)
}

func purgeBelow(
	t *testing.T, nc *nats.Conn, js jetstream.JetStream, cut uint64,
) func() *archivetest.Responder {
	t.Helper()
	before := archivetest.Snapshot(t, js, natsutil.AgentJournalStream, cut)
	epoch, first := archivetest.PurgeBelow(t, js, natsutil.AgentJournalStream, cut)
	if first != cut || len(before) == 0 {
		t.Fatalf("purge left first seq %d with %d archived, want %d", first, len(before), cut)
	}
	return func() *archivetest.Responder {
		return archivetest.Serve(t, nc, "", natsutil.AgentJournalStream, epoch, before)
	}
}

func TestJournal_GoWorkerReplaysPurgedRecordsFromArchive(t *testing.T) {
	nc, serve := purgedWorkerJournal(t, "run-jarch")
	fake := serve()
	tc := delivery(t, nc, journalPayload("run-jarch", 1), 2)
	j, err := tc.Journal()
	if err != nil {
		t.Fatalf("Journal: %v", err)
	}
	records := j.Records()
	if len(records) != 3 || records[0].Type != journal.TypeClaim ||
		string(records[1].Data) != `{"e":1}` || string(records[2].Data) != `{"e":2}` {
		t.Fatalf("records = %+v, want the crashed claim and both entries", records)
	}
	state, err := tc.LoadCheckpoint()
	if err != nil || string(state) != "s0" {
		t.Errorf("LoadCheckpoint = %q, %v; want the archived commit's s0", state, err)
	}
	if fake.Requests.Load() == 0 {
		t.Error("the purged records were not read from the archive")
	}
}

func TestJournal_GoWorkerFailsClosedWithoutArchive(t *testing.T) {
	nc, _ := purgedWorkerJournal(t, "run-jarch-none")
	tc := delivery(t, nc, journalPayload("run-jarch-none", 1), 2)
	j, err := tc.Journal()
	if !errors.Is(err, journal.ErrArchive) || j != nil {
		t.Fatalf("Journal with no archive = %v, %v; want ErrArchive", j, err)
	}
}

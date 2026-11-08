// journal/archive_purged_test.go
// Endless JetStream, the fully purged step: when JetStream holds no
// record of a step at all, no live record is left to name a purged
// predecessor. A read that knows the step had history (a later
// iteration, a retry, a fork's parent) asks the archive instead and
// replays from it, and the next write names the archived tail so later
// reads find the chain again. If the archive holds nothing for a step
// that must have had history, the read fails instead of replaying from
// nothing. A brand-new step's first open never asks the archive.
//
// Methodology: one embedded server per case. The step's records are
// written, another run's record lands after them, and the stream is
// purged below that record, so FirstSeq is past every record of the
// step. The fake archive of internal/archivetest serves the snapshot.
package journal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/internal/archivetest"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// purgeAllOf lands one record of another run after everything written
// so far, then purges below it: every earlier record is archived only.
func purgeAllOf(t *testing.T, c client, p purgedStep) purgedStep {
	t.Helper()
	tail := open(t, c, task(p.runID+"late", 0, 1, 1)).Seq()
	p.before = append(p.before, archivetest.Snapshot(t, c.js, natsutil.AgentJournalStream, tail)...)
	p.epoch, p.first = archivetest.PurgeBelow(t, c.js, natsutil.AgentJournalStream, tail)
	if p.first != tail {
		t.Fatalf("FirstSeq after purge = %d, want %d", p.first, tail)
	}
	return p
}

// fullyPurgedStep writes the two-iteration step of writeStep and purges
// every record of it.
func fullyPurgedStep(t *testing.T) purgedStep {
	t.Helper()
	p := newPurgedStep(t, []byte(`{"n":2}`))
	return purgeAllOf(t, p.c, p)
}

func TestArchive_FullyPurgedStepReplaysOnRedelivery(t *testing.T) {
	p := fullyPurgedStep(t)
	fake := p.serve(t)
	j := open(t, p.c, task(p.runID, 1, 1, 2))
	if string(j.State()) != `"s0"` {
		t.Errorf("redelivery state = %s, want the archived commit's", j.State())
	}
	sameRecords(t, j.Records(), p.want[5:])
	if fake.Requests.Load() == 0 {
		t.Fatal("the fully purged step was not read from the archive")
	}
	// The new claim is the only record JetStream holds; it names the
	// archived tail, so the next delivery and Read find the chain.
	again := open(t, p.c, task(p.runID, 1, 1, 3))
	if string(again.State()) != `"s0"` || len(again.Records()) != 4 {
		t.Errorf("next delivery: state %s with %d records, want s0 and 4",
			again.State(), len(again.Records()))
	}
	got, err := readStep(t, p)
	if err != nil || len(got) != len(p.want)+2 {
		t.Fatalf("Read = %d records, %v; want %d", len(got), err, len(p.want)+2)
	}
	sameRecords(t, got[:len(p.want)], p.want)
}

func TestArchive_FullyPurgedStepReplaysOnRetry(t *testing.T) {
	p := fullyPurgedStep(t)
	fake := p.serve(t)
	j := open(t, p.c, task(p.runID, 1, 2, 1))
	if string(j.State()) != `"s0"` || fake.Requests.Load() == 0 {
		t.Errorf("retry state = %s after %d archive requests, want s0 from the archive",
			j.State(), fake.Requests.Load())
	}
}

func TestArchive_FullyPurgedStepWithEmptyArchiveFails(t *testing.T) {
	fastArchive(t)
	p := fullyPurgedStep(t)
	fake := archivetest.Serve(t, p.nc, "", natsutil.AgentJournalStream, p.epoch, nil)
	last := streamLastSeq(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	j, err := Open(ctx, p.c.js, p.c.pub, task(p.runID, 1, 1, 2))
	if !errors.Is(err, ErrArchive) || j != nil {
		t.Fatalf("Open with an empty archive = %v, %v; want ErrArchive", j, err)
	}
	if fake.Requests.Load() == 0 || streamLastSeq(t, p) != last {
		t.Errorf("archive requests %d, last seq %d (was %d); want a request and no write",
			fake.Requests.Load(), streamLastSeq(t, p), last)
	}
	// A retry of iteration 0 may have had no claim at all (the first
	// attempt failed before it opened), so an empty archive is no
	// history there, not a failure.
	if _, err := Open(ctx, p.c.js, p.c.pub, task(p.runID, 0, 2, 1)); err != nil {
		t.Errorf("iteration 0 retry with an empty archive: %v, want an empty journal", err)
	}
}

func TestArchive_NewStepFirstOpenNeverAsksTheArchive(t *testing.T) {
	p := fullyPurgedStep(t)
	fake := p.serve(t)
	j := open(t, p.c, task("brandnew", 0, 1, 1))
	if len(j.Records()) != 0 || j.State() != nil {
		t.Errorf("new step opened with %d records and state %s", len(j.Records()), j.State())
	}
	if n := fake.Requests.Load(); n != 0 {
		t.Errorf("a new step's first open made %d archive requests, want 0", n)
	}
}

func TestArchive_ForkOfFullyPurgedParentReplaysFromArchive(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	p := purgeAllOf(t, c, purgedStep{c: c, runID: "parent"})
	p.nc = c.js.Conn()
	fake := p.serve(t)
	writeFork(t, c, "child", "parent", seqs["search"])
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	j := open(t, c, task("child", 0, 1, 1))
	var searches atomic.Int32
	first, err := j.Model(ctx, NewScriptedModel([]byte(`"x"`)).Respond)
	if err != nil || string(first) != `"m1"` {
		t.Fatalf("first model call = %s, %v; want the archived parent's m1", first, err)
	}
	result, err := j.Tool(ctx, ToolCall{Name: "search"}, Safe, countingTool(&searches, `"new"`))
	if err != nil || string(result.Output) != `"r1"` || searches.Load() != 0 {
		t.Fatalf("search = %s, %v, ran %d; want the archived r1", result.Output, err,
			searches.Load())
	}
	if fake.Requests.Load() == 0 {
		t.Error("the purged parent was not read from the archive")
	}
}

func TestArchive_ForkOfFullyPurgedParentWithEmptyArchiveFails(t *testing.T) {
	fastArchive(t)
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	p := purgeAllOf(t, c, purgedStep{c: c, runID: "parent"})
	archivetest.Serve(t, c.js.Conn(), "", natsutil.AgentJournalStream, p.epoch,
		[]dagnatsext.ArchivedMessage{})
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	_, err := WriteFork(ctx, c.js, c.pub, "child", Fork{
		ParentRunID: "parent", ParentStepID: "s", ParentSeq: seqs["search"],
	})
	if !errors.Is(err, ErrArchive) {
		t.Fatalf("fork of a purged parent the archive lacks: err = %v, want ErrArchive", err)
	}
}

// Purged exactly at the protected floor (#980), an earlier iteration's
// commit is the first record left: Open needs nothing older and never
// asks the archive, while a full Read still recovers every record.
func TestArchive_OpenAtTheFloorCommitNeedsNoArchive(t *testing.T) {
	p := newPurgedStepAt(t, 4)
	fake := p.serve(t)
	j := open(t, p.c, task(p.runID, 1, 1, 2))
	if string(j.State()) != `"s0"` || fake.Requests.Load() != 0 {
		t.Fatalf("Open at the floor: state %s after %d archive requests, want s0 and none",
			j.State(), fake.Requests.Load())
	}
	sameRecords(t, j.Records(), p.want[5:])
	got, err := readStep(t, p)
	if err != nil || fake.Requests.Load() == 0 {
		t.Fatalf("Read = %d records, %v after %d requests; want the archive read",
			len(got), err, fake.Requests.Load())
	}
	sameRecords(t, got[:len(p.want)], p.want)
}

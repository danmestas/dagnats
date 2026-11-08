// journal/archive_test.go
// Endless JetStream: a journal read that needs records below
// AGENT_JOURNAL's FirstSeq reads them from the archive (the lode range
// protocol), continues from JetStream, and verifies the subject's
// record chain across the boundary. It fails closed, naming the run and
// step, on every archive failure, and never asks the archive for a
// journal JetStream still holds in full.
//
// Methodology: one embedded server per case. A step's journal is
// written through Open/Append/Commit interleaved with another run's
// records, read once as the reference, snapshotted below a sequence and
// purged there, as an archiver would. The fake archive of
// internal/archivetest serves the snapshot. Records are compared field
// by field except Time, which the archive carries at microseconds.
package journal

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/dagnatsext/archivenats"
	"github.com/danmestas/dagnats/internal/archivetest"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

// purgedStep is a step journal with two iterations, partly purged.
type purgedStep struct {
	c      client
	nc     *nats.Conn
	runID  string
	want   []Record
	epoch  int64
	first  uint64
	before []dagnatsext.ArchivedMessage
}

// fastArchive shortens the archive timeouts for the test.
func fastArchive(t *testing.T) {
	t.Helper()
	saved := archiveOptions
	archiveOptions = archivenats.Options{
		RequestTimeout: 300 * time.Millisecond, ReadTimeout: 5 * time.Second,
		BusyRetryMax: 3, BusyBackoff: 10 * time.Millisecond,
	}
	t.Cleanup(func() { archiveOptions = saved })
}

// writeStep writes iteration 0 (entries, a commit with state s0) and
// iteration 1 (two entries), with another run's records between them,
// and returns the step's records as read before any purge.
func writeStep(t *testing.T, c client, nc *nats.Conn, runID string, big []byte) []Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	other := open(t, c, task(runID+"x", 0, 1, 1))
	j0 := open(t, c, task(runID, 0, 1, 1))
	for _, data := range [][]byte{[]byte(`{"n":1}`), big, []byte(`{"n":3}`)} {
		if _, err := j0.Append(ctx, TypeEntry, data); err != nil {
			t.Fatalf("append: %v", err)
		}
		if _, err := other.Append(ctx, TypeEntry, []byte(`{"other":1}`)); err != nil {
			t.Fatalf("append other: %v", err)
		}
	}
	if _, err := j0.Commit(ctx, Commit{Action: ActionContinue, State: []byte(`"s0"`)}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	j1 := open(t, c, task(runID, 1, 1, 1))
	for _, data := range []string{`{"i1":1}`, `{"i1":2}`} {
		if _, err := j1.Append(ctx, TypeEntry, []byte(data)); err != nil {
			t.Fatalf("append i1: %v", err)
		}
	}
	records, err := Read(ctx, c.js, runID, "s")
	if err != nil || len(records) != 8 {
		t.Fatalf("reference Read = %d records, %v; want 8", len(records), err)
	}
	return records
}

// newPurgedStep writes the step and purges the stream below the step's
// fourth record (inside iteration 0), keeping a snapshot of the rest.
func newPurgedStep(t *testing.T, big []byte) purgedStep {
	t.Helper()
	return newPurgedStepCut(t, big, 3)
}

// newPurgedStepAt is newPurgedStep purged below the step's record at
// index cut (4 is iteration 0's commit).
func newPurgedStepAt(t *testing.T, cut int) purgedStep {
	t.Helper()
	return newPurgedStepCut(t, []byte(`{"n":2}`), cut)
}

func newPurgedStepCut(t *testing.T, big []byte, index int) purgedStep {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	runID := natsutil.SubjectToken(strings.ReplaceAll(t.Name(), "/", "-"))
	want := writeStep(t, c, nc, runID, big)
	cut := want[index].Seq
	before := archivetest.Snapshot(t, c.js, natsutil.AgentJournalStream, cut)
	epoch, first := archivetest.PurgeBelow(t, c.js, natsutil.AgentJournalStream, cut)
	if first != cut {
		t.Fatalf("FirstSeq after purge = %d, want %d", first, cut)
	}
	return purgedStep{c: c, nc: nc, runID: runID, want: want, epoch: epoch, first: first,
		before: before}
}

func (p purgedStep) serve(t *testing.T) *archivetest.Responder {
	t.Helper()
	return archivetest.Serve(t, p.nc, "", natsutil.AgentJournalStream, p.epoch, p.before)
}

// sameRecords compares records except Time, which the archive holds at
// microsecond precision.
func sameRecords(t *testing.T, got, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Time.Sub(w.Time).Abs() > time.Microsecond {
			t.Errorf("record %d time %v, want %v", i, g.Time, w.Time)
		}
		g.Time, w.Time = time.Time{}, time.Time{}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func streamLastSeq(t *testing.T, p purgedStep) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	stream, err := p.c.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	return info.State.LastSeq
}

func readStep(t *testing.T, p purgedStep) ([]Record, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	return Read(ctx, p.c.js, p.runID, "s")
}

func TestArchive_PartlyPurgedJournalReplaysIdentically(t *testing.T) {
	p := newPurgedStep(t, []byte(`{"n":2}`))
	fake := p.serve(t)
	got, err := readStep(t, p)
	if err != nil {
		t.Fatalf("Read after purge: %v", err)
	}
	sameRecords(t, got, p.want)
	if fake.Requests.Load() == 0 {
		t.Error("the purged part was not read from the archive")
	}
	j := open(t, p.c, task(p.runID, 1, 1, 2))
	if string(j.State()) != `"s0"` {
		t.Errorf("redelivery state = %s, want the iteration 0 commit's", j.State())
	}
	sameRecords(t, j.Records(), p.want[5:])
}

func TestArchive_UnpurgedJournalNeverAsksTheArchive(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	writeStep(t, c, nc, "early", []byte(`{"n":2}`))
	want := writeStep(t, c, nc, "late", []byte(`{"n":2}`))
	// The stream is purged, but only of records the "late" step never
	// had: its journal is all in JetStream.
	epoch, _ := archivetest.PurgeBelow(t, c.js, natsutil.AgentJournalStream, want[0].Seq)
	fake := archivetest.Serve(t, nc, "", natsutil.AgentJournalStream, epoch, nil)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	got, err := Read(ctx, c.js, "late", "s")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	sameRecords(t, got, want)
	open(t, c, task("late", 1, 1, 2))
	if n := fake.Requests.Load(); n != 0 {
		t.Errorf("a journal held in full made %d archive requests, want 0", n)
	}
}

func TestArchive_MissingBoundaryRecordFailsOpen(t *testing.T) {
	p := newPurgedStep(t, []byte(`{"n":2}`))
	fake := p.serve(t)
	fake.Drop = p.want[2].Seq // the last archived record of the step
	_, err := readStep(t, p)
	if !errors.Is(err, ErrDiscontinuous) {
		t.Fatalf("Read with a hole at the boundary: err = %v, want ErrDiscontinuous", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	_, err = Open(ctx, p.c.js, p.c.pub, task(p.runID, 1, 1, 2))
	if !errors.Is(err, ErrDiscontinuous) {
		t.Errorf("Open with a hole at the boundary: err = %v, want ErrDiscontinuous", err)
	}
}

func TestArchive_FailsClosed(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, p purgedStep)
	}{
		{"no archive", func(*testing.T, purgedStep) {}},
		{"not archived", func(t *testing.T, p purgedStep) {
			fake := p.serve(t)
			fake.Code, fake.Floor = dagnatsext.ArchiveCodeNotArchived, 1
		}},
		{"unknown epoch", func(t *testing.T, p purgedStep) {
			archivetest.Serve(t, p.nc, "", natsutil.AgentJournalStream, p.epoch+1, p.before)
		}},
		{"gap", func(t *testing.T, p purgedStep) {
			p.serve(t).Gaps = []dagnatsext.ArchiveGap{{From: 2, To: 2}}
		}},
		{"timeout", func(t *testing.T, p purgedStep) { p.serve(t).Silent = true }},
		{"busy forever", func(t *testing.T, p purgedStep) { p.serve(t).BusyFor = 1 << 40 }},
		{"not indexed", func(t *testing.T, p purgedStep) {
			p.serve(t).Code = dagnatsext.ArchiveCodeNotIndexed
		}},
		{"unknown code", func(t *testing.T, p purgedStep) { p.serve(t).Code = "no_such_code" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fastArchive(t)
			p := newPurgedStep(t, []byte(`{"n":2}`))
			tc.setup(t, p)
			last := streamLastSeq(t, p)
			_, err := readStep(t, p)
			if !errors.Is(err, ErrArchive) || !strings.Contains(err.Error(), p.runID) {
				t.Fatalf("Read: err = %v, want ErrArchive naming run %s", err, p.runID)
			}
			ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
			defer cancel()
			j, err := Open(ctx, p.c.js, p.c.pub, task(p.runID, 1, 1, 2))
			if !errors.Is(err, ErrArchive) || j != nil {
				t.Fatalf("Open: err = %v, want ErrArchive and no journal", err)
			}
			if got := streamLastSeq(t, p); got != last {
				t.Errorf("a failed Open wrote: last seq %d, was %d", got, last)
			}
		})
	}
}

func TestArchive_BusyIsRetried(t *testing.T) {
	fastArchive(t)
	p := newPurgedStep(t, []byte(`{"n":2}`))
	fake := p.serve(t)
	fake.BusyFor = 2
	got, err := readStep(t, p)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	sameRecords(t, got, p.want)
	if fake.Requests.Load() < 3 {
		t.Errorf("requests = %d, want the two busy replies retried", fake.Requests.Load())
	}
}

func TestArchive_SplitRecordReassemblesByteIdentical(t *testing.T) {
	big := []byte(`{"blob":"` + strings.Repeat("0123456789", 30_000) + `"}`)
	p := newPurgedStep(t, big)
	fake := p.serve(t)
	fake.PageBytes = 64 << 10
	got, err := readStep(t, p)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	sameRecords(t, got, p.want)
	if !bytes.Equal(got[2].Data, big) || fake.Requests.Load() < 5 {
		t.Errorf("big record intact %v over %d requests, want intact over >= 5",
			bytes.Equal(got[2].Data, big), fake.Requests.Load())
	}
}

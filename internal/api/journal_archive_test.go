// internal/api/journal_archive_test.go
// Endless JetStream on the engine side: a program's relayed journal
// (journal.view) and the client read GET /runs/{run}/steps/{step}/journal
// serve records purged below AGENT_JOURNAL's FirstSeq from the archive,
// in order and complete, and fail closed when no archive answers. A
// journal JetStream still holds in full never reaches the archive.
//
// Methodology: real embedded NATS, orchestrator and NATSAPI. A step's
// journal is written, snapshotted below a sequence and purged there, as
// an archiver would; the fake archive of internal/archivetest serves the
// snapshot on the range subject. Every wait is bounded.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/archivetest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// relayedPurge writes a program step's journal through the relay
// (claim + three entries), purges AGENT_JOURNAL below its third entry,
// and returns what a redelivery needs to open it again.
func relayedPurge(t *testing.T) (*nats.Conn, jetstream.JetStream, journal.Task,
	*natsutil.TracingPublisher, []string, func() *archivetest.Responder) {
	t.Helper()
	return relayedPurgeAt(t, false)
}

// relayedPurgeAt is relayedPurge; with whole, every record of the step
// is purged (another run's record lands after them) and the task that
// reopens it is the step's retry, attempt 2.
func relayedPurgeAt(t *testing.T, whole bool) (*nats.Conn, jetstream.JetStream, journal.Task,
	*natsutil.TracingPublisher, []string, func() *archivetest.Responder) {
	t.Helper()
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task := journal.Task{
		RunID: runID, StepID: "work", Iteration: 0, Attempt: 1,
		DispatchNonce: nonce, Delivered: 1, StreamSeq: 1,
	}
	first, err := journal.Open(ctx, js, notify, task)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	var cut uint64
	bodies := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}
	for i, body := range bodies {
		seq, err := first.Append(ctx, journal.TypeEntry, []byte(body))
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if i == 2 {
			cut = seq
		}
	}
	if whole {
		other, err := journal.Open(ctx, js, natsutil.NewTracingPublisher(nc, js), journal.Task{
			RunID: runID + "x", StepID: "work", Attempt: 1, DispatchNonce: "nx", Delivered: 1,
		})
		if err != nil {
			t.Fatalf("open other run: %v", err)
		}
		cut, task.Attempt = other.Seq(), 2
	}
	before := archivetest.Snapshot(t, js, natsutil.AgentJournalStream, cut)
	epoch, _ := archivetest.PurgeBelow(t, js, natsutil.AgentJournalStream, cut)
	serve := func() *archivetest.Responder {
		return archivetest.Serve(t, nc, "", natsutil.AgentJournalStream, epoch, before)
	}
	task.Delivered = 2
	return nc, js, task, notify, bodies, serve
}

func TestRelayedJournalReplaysPurgedRecordsFromArchive(t *testing.T) {
	_, js, task, notify, bodies, serve := relayedPurge(t)
	fake := serve()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	second, err := journal.Open(ctx, js, notify, task)
	if err != nil {
		t.Fatalf("redelivered claim after purge: %v", err)
	}
	records := second.Records()
	var got []string
	for _, r := range records {
		if r.Type == journal.TypeEntry {
			got = append(got, string(r.Data))
		}
	}
	if strings.Join(got, ",") != strings.Join(bodies, ",") {
		t.Fatalf("replayed entries = %v, want %v", got, bodies)
	}
	if records[0].Type != journal.TypeClaim || fake.Requests.Load() == 0 {
		t.Errorf("first record %q after %d archive requests, want the claim from the archive",
			records[0].Type, fake.Requests.Load())
	}
}

func TestRelayedJournalFailsClosedWithoutArchive(t *testing.T) {
	_, js, task, notify, _, _ := relayedPurge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	j, err := journal.Open(ctx, js, notify, task)
	if !errors.Is(err, journal.ErrArchive) || j != nil {
		t.Fatalf("Open with no archive = %v, %v; want ErrArchive", j, err)
	}
	if !strings.Contains(err.Error(), task.RunID) {
		t.Errorf("error %q does not name run %s", err, task.RunID)
	}
}

// purgedFixture is a journal fixture with claim + four entries, purged
// below its third entry.
func purgedFixture(t *testing.T, name string) (*journalFixture, func() *archivetest.Responder) {
	t.Helper()
	f := newJournalFixture(t, name)
	var seqs []uint64
	for _, body := range []string{"one", "two", "three", "four"} {
		seqs = append(seqs, f.append(t, body))
	}
	before := archivetest.Snapshot(t, f.svc.js, natsutil.AgentJournalStream, seqs[2])
	epoch, _ := archivetest.PurgeBelow(t, f.svc.js, natsutil.AgentJournalStream, seqs[2])
	return f, func() *archivetest.Responder {
		return archivetest.Serve(t, f.nc, "", natsutil.AgentJournalStream, epoch, before)
	}
}

func TestJournalPage_ServesPurgedRecordsFromArchive(t *testing.T) {
	f, serve := purgedFixture(t, "jrn-archive")
	_, status := getJournalPage(t, f.url("a", ""))
	if status != http.StatusServiceUnavailable {
		t.Fatalf("purged page with no archive: status %d, want 503", status)
	}
	fake := serve()
	page, status := getJournalPage(t, f.url("a", ""))
	if status != http.StatusOK || len(page.Records) != 5 {
		t.Fatalf("status=%d records=%d, want 200 and 5 (claim + 4)", status, len(page.Records))
	}
	for i := 1; i < len(page.Records); i++ {
		if page.Records[i].Seq <= page.Records[i-1].Seq {
			t.Errorf("records out of order at %d: %d after %d",
				i, page.Records[i].Seq, page.Records[i-1].Seq)
		}
	}
	requests := fake.Requests.Load()
	tail, _ := getJournalPage(t, f.url("a", fmt.Sprintf("?from=%d", page.Records[3].Seq)))
	if len(tail.Records) != 2 || fake.Requests.Load() != requests {
		t.Errorf("page from a held record = %d records after %d new archive requests, "+
			"want 2 and none", len(tail.Records), fake.Requests.Load()-requests)
	}
}

func TestJournalFollow_ServesPurgedRecordsFirst(t *testing.T) {
	f, serve := purgedFixture(t, "jrn-archive-follow")
	serve()
	_, events := openJournalFollow(t, f.url("a", "?follow=1"), "")
	var data []string
	for len(data) < 5 {
		data = append(data, nextRecordEvent(t, events).data)
	}
	if !strings.Contains(data[0], `"type":"claim"`) || !strings.Contains(data[4], `"four"`) {
		t.Errorf("follow events = %v, want the claim first and \"four\" last", data)
	}
	if !strings.Contains(data[1], `"one"`) || !strings.Contains(data[2], `"two"`) {
		t.Errorf("purged entries = %v, %v; want one, two", data[1], data[2])
	}
}

func TestRelayedJournalReplaysAWhollyPurgedStepOnRetry(t *testing.T) {
	_, js, task, notify, bodies, serve := relayedPurgeAt(t, true)
	fake := serve()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	retry, err := journal.Open(ctx, js, notify, task)
	if err != nil {
		t.Fatalf("retry claim after the whole step was purged: %v", err)
	}
	var got []string
	for _, r := range retry.Records() {
		if r.Type == journal.TypeEntry {
			got = append(got, string(r.Data))
		}
	}
	if strings.Join(got, ",") != strings.Join(bodies, ",") || fake.Requests.Load() == 0 {
		t.Fatalf("retry replayed %v after %d archive requests, want %v from the archive",
			got, fake.Requests.Load(), bodies)
	}
}

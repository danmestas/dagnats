// journal/conformance_test.go
// Journal conformance suite (ADR-026), modelled on pi-durable's: every
// append is fenced by the tail its writer saw (R1); a record from
// another claim fences a writer (R2); racing claimers get exactly one
// winner; a claim never fences a later iteration (R4); a rejection is
// not fencing until the tail says so (R3): a lost acknowledgement, an
// own record at the tail and a tail written through another node all
// resolve to success; commits are keyed by attempt and iteration and
// recovery honours a commit only for the task's own attempt.
//
// Methodology: every case runs twice, against one embedded server and
// against an in-process three-node cluster with R3 streams, through
// the public Open/Append/Commit surface. Forged records are published
// raw to stand in for a writer the protocol would otherwise prevent
// (a stale redelivery whose tail read predates a rival claim, a lost
// acknowledgement). Every wait is bounded by a context deadline. Each
// case has at least one positive and one negative assertion.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const caseTimeout = 30 * time.Second

// topology is one NATS deployment the suite runs against.
type topology struct {
	name string
	// connect returns a provisioned connection; extra connections to
	// other nodes (the same node on a single server) come from dial.
	connect func(t *testing.T) *nats.Conn
}

func topologies() []topology {
	return []topology{
		{name: "single", connect: func(t *testing.T) *nats.Conn {
			_, nc := natsutil.StartTestServer(t)
			if err := natsutil.SetupAll(nc); err != nil {
				t.Fatalf("SetupAll: %v", err)
			}
			return nc
		}},
		{name: "cluster3", connect: func(t *testing.T) *nats.Conn {
			if testing.Short() {
				t.Skip("three-node cluster skipped in -short mode")
			}
			nc := startCluster(t)
			if err := natsutil.SetupAll(nc); err != nil {
				t.Fatalf("SetupAll: %v", err)
			}
			return nc
		}},
	}
}

// forEachTopology runs fn as a subtest per topology.
func forEachTopology(t *testing.T, fn func(t *testing.T, nc *nats.Conn)) {
	t.Helper()
	for _, topo := range topologies() {
		t.Run(topo.name, func(t *testing.T) {
			fn(t, topo.connect(t))
		})
	}
}

// dialOther opens a second connection, to a different node when the
// topology has one.
func dialOther(t *testing.T, nc *nats.Conn) *nats.Conn {
	t.Helper()
	url := nc.ConnectedUrl()
	for _, candidate := range nc.Servers() {
		if candidate != url {
			url = candidate
			break
		}
	}
	other, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(other.Close)
	return other
}

type client struct {
	js  jetstream.JetStream
	pub *natsutil.TracingPublisher
}

func newClient(t *testing.T, nc *nats.Conn) client {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return client{js: js, pub: natsutil.NewTracingPublisher(nc, js)}
}

func task(runID string, iteration, attempt int) Task {
	return Task{
		RunID: runID, StepID: "s", Iteration: iteration, Attempt: attempt,
		DispatchNonce: "nonce-" + strconv.Itoa(attempt), Delivered: attempt,
	}
}

func open(t *testing.T, c client, tk Task) *Journal {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	j, err := Open(ctx, c.js, c.pub, tk)
	if err != nil {
		t.Fatalf("Open %+v: %v", tk, err)
	}
	return j
}

func countRecords(t *testing.T, c client, runID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	records, err := Read(ctx, c.js, runID, "s")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return len(records)
}

// forge publishes a record outside the protocol with the given claim
// identity and index, fenced on expected so it lands at the tail.
func forge(
	t *testing.T, nc *nats.Conn, tk Task, recordType string, index int,
	expected uint64, data []byte,
) uint64 {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	msg := recordMessage(tk, recordType, index, expected, data)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	ack, err := js.PublishMsg(ctx, msg)
	if err != nil {
		t.Fatalf("forge %s: %v", recordType, err)
	}
	return ack.Sequence
}

func TestConformance_AppendCarriesFenceAndStableMsgID(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		tk := task("r-headers", 0, 1)
		j := open(t, c, tk)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		seq, err := j.Append(ctx, TypeEntry, []byte(`{"n":1}`))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		stream, err := c.js.Stream(ctx, natsutil.AgentJournalStream)
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			t.Fatalf("GetMsg(%d): %v", seq, err)
		}
		if got := raw.Header.Get(headerExpectedLastSeq); got != "1" {
			t.Errorf("expected-last-subject-sequence = %q, want 1 (the claim)", got)
		}
		if got, want := raw.Header.Get("Nats-Msg-Id"), MsgID(tk, 1); got != want {
			t.Errorf("Nats-Msg-Id = %q, want %q", got, want)
		}
		if raw.Header.Get(HeaderType) != TypeEntry {
			t.Errorf("type header = %q, want entry", raw.Header.Get(HeaderType))
		}
		if j.Fenced() {
			t.Error("a successful append must not fence the writer")
		}
	})
}

func TestConformance_RecordFromAnotherClaimFences(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		stalled := open(t, c, task("r-fence", 0, 1))
		successor := open(t, c, task("r-fence", 0, 2))
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		if _, err := stalled.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrFenced) {
			t.Fatalf("stalled Append err = %v, want ErrFenced", err)
		}
		if !stalled.Fenced() {
			t.Error("stalled journal must report Fenced after the rejection")
		}
		if _, err := stalled.Commit(ctx, Commit{Action: ActionComplete}); !errors.Is(err, ErrFenced) {
			t.Errorf("stalled Commit err = %v, want ErrFenced", err)
		}
		if _, err := successor.Append(ctx, TypeEntry, []byte(`2`)); err != nil {
			t.Fatalf("successor Append: %v", err)
		}
		if got := countRecords(t, c, "r-fence"); got != 3 {
			t.Errorf("records = %d, want 3 (two claims, one entry)", got)
		}
	})
}

func TestConformance_RacingClaimsLeaveExactlyOneWriter(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		const rounds, racers = 10, 4
		for round := 0; round < rounds; round++ {
			runID := fmt.Sprintf("r-race-%d", round)
			var wg sync.WaitGroup
			journals := make([]*Journal, racers)
			openErrs := make([]error, racers)
			for i := 0; i < racers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					c := newClient(t, nc)
					ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
					defer cancel()
					journals[i], openErrs[i] = Open(ctx, c.js, c.pub, task(runID, 0, i+1))
				}(i)
			}
			wg.Wait()
			// Claims that landed after a rival's fence the rival, so more
			// than one Open may succeed; afterwards exactly one journal
			// may still write, and that one's claim is the newest.
			claimed, writers := 0, 0
			for i := range journals {
				if openErrs[i] != nil {
					if !errors.Is(openErrs[i], ErrFenced) {
						t.Fatalf("round %d racer %d: %v", round, i, openErrs[i])
					}
					continue
				}
				claimed++
				ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
				_, err := journals[i].Append(ctx, TypeEntry, []byte(`1`))
				cancel()
				switch {
				case err == nil:
					writers++
				case !errors.Is(err, ErrFenced):
					t.Fatalf("round %d racer %d Append: %v", round, i, err)
				}
			}
			if claimed < 1 || writers != 1 {
				t.Fatalf("round %d: %d claims, %d writers; want >= 1 and exactly 1",
					round, claimed, writers)
			}
			if got := countRecords(t, newClient(t, nc), runID); got != claimed+1 {
				t.Fatalf("round %d: %d records, want %d claims plus one entry", round, got, claimed)
			}
		}
	})
}

func TestConformance_StaleIterationStopsWithoutWriting(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		first := open(t, c, task("r-stale", 0, 1))
		if _, err := first.Commit(ctx, Commit{Action: ActionContinue, State: []byte("s0")}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		open(t, c, task("r-stale", 1, 1))
		before := countRecords(t, c, "r-stale")
		_, err := Open(ctx, c.js, c.pub, task("r-stale", 0, 2))
		if !errors.Is(err, ErrStale) {
			t.Fatalf("late redelivery Open err = %v, want ErrStale", err)
		}
		if got := countRecords(t, c, "r-stale"); got != before {
			t.Errorf("records = %d, want %d: a stale task must not write", got, before)
		}
	})
}

func TestConformance_EarlierIterationClaimNeverFencesLater(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		first := open(t, c, task("r-scope", 0, 1))
		if _, err := first.Commit(ctx, Commit{Action: ActionContinue}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		live := open(t, c, task("r-scope", 1, 1))
		// A redelivery of iteration 0 whose tail read predates the live
		// claim: its claim lands at the tail behind the live worker.
		late := task("r-scope", 0, 2)
		lateClaimSeq := forge(t, nc, late, TypeClaim, 0, live.Seq(), []byte(`{}`))
		if _, err := live.Append(ctx, TypeEntry, []byte(`1`)); err != nil {
			t.Fatalf("live Append after an earlier-iteration claim: %v", err)
		}
		if live.Fenced() {
			t.Error("a claim for an earlier iteration must not fence the live worker")
		}
		// And the late claimer, which last saw its own claim land and
		// whose next append finds the live worker's record at the tail.
		lateJournal := &Journal{task: late, seq: lateClaimSeq, next: 1}
		lateJournal.attach(c.js, c.pub)
		if err := lateJournal.bind(ctx); err != nil {
			t.Fatalf("bind: %v", err)
		}
		if _, err := lateJournal.Append(ctx, TypeEntry, []byte(`x`)); !errors.Is(err, ErrFenced) {
			t.Errorf("late Append err = %v, want ErrFenced", err)
		}
	})
}

func TestConformance_CommitStateRecoveredByNextIterationAndRetry(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		first := open(t, c, task("r-commit", 0, 1))
		if _, err := first.Append(ctx, TypeEntry, []byte(`"a"`)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if _, err := first.Commit(ctx, Commit{Action: ActionContinue, State: []byte("s0")}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if _, err := first.Append(ctx, TypeEntry, []byte(`"late"`)); !errors.Is(err, ErrCommitted) {
			t.Errorf("Append after Commit err = %v, want ErrCommitted", err)
		}
		next := open(t, c, task("r-commit", 1, 1))
		if string(next.State()) != "s0" {
			t.Errorf("iteration 1 State = %q, want s0", next.State())
		}
		if len(next.Records()) != 0 {
			t.Errorf("iteration 1 Records = %d, want none from iteration 0", len(next.Records()))
		}
		if _, ok := next.Committed(); ok {
			t.Error("iteration 1 must not see iteration 0's commit as its own")
		}
		if _, err := next.Append(ctx, TypeEntry, []byte(`"b"`)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		retry := open(t, c, task("r-commit", 1, 2))
		if string(retry.State()) != "s0" {
			t.Errorf("retry State = %q, want s0 (committed before the iteration)", retry.State())
		}
		records := retry.Records()
		if len(records) != 2 || records[0].Type != TypeClaim || records[1].Type != TypeEntry {
			t.Fatalf("retry Records = %+v, want the first attempt's claim and entry", records)
		}
		if string(records[1].Data) != `"b"` || records[1].Attempt != 1 {
			t.Errorf("reused record = %+v, want attempt 1's \"b\"", records[1])
		}
	})
}

func TestConformance_OwnAttemptCommitIsHonoured(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		first := open(t, c, task("r-own", 0, 1))
		if _, err := first.Commit(ctx, Commit{
			Action: ActionComplete, Output: json.RawMessage(`"done"`),
		}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		before := countRecords(t, c, "r-own")
		again := open(t, c, task("r-own", 0, 1))
		commit, ok := again.Committed()
		if !ok || commit.Action != ActionComplete || string(commit.Output) != `"done"` {
			t.Fatalf("Committed = %+v, %v; want the complete commit", commit, ok)
		}
		if got := countRecords(t, c, "r-own"); got != before {
			t.Errorf("records = %d, want %d: a recovered attempt claims nothing", got, before)
		}
		if _, err := again.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrCommitted) {
			t.Errorf("Append after recovery err = %v, want ErrCommitted", err)
		}
		other := open(t, c, task("r-own", 0, 2))
		if _, ok := other.Committed(); ok {
			t.Error("another attempt must not honour attempt 1's commit")
		}
	})
}

func TestConformance_FalseRejectionLostAck(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		tk := task("r-lostack", 0, 1)
		j := open(t, c, tk)
		// The append landed but its acknowledgement never came back: the
		// writer resends the same message against the tail it last saw.
		landed := forge(t, dialOther(t, nc), tk, TypeEntry, 1, j.Seq(), []byte(`"e"`))
		seq, err := j.Append(ctx, TypeEntry, []byte(`"e"`))
		if err != nil {
			t.Fatalf("resend err = %v, want success", err)
		}
		if seq != landed {
			t.Errorf("resend seq = %d, want the landed %d", seq, landed)
		}
		if got := countRecords(t, c, "r-lostack"); got != 2 {
			t.Errorf("records = %d, want 2 (no duplicate entry)", got)
		}
		if j.Fenced() {
			t.Error("a lost ack must not fence the writer")
		}
	})
}

func TestConformance_FalseRejectionOwnRecordAtTailViaOtherNode(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		tk := task("r-owntail", 0, 1)
		j := open(t, c, tk)
		// A record of this claim written through another node (an earlier
		// in-flight append) sits at the tail: the writer resyncs, not fences.
		forge(t, dialOther(t, nc), tk, TypeEntry, 7, j.Seq(), []byte(`"earlier"`))
		seq, err := j.Append(ctx, TypeEntry, []byte(`"next"`))
		if err != nil {
			t.Fatalf("Append err = %v, want success after resync", err)
		}
		if seq != j.Seq() || seq < 3 {
			t.Errorf("seq = %d, want the new tail (>= 3)", seq)
		}
		if got := countRecords(t, c, "r-owntail"); got != 3 {
			t.Errorf("records = %d, want 3", got)
		}
	})
}

func TestConformance_PauseRecordResumes(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		first := open(t, c, task("r-pause", 0, 1))
		if _, err := first.Pause(ctx, "cooldown", 50*time.Millisecond); err != nil {
			t.Fatalf("Pause: %v", err)
		}
		resumed := open(t, c, task("r-pause", 0, 2))
		if name, ok := resumed.Resume(); !ok || name != "cooldown" {
			t.Errorf("Resume = %q, %v; want cooldown", name, ok)
		}
		if _, err := resumed.Append(ctx, TypeEntry, []byte(`1`)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		third := open(t, c, task("r-pause", 0, 3))
		if _, ok := third.Resume(); ok {
			t.Error("an entry after the pause means this delivery is not a resume")
		}
	})
}

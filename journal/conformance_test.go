// journal/conformance_test.go
// Journal conformance suite (ADR-026), modelled on pi-durable's: every
// append is fenced by the tail its writer saw (R1); a record from
// another claim fences a writer (R2); two deliveries of one task
// message leave exactly one writer; a claim never fences a later
// iteration (R4); a rejection is not fencing until the tail says so
// (R3): a lost acknowledgement, an own record at the tail and a tail
// written through another node all resolve to success; commits are
// keyed by attempt and iteration and recovery honours a commit only
// for the task's own attempt, across deliveries.
//
// Methodology: every case runs against one embedded server and an
// in-process three-node cluster with R3 streams, through the public
// Open/Append/Commit surface, and models production dispatch: the
// engine stamps the attempt from iteration 1 on, so a stalled worker
// and the AckWait redelivery that replaces it hold the same run, step,
// iteration, attempt and nonce and differ only in delivery count; on
// iteration 0 the attempt follows the delivery count. Forged records
// are published raw to stand in for a writer the protocol would
// otherwise prevent. Every wait is bounded. Each case has at least one
// positive and one negative assertion.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	name    string
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
			if err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(clusterStoreBudget)); err != nil {
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

// dispatch is one shape of a task payload as the engine stamps it.
type dispatch struct {
	name      string
	iteration int
	// attempt of the first delivery and of its AckWait redelivery: on
	// iteration 0 the payload carries no attempt and the worker uses
	// the delivery count; from iteration 1 the engine stamps it.
	first, redelivered int
}

func dispatches() []dispatch {
	return []dispatch{
		{name: "iteration0", iteration: 0, first: 1, redelivered: 2},
		{name: "iteration1", iteration: 1, first: 1, redelivered: 1},
	}
}

// forEachDispatch runs fn per topology and dispatch shape, on a run of
// its own, with iteration 0 already committed when the shape starts at
// iteration 1.
func forEachDispatch(
	t *testing.T, fn func(t *testing.T, nc *nats.Conn, runID string, d dispatch),
) {
	t.Helper()
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		for _, d := range dispatches() {
			t.Run(d.name, func(t *testing.T) {
				runID := fmt.Sprintf("%s-%s", t.Name()[len("TestConformance_"):], d.name)
				runID = natsutil.SubjectToken(runID)
				if d.iteration == 1 {
					c := newClient(t, nc)
					ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
					defer cancel()
					prior := open(t, c, task(runID, 0, 1, 1))
					if _, err := prior.Commit(ctx, Commit{
						Action: ActionContinue, State: []byte("s0"),
					}); err != nil {
						t.Fatalf("commit iteration 0: %v", err)
					}
				}
				fn(t, nc, runID, d)
			})
		}
	})
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

// task is one delivery of a step's dispatch. The nonce is the run's:
// one dispatch, however often it is delivered.
func task(runID string, iteration, attempt, delivered int) Task {
	return Task{
		RunID: runID, StepID: "s", Iteration: iteration, Attempt: attempt,
		DispatchNonce: "nonce-" + runID, Delivered: delivered,
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

// shortenDedupWindow sets the journal's dedup window so a test can
// step past it.
func shortenDedupWindow(t *testing.T, c client, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	stream, err := c.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cfg := stream.CachedInfo().Config
	cfg.Duplicates = window
	if _, err := c.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}
}

func TestConformance_AppendCarriesFenceAndStableMsgID(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		tk := task("r-headers", 0, 1, 1)
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
		if MsgID(tk, 1) == MsgID(task("r-headers", 0, 1, 2), 1) {
			t.Error("msg-id must differ between deliveries of one dispatch")
		}
		if raw.Header.Get(HeaderType) != TypeEntry || raw.Header.Get(HeaderDelivered) != "1" {
			t.Errorf("headers = %v, want type entry and delivered 1", raw.Header)
		}
		if j.Fenced() {
			t.Error("a successful append must not fence the writer")
		}
	})
}

func TestConformance_RedeliveryFencesStalledWorker(t *testing.T) {
	forEachDispatch(t, func(t *testing.T, nc *nats.Conn, runID string, d dispatch) {
		c := newClient(t, nc)
		stalled := open(t, c, task(runID, d.iteration, d.first, 1))
		before := countRecords(t, c, runID)
		successor := open(t, c, task(runID, d.iteration, d.redelivered, 2))
		if got := countRecords(t, c, runID); got != before+1 {
			t.Fatalf("records = %d, want %d: the redelivery's claim must land, not dedup",
				got, before+1)
		}
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
		if _, err := successor.Commit(ctx, Commit{Action: ActionComplete}); err != nil {
			t.Fatalf("successor Commit: %v", err)
		}
		if got := countRecords(t, c, runID); got != before+2 {
			t.Errorf("records = %d, want %d: one commit, from the successor", got, before+2)
		}
	})
}

func TestConformance_RedeliveryOutsideDedupWindowStillFences(t *testing.T) {
	forEachDispatch(t, func(t *testing.T, nc *nats.Conn, runID string, d dispatch) {
		c := newClient(t, nc)
		shortenDedupWindow(t, c, 100*time.Millisecond)
		stalled := open(t, c, task(runID, d.iteration, d.first, 1))
		time.Sleep(300 * time.Millisecond)
		successor := open(t, c, task(runID, d.iteration, d.redelivered, 2))
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		if _, err := stalled.Commit(ctx, Commit{Action: ActionComplete}); !errors.Is(err, ErrFenced) {
			t.Fatalf("stalled Commit err = %v, want ErrFenced", err)
		}
		if _, err := successor.Commit(ctx, Commit{Action: ActionComplete}); err != nil {
			t.Fatalf("successor Commit: %v", err)
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel2()
		records, err := Read(ctx2, c.js, runID, "s")
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		commits := 0
		for _, r := range records {
			if r.Type == TypeCommit && r.Iteration == d.iteration {
				commits++
			}
		}
		if commits != 1 {
			t.Errorf("iteration %d commits = %d, want exactly 1", d.iteration, commits)
		}
	})
}

func TestConformance_TwoDeliveriesLeaveExactlyOneWriter(t *testing.T) {
	forEachDispatch(t, func(t *testing.T, nc *nats.Conn, runID string, d dispatch) {
		const rounds = 6
		for round := 0; round < rounds; round++ {
			var wg sync.WaitGroup
			journals := make([]*Journal, 2)
			openErrs := make([]error, 2)
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					c := newClient(t, nc)
					ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
					defer cancel()
					// Delivery counts only grow on one message; on
					// iteration 0 the attempt follows them.
					delivered := 2*round + i + 1
					attempt := d.first
					if d.iteration == 0 {
						attempt = delivered
					}
					journals[i], openErrs[i] = Open(ctx, c.js, c.pub,
						task(runID, d.iteration, attempt, delivered))
				}(i)
			}
			wg.Wait()
			writers := 0
			for i := range journals {
				if openErrs[i] != nil {
					if !errors.Is(openErrs[i], ErrFenced) {
						t.Fatalf("round %d delivery %d: %v", round, i+1, openErrs[i])
					}
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
				_, err := journals[i].Append(ctx, TypeEntry, []byte(`1`))
				cancel()
				switch {
				case err == nil:
					writers++
				case !errors.Is(err, ErrFenced):
					t.Fatalf("round %d delivery %d Append: %v", round, i+1, err)
				}
			}
			if writers != 1 {
				t.Fatalf("round %d: %d writers, want exactly 1", round, writers)
			}
			// Next round: two later deliveries of the same message race
			// again and must fence this round's survivor.
		}
	})
}

func TestConformance_StaleIterationStopsWithoutWriting(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		first := open(t, c, task("r-stale", 0, 1, 1))
		if _, err := first.Commit(ctx, Commit{Action: ActionContinue, State: []byte("s0")}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		open(t, c, task("r-stale", 1, 1, 1))
		before := countRecords(t, c, "r-stale")
		_, err := Open(ctx, c.js, c.pub, task("r-stale", 0, 2, 2))
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
		first := open(t, c, task("r-scope", 0, 1, 1))
		if _, err := first.Commit(ctx, Commit{Action: ActionContinue}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		live := open(t, c, task("r-scope", 1, 1, 1))
		// A redelivery of iteration 0 whose tail read predates the live
		// claim: its claim lands at the tail behind the live worker.
		late := task("r-scope", 0, 2, 2)
		lateClaimSeq := forge(t, nc, late, TypeClaim, 0, live.Seq(), []byte(`{}`))
		if _, err := live.Append(ctx, TypeEntry, []byte(`1`)); err != nil {
			t.Fatalf("live Append after an earlier-iteration claim: %v", err)
		}
		if live.Fenced() {
			t.Error("a claim for an earlier iteration must not fence the live worker")
		}
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
		first := open(t, c, task("r-commit", 0, 1, 1))
		if _, err := first.Append(ctx, TypeEntry, []byte(`"a"`)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if _, err := first.Commit(ctx, Commit{Action: ActionContinue, State: []byte("s0")}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if _, err := first.Append(ctx, TypeEntry, []byte(`"late"`)); !errors.Is(err, ErrCommitted) {
			t.Errorf("Append after Commit err = %v, want ErrCommitted", err)
		}
		next := open(t, c, task("r-commit", 1, 1, 1))
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
		// A retry after a failure: same iteration, new attempt.
		retry := open(t, c, task("r-commit", 1, 2, 1))
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

func TestConformance_OwnAttemptCommitIsHonouredAcrossDeliveries(t *testing.T) {
	forEachDispatch(t, func(t *testing.T, nc *nats.Conn, runID string, d dispatch) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		crashed := open(t, c, task(runID, d.iteration, d.first, 1))
		if _, err := crashed.Commit(ctx, Commit{
			Action: ActionComplete, Output: json.RawMessage(`"done"`),
		}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		before := countRecords(t, c, runID)
		redelivered := open(t, c, task(runID, d.iteration, d.redelivered, 2))
		commit, ok := redelivered.Committed()
		if d.redelivered == d.first {
			// Stamped attempt: the redelivery is the same attempt and
			// honours the commit without repeating the turn.
			if !ok || commit.Action != ActionComplete || string(commit.Output) != `"done"` {
				t.Fatalf("Committed = %+v, %v; want the complete commit", commit, ok)
			}
			if got := countRecords(t, c, runID); got != before {
				t.Errorf("records = %d, want %d: a recovered attempt claims nothing", got, before)
			}
			if _, err := redelivered.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrCommitted) {
				t.Errorf("Append after recovery err = %v, want ErrCommitted", err)
			}
			return
		}
		// Iteration 0: the redelivery is a new attempt and must not
		// honour another attempt's commit.
		if ok {
			t.Error("another attempt must not honour attempt 1's commit")
		}
		if _, err := crashed.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrCommitted) {
			t.Errorf("crashed writer Append err = %v, want ErrCommitted", err)
		}
	})
}

func TestConformance_FalseRejectionLostAck(t *testing.T) {
	forEachTopology(t, func(t *testing.T, nc *nats.Conn) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		tk := task("r-lostack", 0, 1, 1)
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
		tk := task("r-owntail", 0, 1, 1)
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

func TestConformance_PauseRecordResumesOnRedelivery(t *testing.T) {
	forEachDispatch(t, func(t *testing.T, nc *nats.Conn, runID string, d dispatch) {
		c := newClient(t, nc)
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		defer cancel()
		first := open(t, c, task(runID, d.iteration, d.first, 1))
		if _, err := first.Pause(ctx, "cooldown", 50*time.Millisecond); err != nil {
			t.Fatalf("Pause: %v", err)
		}
		resumed := open(t, c, task(runID, d.iteration, d.redelivered, 2))
		if name, ok := resumed.Resume(); !ok || name != "cooldown" {
			t.Errorf("Resume = %q, %v; want cooldown", name, ok)
		}
		if _, err := resumed.Append(ctx, TypeEntry, []byte(`1`)); err != nil {
			t.Fatalf("Append after resume: %v", err)
		}
		if _, err := first.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrFenced) {
			t.Errorf("paused delivery Append err = %v, want ErrFenced", err)
		}
		third := open(t, c, task(runID, d.iteration, d.redelivered, 3))
		if _, ok := third.Resume(); ok {
			t.Error("an entry after the pause means this delivery is not a resume")
		}
	})
}

// worker/journal_test.go
// The worker side of the agent journal (ADR-026): Journal() claims the
// dispatch; Continue, Complete and Fail commit one journal record
// before their lifecycle event, which carries the commit's sequence
// and, for a journal step, no state (the journal is the state of
// record); LoadCheckpoint reads the last committed state and the
// pause marker from the journal; a stale task is acknowledged without
// writing; an attempt whose commit already exists republishes it; and
// a worker stalled past its AckWait is fenced by its successor.
//
// Methodology: taskContexts built from a TaskPayload the way the
// Worker builds one, against an embedded NATS server with the journal
// provisioned, plus one real orchestrator with two real Workers for
// the takeover. Dispatch is modelled as the engine does it: from
// iteration 1 on the payload carries the attempt, so a stalled worker
// and its AckWait redelivery differ only in the message's delivery
// count. Every wait is bounded.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const journalTestTimeout = 10 * time.Second

func journalServer(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return nc, js
}

// journalPayload is the engine's payload for one dispatch: attempt 1
// stamped on every iteration (ADR-026), so a redelivery of the
// message keeps it and differs only in delivery count.
func journalPayload(runID string, iteration int) protocol.TaskPayload {
	return protocol.TaskPayload{
		RunID: runID, StepID: "s", Iteration: iteration, Attempt: 1,
		DispatchNonce: "nonce", StateMaxBytes: protocol.LoopStateMaxBytesDefault,
	}
}

// deliveredMsg is a task message delivered n times.
type deliveredMsg struct {
	testJetstreamMsg
	delivered uint64
	streamSeq uint64
}

func (m *deliveredMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		NumDelivered: m.delivered,
		Sequence:     jetstream.SequencePair{Stream: m.streamSeq},
	}, nil
}

// delivery builds the taskContext for the n-th delivery of payload,
// with step.started published as the Worker does before the handler.
func delivery(t *testing.T, nc *nats.Conn, payload protocol.TaskPayload, n int) *taskContext {
	t.Helper()
	tc := contextForPayload(t, nc, payload, nil)
	tc.msg = &deliveredMsg{delivered: uint64(n)}
	tc.publishMsg = tc.tp.JSPublishMsg
	if err := tc.publishStarted(tc.msg); err != nil {
		t.Fatalf("publishStarted: %v", err)
	}
	return tc
}

func readJournal(t *testing.T, js jetstream.JetStream, runID string) []journal.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	records, err := journal.Read(ctx, js, runID, "s")
	if err != nil {
		t.Fatalf("journal.Read: %v", err)
	}
	return records
}

// nextHistory returns the first history event of runID whose type is
// want.
func nextHistory(
	t *testing.T, nc *nats.Conn, runID string, want protocol.EventType,
) protocol.Event {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("history."+runID, nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	for i := 0; i < 50; i++ {
		msg, err := sub.NextMsg(journalTestTimeout)
		if err != nil {
			t.Fatalf("no %s on history: %v", want, err)
		}
		evt, err := protocol.UnmarshalEvent(msg.Data)
		if err != nil {
			t.Fatalf("decode event: %v", err)
		}
		if evt.Type == want {
			return evt
		}
	}
	t.Fatalf("no %s within 50 events", want)
	return protocol.Event{}
}

func TestJournal_ContinueCommitsBeforeEventAndCarriesNoState(t *testing.T) {
	nc, js := journalServer(t)
	tc := delivery(t, nc, journalPayload("run-jc", 0), 1)
	j, err := tc.Journal()
	if err != nil {
		t.Fatalf("Journal: %v", err)
	}
	if j.Task().Attempt != 1 || j.Task().Iteration != 0 {
		t.Errorf("claimed task = %+v, want iteration 0 attempt 1", j.Task())
	}
	if err := tc.Checkpoint([]byte("s1")); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := tc.Continue([]byte(`"out"`)); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	records := readJournal(t, js, "run-jc")
	if len(records) != 2 || records[1].Type != journal.TypeCommit {
		t.Fatalf("records = %+v, want claim then commit", records)
	}
	var commit journal.Commit
	if err := json.Unmarshal(records[1].Data, &commit); err != nil {
		t.Fatalf("decode commit: %v", err)
	}
	if commit.Action != journal.ActionContinue || string(commit.State) != "s1" {
		t.Errorf("commit = %+v, want continue with state s1", commit)
	}
	evt := nextHistory(t, nc, "run-jc", protocol.EventStepContinue)
	if evt.JournalSeq != records[1].Seq {
		t.Errorf("event journal_seq = %d, want the commit's %d", evt.JournalSeq, records[1].Seq)
	}
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.StateSet || len(payload.State) != 0 {
		t.Errorf("continue payload carries state %q; the journal is the state of record", payload.State)
	}
	if string(payload.Output) != `"out"` {
		t.Errorf("output = %s, want \"out\"", payload.Output)
	}
}

func TestJournal_LoadCheckpointReadsCommittedStateNotPayload(t *testing.T) {
	nc, js := journalServer(t)
	pub := natsutil.NewTracingPublisher(nc, js)
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	first, err := journal.Open(ctx, js, pub, journal.Task{
		RunID: "run-jl", StepID: "s", Attempt: 1, DispatchNonce: "nonce", Delivered: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.Commit(ctx, journal.Commit{
		Action: journal.ActionContinue, State: []byte("s0"),
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	payload := journalPayload("run-jl", 1)
	payload.State = []byte("engine-state")
	tc := delivery(t, nc, payload, 1)
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	state, err := tc.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if string(state) != "s0" {
		t.Errorf("LoadCheckpoint = %q, want the journal's s0, not the payload's", state)
	}
	if err := tc.Checkpoint([]byte("s1")); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if state, _ := tc.LoadCheckpoint(); string(state) != "s1" {
		t.Errorf("LoadCheckpoint after Checkpoint = %q, want s1", state)
	}
}

func TestJournal_StaleTaskStopsWithoutWritingAndAcks(t *testing.T) {
	nc, js := journalServer(t)
	pub := natsutil.NewTracingPublisher(nc, js)
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	if _, err := journal.Open(ctx, js, pub, journal.Task{
		RunID: "run-js", StepID: "s", Iteration: 1, Attempt: 1, DispatchNonce: "nonce",
		Delivered: 1,
	}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	tc := delivery(t, nc, journalPayload("run-js", 0), 2)
	_, err := tc.Journal()
	if !errors.Is(err, journal.ErrStale) {
		t.Fatalf("Journal err = %v, want ErrStale", err)
	}
	if got := len(readJournal(t, js, "run-js")); got != 1 {
		t.Errorf("records = %d, want 1: a stale task writes nothing", got)
	}
	done, ack := tc.journalOutcome(err)
	if !done || !ack {
		t.Errorf("journalOutcome = done %v ack %v, want the task acknowledged", done, ack)
	}
	if err := tc.Complete(nil); !errors.Is(err, journal.ErrStale) {
		t.Errorf("Complete on a stale task err = %v, want ErrStale", err)
	}
}

func TestJournal_OwnAttemptCommitIsRepublishedNotRepeated(t *testing.T) {
	nc, js := journalServer(t)
	pub := natsutil.NewTracingPublisher(nc, js)
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	// Iteration 1 with the stamped attempt: the first delivery commits
	// and crashes before publishing; the second delivery is the same
	// attempt and must republish, not repeat the turn.
	crashed, err := journal.Open(ctx, js, pub, journal.Task{
		RunID: "run-jo", StepID: "s", Iteration: 1, Attempt: 1, DispatchNonce: "nonce",
		Delivered: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	seq, err := crashed.Commit(ctx, journal.Commit{
		Action: journal.ActionComplete, Output: json.RawMessage(`"o"`),
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tc := delivery(t, nc, journalPayload("run-jo", 1), 2)
	_, err = tc.Journal()
	if !errors.Is(err, journal.ErrCommitted) {
		t.Fatalf("Journal err = %v, want ErrCommitted", err)
	}
	evt := nextHistory(t, nc, "run-jo", protocol.EventStepCompleted)
	if string(evt.Payload) != `"o"` || evt.AttemptNumber != 1 || evt.JournalSeq != seq {
		t.Errorf("republished event = %+v, want output \"o\", attempt 1, journal_seq %d", evt, seq)
	}
	records := readJournal(t, js, "run-jo")
	commits := 0
	for _, r := range records {
		if r.Type == journal.TypeCommit {
			commits++
		}
	}
	if len(records) != 2 || commits != 1 {
		t.Errorf("records = %d with %d commits, want 2 and 1: recovery appends nothing",
			len(records), commits)
	}
	if done, ack := tc.journalOutcome(err); !done || !ack {
		t.Errorf("journalOutcome = done %v ack %v, want acknowledged", done, ack)
	}
}

func TestJournal_PauseWritesRecordAndResumeMarker(t *testing.T) {
	nc, js := journalServer(t)
	tc := delivery(t, nc, journalPayload("run-jp", 1), 1)
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	if err := tc.Pause("cooldown", 10*time.Millisecond); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	records := readJournal(t, js, "run-jp")
	if len(records) != 2 || records[1].Type != journal.TypePause {
		t.Fatalf("records = %+v, want claim then pause", records)
	}
	resumed := delivery(t, nc, journalPayload("run-jp", 1), 2)
	if _, err := resumed.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	state, err := resumed.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	var marker map[string]string
	if err := json.Unmarshal(state, &marker); err != nil || marker["pause_resume"] != "cooldown" {
		t.Errorf("LoadCheckpoint = %s (%v), want the pause marker for cooldown", state, err)
	}
	if err := tc.Complete(nil); !errors.Is(err, journal.ErrFenced) {
		t.Errorf("paused delivery Complete err = %v, want ErrFenced by the resumed one", err)
	}
}

func TestJournal_StalledWorkerIsFencedBySuccessor(t *testing.T) {
	for _, stallAt := range []int{0, 1} {
		t.Run(fmt.Sprintf("iteration%d", stallAt), func(t *testing.T) {
			testStalledWorkerFenced(t, stallAt)
		})
	}
}

// testStalledWorkerFenced stalls the first delivery of iteration
// stallAt past AckWait; the redelivery, which from iteration 1 carries
// the same attempt, takes over.
func testStalledWorkerFenced(t *testing.T, stallAt int) {
	t.Helper()
	runID := fmt.Sprintf("run-takeover-%d", stallAt)
	nc, js, jsNew, defData := startLoopEngine(t, "wf-journal-"+runID, "journal-task-"+runID)
	release := make(chan struct{})
	stalledFenced := make(chan error, 1)
	claimed := make(chan struct{}, 2)
	handler := func(tc TaskContext) error {
		j, err := tc.Journal()
		if err != nil {
			return err
		}
		if j.Task().Iteration < stallAt {
			return tc.Continue(nil)
		}
		claimed <- struct{}{}
		if j.Task().Delivered == 1 {
			<-release
			_, err := j.Append(tc.Context(), journal.TypeEntry, []byte(`"late"`))
			stalledFenced <- err
			if err != nil {
				return err
			}
		}
		return tc.Complete([]byte(`"by-attempt-2"`))
	}
	// Two Workers: the first delivery blocks in one, the redelivery
	// after AckWait goes to the other.
	startFencingWorker(t, nc, "journal-task-"+runID, handler)
	startFencingWorker(t, nc, "journal-task-"+runID, handler)
	startStepRun(t, js, runID, defData)
	waitClosed(t, claimed, "first claim")
	// The first worker stalls past AckWait; the redelivery is claimed.
	waitClosed(t, claimed, "second claim")
	run := waitStepSettled(t, jsNew, runID, journalTestTimeout)
	step := run.Steps["s"]
	if step.Status != dag.StepStatusCompleted || string(step.Output) != `"by-attempt-2"` {
		t.Fatalf("step = %+v, want completed by attempt 2", run.Steps["s"])
	}
	close(release)
	select {
	case err := <-stalledFenced:
		if !errors.Is(err, journal.ErrFenced) {
			t.Errorf("stalled append err = %v, want ErrFenced", err)
		}
	case <-time.After(journalTestTimeout):
		t.Fatal("stalled worker never resumed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	report, err := journal.Check(ctx, nc, runID)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !report.OK() || len(report.Steps) != 1 || report.Steps[0].Commits != stallAt+1 {
		t.Errorf("report = %+v, want %d commits and no violations", report, stallAt+1)
	}
	if run.Steps["s"].Iterations != stallAt {
		t.Errorf("iterations = %d, want %d", run.Steps["s"].Iterations, stallAt)
	}
}

// A handler that returns the ErrFenced of its own j.Append, rather
// than of a Journal or commit call, must still leave the message to
// the successor: acknowledging or NAKing it would redeliver the task
// the successor is running and fence the live claim.
func TestJournal_FencedAppendErrorLeavesMessageToSuccessor(t *testing.T) {
	nc, _ := journalServer(t)
	payload := journalPayload("run-fenced-append", 1)
	stalled := delivery(t, nc, payload, 1)
	j, err := stalled.Journal()
	if err != nil {
		t.Fatalf("first Journal: %v", err)
	}
	successor := delivery(t, nc, payload, 2)
	if _, err := successor.Journal(); err != nil {
		t.Fatalf("successor Journal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	_, err = j.Append(ctx, journal.TypeEntry, []byte(`"late"`))
	if !errors.Is(err, journal.ErrFenced) {
		t.Fatalf("stalled append err = %v, want ErrFenced", err)
	}
	done, ack := stalled.journalOutcome(err)
	if !done || ack {
		t.Fatalf("journalOutcome = done %v ack %v, want done and no ack", done, ack)
	}
	if stalled.journalStop != journalFenced {
		t.Fatalf("journalStop = %v, want fenced", stalled.journalStop)
	}
}

// A dispatch resend is a fresh task message: delivery count 1 again,
// with the nonce, attempt and iteration of the original still in
// flight. Its stream sequence is what makes its claim fence the
// original's.
func TestJournal_ResendClaimFencesOriginalWithSameDelivery(t *testing.T) {
	nc, _ := journalServer(t)
	payload := journalPayload("run-resend", 1)
	original := delivery(t, nc, payload, 1)
	original.msg.(*deliveredMsg).streamSeq = 10
	j, err := original.Journal()
	if err != nil {
		t.Fatalf("original Journal: %v", err)
	}
	resend := delivery(t, nc, payload, 1)
	resend.msg.(*deliveredMsg).streamSeq = 11
	if _, err := resend.Journal(); err != nil {
		t.Fatalf("resend Journal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	_, err = j.Append(ctx, journal.TypeEntry, []byte(`"late"`))
	if !errors.Is(err, journal.ErrFenced) {
		t.Fatalf("original append err = %v, want ErrFenced", err)
	}
}

// A Complete retried after its commit landed but its publish failed
// must republish the stored commit's event and settle the task for ack,
// not surface the wrapped ErrCommitted as a failure.
func TestJournal_RetriedCompleteRepublishesStoredCommit(t *testing.T) {
	nc, _ := journalServer(t)
	const runID = "run-retried-complete"
	tc := delivery(t, nc, journalPayload(runID, 0), 1)
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	// The first Complete: its commit lands, its publish "fails".
	if err := tc.commitJournal(journal.Commit{
		Action: journal.ActionComplete, Output: json.RawMessage(`{"r":1}`),
	}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	seq := tc.journalSeq
	err := tc.Complete([]byte(`{"r":1}`))
	if !errors.Is(err, journal.ErrCommitted) {
		t.Fatalf("retried Complete err = %v, want ErrCommitted", err)
	}
	evt := nextHistory(t, nc, runID, protocol.EventStepCompleted)
	if string(evt.Payload) != `{"r":1}` || evt.JournalSeq != seq || seq == 0 {
		t.Fatalf("republished event = %+v, want payload {\"r\":1} and journal_seq %d", evt, seq)
	}
	if done, ack := tc.journalOutcome(err); !done || !ack {
		t.Fatalf("journalOutcome = done %v ack %v, want both", done, ack)
	}
}

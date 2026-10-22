// Package journal is the agent journal of ADR-026: one append-only
// JetStream subject per step, journal.{run}.{step}, where an agent
// step's SDK records what it did so a successor can take over without
// repeating a turn. Every append is fenced on the journal tail its
// writer last saw, so a worker that lost the tail (stalled past its
// AckWait, superseded by a redelivery) cannot write, and is told so.
//
// A Journal is one claim on one dispatch of a step. Open reads the
// tail from the stream leader, stops without writing when the step
// has moved to a later iteration, rebuilds what this iteration needs
// from the records so far, and appends a claim record. Append and
// Commit then carry Nats-Expected-Last-Subject-Sequence and a stable
// Nats-Msg-Id. A rejected append is not fencing by itself: the writer
// re-reads the tail and only a record from another claim of the same
// or a later iteration fences it.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/danmestas/dagnats/internal/chaos"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Record types. Handlers write TypeEntry; the SDK writes the rest.
// The model and tool types are reserved for the helpers of #805.
const (
	TypeClaim         = "claim"
	TypePause         = "pause"
	TypeCommit        = "commit"
	TypeEntry         = "entry"
	TypeModelResponse = "model.response"
	TypeToolIntent    = "tool.intent"
	TypeToolResult    = "tool.result"
	TypeInputPlaced   = "input.placed"
)

// Commit actions: what the iteration's lifecycle event will say.
const (
	ActionContinue = "continue"
	ActionComplete = "complete"
	ActionFail     = "fail"
)

// Record headers. The identity lives in headers so a tail read can
// classify a record without decoding its body.
const (
	HeaderType      = "Dagnats-Journal-Type"
	HeaderIteration = "Dagnats-Journal-Iteration"
	HeaderAttempt   = "Dagnats-Journal-Attempt"
	HeaderIndex     = "Dagnats-Journal-Index"
	HeaderDelivered = "Dagnats-Journal-Delivered"
	HeaderStreamSeq = "Dagnats-Journal-Stream-Seq"
	HeaderNonce     = "Dagnats-Dispatch-Nonce"

	headerExpectedLastSeq = "Nats-Expected-Last-Subject-Sequence"
	headerMsgID           = "Nats-Msg-Id"
)

// Bounds.
const (
	// appendRetryMax bounds the resync loop after a rejected append.
	appendRetryMax = 8
	// recordsMax bounds one journal read. A step that writes more has
	// outgrown a per-step journal.
	recordsMax = 10_000
	// readBatch is the fetch size of one journal read.
	readBatch = 256
	// readWait bounds one fetch of a journal read.
	readWait = 5 * time.Second
	// resyncBackoff is the wait before retrying when the tail has not
	// moved: a rival append on the subject is still in process on the
	// server (its code 10164), and will have landed or failed by then.
	resyncBackoff = 10 * time.Millisecond
)

// Sentinel errors a handler is expected to check.
var (
	// ErrStale: the journal tail belongs to a later iteration, so this
	// task is a late redelivery. Nothing was written; the task is done.
	ErrStale = errors.New("journal: task is stale, a later iteration holds the step")
	// ErrFenced: a record from another claim follows this writer's.
	// Nothing more may be written or published for this task.
	ErrFenced = errors.New("journal: fenced by another claim")
	// ErrCommitted: this attempt already committed the iteration.
	// Open found the commit; the caller republishes its event.
	ErrCommitted = errors.New("journal: iteration already committed by this attempt")
	// ErrRestartLimit: the crash-loop guard failed the step (ADR-028).
	// The worker has published step.failed; the handler must return.
	ErrRestartLimit = errors.New("journal: restart limit reached, the step has been failed")
	// ErrNotProvisioned: the stream is missing.
	ErrNotProvisioned = errors.New(
		"journal: AGENT_JOURNAL stream not provisioned" +
			" — ensure natsutil.SetupAll() has been called" +
			" or start the server with 'dagnats serve'",
	)
)

// Task identifies one dispatch of a step: the identity every record
// carries and every lifecycle event names (ADR-025).
type Task struct {
	RunID         string
	StepID        string
	Iteration     int
	Attempt       int
	DispatchNonce string
	// Delivered is the delivery count of the task message. It is part
	// of the claim identity: from iteration 1 the engine stamps the
	// attempt, so a stalled worker and the AckWait redelivery that
	// replaces it share run, step, iteration, attempt and nonce, and
	// only the delivery count tells their claims apart. It also feeds
	// the crash-loop guard (#807).
	Delivered int
	// StreamSeq is the task message's sequence in the task stream.
	// Redeliveries share it; a dispatch resend is a fresh message with
	// a new one and delivery count 1 again, so only it tells a resend's
	// claim from the original's while that is still in flight. Zero
	// when unknown.
	StreamSeq uint64
}

// Record is one journal message.
type Record struct {
	Seq           uint64          `json:"seq"`
	Type          string          `json:"type"`
	Iteration     int             `json:"iteration"`
	Attempt       int             `json:"attempt"`
	DispatchNonce string          `json:"dispatch_nonce,omitempty"`
	Delivered     int             `json:"delivered"`
	StreamSeq     uint64          `json:"stream_seq,omitempty"`
	Index         int             `json:"index"`
	Time          time.Time       `json:"time"`
	Data          json.RawMessage `json:"data,omitempty"`
}

// sameClaim reports whether r was written by the claim for task: the
// same dispatch and the same delivery of it.
func (r Record) sameClaim(task Task) bool {
	return r.Iteration == task.Iteration && r.Attempt == task.Attempt &&
		r.DispatchNonce == task.DispatchNonce && r.Delivered == task.Delivered &&
		r.StreamSeq == task.StreamSeq
}

// Claim is the body of a claim record.
type Claim struct {
	Iteration     int    `json:"iteration"`
	Attempt       int    `json:"attempt"`
	DispatchNonce string `json:"dispatch_nonce,omitempty"`
	Delivered     int    `json:"delivered"`
	StreamSeq     uint64 `json:"stream_seq,omitempty"`
}

// Pause is the body of a pause record.
type Pause struct {
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
}

// Commit is the body of a commit record: the iteration's outcome and
// the state the next iteration starts from.
type Commit struct {
	Action       string          `json:"action"`
	Output       json.RawMessage `json:"output,omitempty"`
	State        []byte          `json:"state,omitempty"`
	Error        string          `json:"error,omitempty"`
	FailureType  string          `json:"failure_type,omitempty"`
	RetryAfterMs int64           `json:"retry_after_ms,omitempty"`
	// Wait is the durable wait a continue asked for (ADR-027), so a
	// republished step.continue carries the same one.
	Wait *protocol.ContinueWait `json:"wait,omitempty"`
}

// Journal is one claim on one dispatch of a step. A handle belongs to
// one goroutine: replay matches tool and model calls by position, so
// calls issued from several goroutines would replay in another order.
// A handler running a model's parallel tool calls issues them through
// the journal one at a time. Concurrent use panics; a tool function
// may call back into the journal from the goroutine that owns it.
type Journal struct {
	js     jetstream.JetStream
	pub    *natsutil.TracingPublisher
	stream jetstream.Stream
	task   Task
	// seq is the last journal sequence this writer saw; next is the
	// index of its next record (the claim is 0).
	seq  uint64
	next int
	// fenced is set once a record from another claim is seen at the
	// tail; done once this attempt's commit is stored or found.
	fenced bool
	done   bool
	// killed is set by the test-only crash seam (internal/chaos): the
	// holder died here and writes nothing more.
	killed bool
	// Rebuilt by Open from the records before the claim.
	state        []byte
	records      []Record
	committed    *Commit
	committedSeq uint64
	resume       string
	// lastCommitSeq is the iteration's newest commit before the claim:
	// records at or before it belong to an attempt the engine retried.
	lastCommitSeq uint64
	// restarts is the number of claims on this iteration at the tail
	// before this one with nothing else between them: the crash-loop
	// guard's count (ADR-028).
	restarts int
	// toolCalls and modelCalls are the positions of the next Tool and
	// Model calls, matched against a previous holder's records.
	toolCalls  int
	modelCalls int
	// honoured holds the sequences of prior-attempt unsafe intents
	// this attempt has already answered.
	honoured map[uint64]bool
	// owner is the goroutine inside a public method, 0 when none;
	// depth counts its nested calls (owner.go).
	owner atomic.Uint64
	depth int
	// placed is every input.placed record so far, any iteration, plus
	// this claim's own (ADR-027).
	placed []InputPlaced
}

// MsgID is the stable dedup key of a record:
// {run}.{step}.{attempt}.{iteration}.{stream_seq}.{delivered}.{index}.
// The delivery count keeps a redelivery's records distinct from the
// stalled delivery's inside the dedup window; the stream sequence does
// the same for a resend, which restarts the count. Without them the
// successor's claim would be answered with the old claim's sequence.
func MsgID(task Task, index int) string {
	if task.RunID == "" || task.StepID == "" {
		panic("MsgID: run and step must not be empty")
	}
	if index < 0 || task.Delivered < 1 {
		panic("MsgID: index must not be negative and delivered must be >= 1")
	}
	return task.RunID + "." + task.StepID + "." + strconv.Itoa(task.Attempt) +
		"." + strconv.Itoa(task.Iteration) + "." +
		strconv.FormatUint(task.StreamSeq, 10) + "." + strconv.Itoa(task.Delivered) +
		"." + strconv.Itoa(index)
}

// Open claims task's dispatch. It returns ErrStale when the tail
// belongs to a later iteration (nothing written), ErrFenced when a
// rival claim for the same iteration won, and a journal whose
// Committed reports this attempt's own commit when the previous
// holder crashed between committing and publishing (no claim is
// appended then).
func Open(
	ctx context.Context, js jetstream.JetStream, pub *natsutil.TracingPublisher,
	task Task,
) (*Journal, error) {
	if ctx == nil {
		panic("journal.Open: ctx must not be nil")
	}
	if task.RunID == "" || task.StepID == "" {
		panic("journal.Open: run and step must not be empty")
	}
	if task.Iteration < 0 || task.Attempt < 1 || task.Delivered < 1 {
		panic("journal.Open: iteration must be >= 0, attempt and delivered >= 1")
	}
	j := &Journal{task: task, honoured: map[uint64]bool{}}
	j.attach(js, pub)
	if err := j.bind(ctx); err != nil {
		return nil, err
	}
	tail, err := j.readTail(ctx)
	if err != nil {
		return nil, err
	}
	if tail.Seq > 0 && tail.Iteration > task.Iteration {
		return nil, ErrStale
	}
	records, err := j.readUpTo(ctx, tail.Seq)
	if err != nil {
		return nil, err
	}
	j.rebuild(records)
	j.seq = tail.Seq
	if j.committed != nil {
		j.done = true
		return j, nil
	}
	claim, err := json.Marshal(Claim{
		Iteration: task.Iteration, Attempt: task.Attempt,
		DispatchNonce: task.DispatchNonce, Delivered: task.Delivered,
		StreamSeq: task.StreamSeq,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal claim: %w", err)
	}
	if _, err := j.append(ctx, TypeClaim, claim); err != nil {
		return nil, err
	}
	return j, nil
}

// attach sets the connections. Separate from Open so a test can build
// a writer that stands in for a stale claimer.
func (j *Journal) attach(js jetstream.JetStream, pub *natsutil.TracingPublisher) {
	if js == nil {
		panic("journal: js must not be nil")
	}
	if pub == nil {
		panic("journal: pub must not be nil")
	}
	j.js = js
	j.pub = pub
}

// bind resolves the stream handle, once per journal.
func (j *Journal) bind(ctx context.Context) error {
	if j.js == nil {
		panic("journal.bind: js must not be nil")
	}
	if j.stream != nil {
		return nil
	}
	stream, err := j.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return ErrNotProvisioned
		}
		return fmt.Errorf("journal: stream: %w", err)
	}
	j.stream = stream
	return nil
}

// Task returns the dispatch this journal claims.
func (j *Journal) Task() Task { return j.task }

// crashAt runs the test-only crash seam at p for this claim; nil in
// production. A kill marks the journal so nothing more is appended.
func (j *Journal) crashAt(ctx context.Context, p chaos.Point) error {
	if j.task.RunID == "" {
		panic("journal.crashAt: task must name a run")
	}
	if j.task.Delivered < 1 {
		panic("journal.crashAt: delivered must be >= 1")
	}
	if !chaos.Installed() {
		return nil
	}
	err := chaos.At(ctx, p, chaos.Site{
		RunID: j.task.RunID, StepID: j.task.StepID, Iteration: j.task.Iteration,
		Attempt: j.task.Attempt, Delivered: j.task.Delivered,
	})
	if chaos.Killed(err) {
		j.killed = true
	}
	return err
}

// Seq returns the last journal sequence this writer saw.
func (j *Journal) Seq() uint64 { return j.seq }

// Fenced reports whether a record from another claim fenced this one.
func (j *Journal) Fenced() bool { return j.fenced }

// State returns the state of the last commit before this iteration,
// nil when there is none.
func (j *Journal) State() []byte { return j.state }

// Records returns this iteration's records written before this claim,
// in journal order: what a recovering attempt may reuse.
func (j *Journal) Records() []Record { return j.records }

// Committed returns this attempt's own commit: the one Open found, or
// the one Commit stored.
func (j *Journal) Committed() (Commit, bool) {
	if j.committed == nil {
		return Commit{}, false
	}
	return *j.committed, true
}

// CommittedSeq returns the journal sequence of the commit Committed
// reports, 0 when there is none.
func (j *Journal) CommittedSeq() uint64 { return j.committedSeq }

// Resume returns the pause name when the newest record of this
// iteration before the claim is a pause: this delivery resumes it.
func (j *Journal) Resume() (string, bool) {
	return j.resume, j.resume != ""
}

// Restarts returns how many claims on this iteration sat at the tail
// before this claim with nothing between them: deliveries that
// claimed and then made no progress. A pause, an entry, a tool or
// model record, or a commit resets the count; a claim after a commit
// (a retry) starts from 0. Feeds the worker's restart limit
// (ADR-028).
func (j *Journal) Restarts() int { return j.restarts }

// Append writes a record of recordType with data as its body.
func (j *Journal) Append(
	ctx context.Context, recordType string, data []byte,
) (uint64, error) {
	if recordType == "" {
		panic("journal.Append: recordType must not be empty")
	}
	if recordType == TypeClaim || recordType == TypeCommit {
		panic("journal.Append: claim and commit are written by Open and Commit")
	}
	defer j.enter("Append")()
	return j.append(ctx, recordType, data)
}

// Pause writes the pause record Pause publishes before its delayed
// redelivery; the next claim on this iteration sees it in Resume.
func (j *Journal) Pause(
	ctx context.Context, name string, duration time.Duration,
) (uint64, error) {
	if name == "" {
		panic("journal.Pause: name must not be empty")
	}
	if duration <= 0 {
		panic("journal.Pause: duration must be positive")
	}
	defer j.enter("Pause")()
	data, err := json.Marshal(Pause{Name: name, DurationMs: duration.Milliseconds()})
	if err != nil {
		return 0, fmt.Errorf("marshal pause: %w", err)
	}
	return j.append(ctx, TypePause, data)
}

// Commit ends the iteration with one record: the action its lifecycle
// event will carry and the state the next iteration starts from. One
// message, not an atomic batch (ADR-026). After it nothing more may
// be appended under this claim.
func (j *Journal) Commit(ctx context.Context, c Commit) (uint64, error) {
	switch c.Action {
	case ActionContinue, ActionComplete, ActionFail:
	default:
		panic("journal.Commit: action must be continue, complete or fail")
	}
	if ctx == nil {
		panic("journal.Commit: ctx must not be nil")
	}
	defer j.enter("Commit")()
	data, err := json.Marshal(c)
	if err != nil {
		return 0, fmt.Errorf("marshal commit: %w", err)
	}
	seq, err := j.append(ctx, TypeCommit, data)
	if err != nil {
		return 0, err
	}
	j.done = true
	j.committed, j.committedSeq = &c, seq
	return seq, nil
}

// append is the fenced write with the rejection protocol of ADR-026.
func (j *Journal) append(
	ctx context.Context, recordType string, data []byte,
) (uint64, error) {
	if j.pub == nil {
		panic("journal.append: pub must not be nil")
	}
	if j.next < 0 {
		panic("journal.append: next must not be negative")
	}
	if j.fenced {
		return 0, ErrFenced
	}
	if j.done {
		return 0, ErrCommitted
	}
	if j.killed {
		return 0, chaos.ErrKilled
	}
	msgID := MsgID(j.task, j.next)
	expected := j.seq
	for try := 0; try < appendRetryMax; try++ {
		msg := recordMessage(j.task, recordType, j.next, expected, data)
		ack, err := j.pub.JSPublishMsg(ctx, msg)
		if err == nil {
			return j.advance(ack.Sequence)
		}
		if !wrongLastSequence(err) {
			return 0, fmt.Errorf("journal: append %s: %w", recordType, err)
		}
		tail, err := j.readTail(ctx)
		if err != nil {
			return 0, err
		}
		switch {
		case tail.Seq == expected, tail.Seq == 0:
			// Unmoved: a rival publish is in process. Empty: the subject
			// was purged under us. Both wait before retrying.
			time.Sleep(resyncBackoff * time.Duration(try+1))
			expected = tail.Seq
		case tail.msgID == msgID:
			// Landed; the acknowledgement was lost.
			return j.advance(tail.Seq)
		case tail.sameClaim(j.task), tail.Iteration < j.task.Iteration:
			// Own earlier record, or a late claim for an earlier
			// iteration: resync, never fence.
			expected = tail.Seq
		default:
			j.fenced = true
			return 0, ErrFenced
		}
	}
	return 0, fmt.Errorf("journal: append %s: %d resyncs without landing",
		recordType, appendRetryMax)
}

// advance records a stored sequence for this writer's next record. A
// sequence behind the tail this writer saw means the server answered
// with an older record's sequence (a dedup hit on a record that is not
// ours): the journal has moved past us, so that is a fence, never a
// panic that would take the worker down.
func (j *Journal) advance(seq uint64) (uint64, error) {
	if seq == 0 {
		panic("journal.advance: seq must be positive")
	}
	if j.next < 0 {
		panic("journal.advance: next must not be negative")
	}
	if seq < j.seq {
		j.fenced = true
		return 0, ErrFenced
	}
	j.seq = seq
	j.next++
	return seq, nil
}

// tailRecord is the newest record on the subject, plus its msg-id.
type tailRecord struct {
	Record
	msgID string
}

// readTail reads the newest record on the subject from the stream
// leader. AGENT_JOURNAL has direct get disabled, so this is the
// JetStream API path, which only the leader answers. Seq 0 means the
// subject is empty.
func (j *Journal) readTail(ctx context.Context) (tailRecord, error) {
	if j.stream == nil {
		panic("journal.readTail: stream must be bound")
	}
	if ctx == nil {
		panic("journal.readTail: ctx must not be nil")
	}
	var raw *jetstream.RawStreamMsg
	err := retryTransient(ctx, func() error {
		var getErr error
		raw, getErr = j.stream.GetLastMsgForSubject(
			ctx, natsutil.JournalSubject(j.task.RunID, j.task.StepID),
		)
		return getErr
	})
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return tailRecord{}, nil
		}
		return tailRecord{}, fmt.Errorf("journal: read tail: %w", err)
	}
	return tailRecord{
		Record: decodeRecord(raw.Sequence, raw.Header, raw.Data, raw.Time),
		msgID:  raw.Header.Get(headerMsgID),
	}, nil
}

// readUpTo reads every record on the subject up to and including
// tailSeq, in order. An empty subject (tailSeq 0) reads nothing.
func (j *Journal) readUpTo(ctx context.Context, tailSeq uint64) ([]Record, error) {
	if j.js == nil {
		panic("journal.readUpTo: js must not be nil")
	}
	if tailSeq == 0 {
		return nil, nil
	}
	return readSubject(ctx, j.js, j.task.RunID, j.task.StepID, tailSeq)
}

// rebuild derives the recovery view from the records before the
// claim: the last committed state of an earlier iteration, this
// iteration's records, its newest pause, and this attempt's commit.
func (j *Journal) rebuild(records []Record) {
	if len(records) > recordsMax {
		panic("journal.rebuild: records exceed the read bound")
	}
	if j.task.Attempt < 1 {
		panic("journal.rebuild: attempt must be >= 1")
	}
	j.state, j.records, j.committed, j.resume, j.placed = nil, nil, nil, "", nil
	j.restarts, j.lastCommitSeq = 0, 0
	for i := range records {
		r := records[i]
		if r.Type == TypeInputPlaced {
			if p, ok := placedInput(r); ok {
				j.placed = append(j.placed, p)
			}
		}
		if r.Iteration < j.task.Iteration {
			if r.Type == TypeCommit {
				j.state = commitState(r)
			}
			continue
		}
		j.records = append(j.records, r)
		j.restarts++
		if r.Type != TypeClaim {
			j.restarts = 0
		}
		switch r.Type {
		case TypePause:
			j.resume = pauseName(r)
		case TypeCommit:
			j.resume = ""
			j.lastCommitSeq = r.Seq
			if r.Attempt == j.task.Attempt {
				var c Commit
				if err := json.Unmarshal(r.Data, &c); err == nil {
					j.committed = &c
					j.committedSeq = r.Seq
				}
			}
		default:
			j.resume = ""
		}
	}
}

// commitState decodes a commit record's state; nil on a bad body.
func commitState(r Record) []byte {
	if r.Type != TypeCommit {
		panic("commitState: record must be a commit")
	}
	var c Commit
	if err := json.Unmarshal(r.Data, &c); err != nil {
		return nil
	}
	return c.State
}

// pauseName decodes a pause record's name; "" on a bad body.
func pauseName(r Record) string {
	if r.Type != TypePause {
		panic("pauseName: record must be a pause")
	}
	var p Pause
	if err := json.Unmarshal(r.Data, &p); err != nil {
		return ""
	}
	return p.Name
}

// recordMessage builds the fenced message for one record.
func recordMessage(
	task Task, recordType string, index int, expected uint64, data []byte,
) *nats.Msg {
	if recordType == "" {
		panic("recordMessage: recordType must not be empty")
	}
	if index < 0 {
		panic("recordMessage: index must not be negative")
	}
	return &nats.Msg{
		Subject: natsutil.JournalSubject(task.RunID, task.StepID),
		Data:    data,
		Header: nats.Header{
			headerMsgID:           {MsgID(task, index)},
			headerExpectedLastSeq: {strconv.FormatUint(expected, 10)},
			HeaderType:            {recordType},
			HeaderIteration:       {strconv.Itoa(task.Iteration)},
			HeaderAttempt:         {strconv.Itoa(task.Attempt)},
			HeaderIndex:           {strconv.Itoa(index)},
			HeaderDelivered:       {strconv.Itoa(task.Delivered)},
			HeaderStreamSeq:       {strconv.FormatUint(task.StreamSeq, 10)},
			HeaderNonce:           {task.DispatchNonce},
		},
	}
}

// decodeRecord reads a record's identity from its headers.
func decodeRecord(seq uint64, hdr nats.Header, data []byte, at time.Time) Record {
	if seq == 0 {
		panic("decodeRecord: seq must be positive")
	}
	if hdr == nil {
		hdr = nats.Header{}
	}
	return Record{
		Seq:           seq,
		Type:          hdr.Get(HeaderType),
		Iteration:     headerInt(hdr, HeaderIteration),
		Attempt:       headerInt(hdr, HeaderAttempt),
		DispatchNonce: hdr.Get(HeaderNonce),
		Delivered:     headerInt(hdr, HeaderDelivered),
		StreamSeq:     headerUint(hdr, HeaderStreamSeq),
		Index:         headerInt(hdr, HeaderIndex),
		Time:          at,
		Data:          json.RawMessage(data),
	}
}

// wrongLastSequence reports whether err is the server's rejection of
// a stale expected sequence (10071, naming the real tail) or its
// refusal while another expected-sequence publish on the subject is
// still in process on a clustered stream (10164, "wrong last
// sequence" with no number). Both mean: re-read the tail.
func wrongLastSequence(err error) bool {
	if err == nil {
		panic("wrongLastSequence: err must not be nil")
	}
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == errCodeWrongLastSequenceInProcess
}

// errCodeWrongLastSequenceInProcess is nats-server's
// JSStreamWrongLastSequenceConstantErr, which nats.go does not name.
const errCodeWrongLastSequenceInProcess jetstream.ErrorCode = 10164

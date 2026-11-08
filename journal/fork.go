// journal/fork.go
// Forks (#810). A forked step's journal starts with one fork record
// naming a parent journal and a sequence in it. Open then treats the
// parent's records up to and including that sequence as this step's
// history: the parent's committed state before the forked iteration
// is the state, and the forked iteration's records up to the point are
// replayed by position as the forked step's iteration 0, so model
// responses and tool results recorded before the point are reused and
// execution diverges after it. The parent journal is only read.
//
// The parent's records must still be in AGENT_JOURNAL: a fork cannot
// read records archived out of the stream.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TypeFork is the first record of a forked step's journal.
const TypeFork = "fork"

// forkChainMax bounds how many forks a chain of forks of forks may
// follow back to a journal that is not a fork.
const forkChainMax = 16

// Fork errors.
var (
	// ErrForkNoJournal: the parent step has no journal, so it is not
	// an agent step there is anything to fork from.
	ErrForkNoJournal = errors.New("journal: the parent step has no journal")
	// ErrForkPoint: the sequence is not a record of the parent step's
	// journal that a fork may start from (a commit or fork record is
	// not one), or the record is no longer in the stream.
	ErrForkPoint = errors.New("journal: the sequence is not a fork point in the parent journal")
	// ErrForkExists: the forked step's journal already holds records.
	ErrForkExists = errors.New("journal: the forked step's journal is not empty")
	// errForkRelayed: a program's journal reads only its own subject
	// through the engine relay, so it cannot read a parent's records.
	errForkRelayed = errors.New(
		"journal: a forked journal cannot be replayed through the program relay")
)

// Fork is the body of a fork record: the parent journal and the
// sequence, in AGENT_JOURNAL, of the last parent record the fork
// replays. A forked step keeps the parent's step ID.
type Fork struct {
	ParentRunID   string `json:"parent_run_id"`
	ParentStepID  string `json:"parent_step_id"`
	ParentSubject string `json:"parent_subject"`
	ParentSeq     uint64 `json:"parent_seq"`
}

// forkBase is the history a forked journal starts from: the state
// before its iteration 0, that iteration's inherited records (claims
// dropped, every record restamped to iteration 0, attempt 1 for the
// fork point's attempt and 0 for an attempt it superseded), and every
// input the parent chain placed up to the fork point.
type forkBase struct {
	state   []byte
	records []Record
	placed  []InputPlaced
}

// WriteFork checks that fork names a fork point in a parent journal,
// then writes the fork record as the first record of runID's journal
// for the parent's step. It returns the record's sequence.
func WriteFork(
	ctx context.Context, js jetstream.JetStream, pub *natsutil.TracingPublisher,
	runID string, fork Fork,
) (uint64, error) {
	if ctx == nil || js == nil || pub == nil {
		panic("journal.WriteFork: ctx, js and pub must not be nil")
	}
	if runID == "" || fork.ParentRunID == "" || fork.ParentStepID == "" {
		panic("journal.WriteFork: run, parent run and parent step must not be empty")
	}
	if runID == fork.ParentRunID {
		panic("journal.WriteFork: a run cannot fork itself")
	}
	fork.ParentSubject = natsutil.JournalSubject(fork.ParentRunID, fork.ParentStepID)
	if _, err := readForkBase(ctx, js, fork); err != nil {
		return 0, err
	}
	data, err := json.Marshal(fork)
	if err != nil {
		return 0, fmt.Errorf("marshal fork: %w", err)
	}
	ack, err := pub.JSPublishMsg(ctx, forkMessage(runID, fork.ParentStepID, data))
	if err != nil {
		if wrongLastSequence(err) {
			return 0, ErrForkExists
		}
		return 0, fmt.Errorf("journal: append fork: %w", err)
	}
	return ack.Sequence, nil
}

// forkMessage is the fork record, fenced on an empty subject. It
// belongs to no claim: iteration 0, attempt and delivery 0.
func forkMessage(runID, stepID string, data []byte) *nats.Msg {
	if runID == "" || stepID == "" {
		panic("forkMessage: run and step must not be empty")
	}
	if len(data) == 0 {
		panic("forkMessage: data must not be empty")
	}
	return &nats.Msg{
		Subject: natsutil.JournalSubject(runID, stepID),
		Data:    data,
		Header: nats.Header{
			headerMsgID:           {runID + "." + stepID + ".fork"},
			headerExpectedLastSeq: {"0"},
			HeaderType:            {TypeFork},
			HeaderIteration:       {"0"},
			HeaderAttempt:         {"0"},
			HeaderIndex:           {"0"},
			HeaderDelivered:       {"0"},
			HeaderStreamSeq:       {"0"},
		},
	}
}

// forkOf decodes the fork record a journal starts with; ok is false
// when records does not start with one.
func forkOf(records []Record) (Fork, bool, error) {
	if len(records) > recordsMax {
		panic("forkOf: records exceed the read bound")
	}
	if len(records) == 0 || records[0].Type != TypeFork {
		return Fork{}, false, nil
	}
	if records[0].Seq == 0 {
		panic("forkOf: record must have a sequence")
	}
	var fork Fork
	if err := json.Unmarshal(records[0].Data, &fork); err != nil {
		return Fork{}, false, fmt.Errorf("journal: decode fork at seq %d: %w", records[0].Seq, err)
	}
	if fork.ParentRunID == "" || fork.ParentStepID == "" || fork.ParentSeq == 0 {
		return Fork{}, false, fmt.Errorf("journal: fork at seq %d names no parent", records[0].Seq)
	}
	return fork, true, nil
}

// readHistory reads the journal up to tailSeq and, for a forked
// journal, the base it starts from; the fork record is not returned.
func (j *Journal) readHistory(ctx context.Context, tailSeq uint64) (forkBase, []Record, error) {
	if ctx == nil {
		panic("journal.readHistory: ctx must not be nil")
	}
	if j.js == nil {
		panic("journal.readHistory: js must not be nil")
	}
	records, err := j.readUpTo(ctx, tailSeq)
	if err != nil {
		return forkBase{}, nil, err
	}
	fork, forked, err := forkOf(records)
	if err != nil || !forked {
		return forkBase{}, records, err
	}
	if j.relayed() {
		return forkBase{}, nil, errForkRelayed
	}
	base, err := readForkBase(ctx, j.js, fork)
	if err != nil {
		return forkBase{}, nil, fmt.Errorf("journal: read fork parent: %w", err)
	}
	if err := withinReadBound(base, records[1:]); err != nil {
		return forkBase{}, nil, err
	}
	return base, records[1:], nil
}

// withinReadBound refuses a forked journal whose inherited and own
// records together outgrow one journal read.
func withinReadBound(base forkBase, records []Record) error {
	if len(base.records) > recordsMax {
		panic("withinReadBound: a fork base is one read")
	}
	if len(records) > recordsMax {
		panic("withinReadBound: records are one read")
	}
	if len(base.records)+len(records) > recordsMax {
		return fmt.Errorf("journal: more than %d records with those the fork inherits", recordsMax)
	}
	return nil
}

// forkSegment is one journal of a fork chain read up to its fork
// point, without its own fork record.
type forkSegment struct {
	fork    Fork
	records []Record
}

// readForkBase follows fork back through any forks of forks, reading
// each parent up to its fork point, then folds the chain from its root
// into the base fork's journal starts from. Iterative, bounded by
// forkChainMax.
func readForkBase(ctx context.Context, js jetstream.JetStream, fork Fork) (forkBase, error) {
	if ctx == nil || js == nil {
		panic("readForkBase: ctx and js must not be nil")
	}
	if fork.ParentRunID == "" || fork.ParentStepID == "" {
		panic("readForkBase: fork must name its parent")
	}
	var chain []forkSegment
	for next, more := fork, true; more; {
		if len(chain) >= forkChainMax {
			return forkBase{}, fmt.Errorf("journal: fork chain longer than %d", forkChainMax)
		}
		records, err := readForkPoint(ctx, js, next)
		if err != nil {
			return forkBase{}, err
		}
		segment := forkSegment{fork: next, records: records}
		var parent Fork
		parent, more, err = forkOf(records)
		if err != nil {
			return forkBase{}, err
		}
		if more {
			segment.records = records[1:]
		}
		chain = append(chain, segment)
		next = parent
	}
	var base forkBase
	for i := len(chain) - 1; i >= 0; i-- {
		if err := withinReadBound(base, chain[i].records); err != nil {
			return forkBase{}, err
		}
		base = chain[i].view(base)
	}
	return base, nil
}

// readForkPoint reads the parent journal fork names up to and
// including its sequence, which must be a record of that journal
// other than a commit or a fork.
func readForkPoint(ctx context.Context, js jetstream.JetStream, fork Fork) ([]Record, error) {
	if ctx == nil || js == nil {
		panic("readForkPoint: ctx and js must not be nil")
	}
	if fork.ParentRunID == "" || fork.ParentStepID == "" {
		panic("readForkPoint: fork must name its parent")
	}
	records, err := readParentUpTo(ctx, js, fork)
	if err != nil {
		return nil, err
	}
	point := records[len(records)-1]
	switch {
	case point.Seq != fork.ParentSeq:
		return nil, fmt.Errorf("%w: seq %d is not a record of %s",
			ErrForkPoint, fork.ParentSeq, natsutil.JournalSubject(fork.ParentRunID, fork.ParentStepID))
	case point.Type == TypeCommit, point.Type == TypeFork:
		return nil, fmt.Errorf("%w: seq %d is a %s record; fork at the next claim instead",
			ErrForkPoint, fork.ParentSeq, point.Type)
	case point.Iteration < 0 || point.Attempt < 1:
		return nil, fmt.Errorf("%w: seq %d carries no claim identity", ErrForkPoint, fork.ParentSeq)
	}
	return records, nil
}

// readParentUpTo reads the parent journal up to the fork point. A
// parent JetStream holds no record of was purged whole, or never had a
// journal: the fork names a record, so the archive must hold it.
func readParentUpTo(ctx context.Context, js jetstream.JetStream, fork Fork) ([]Record, error) {
	if ctx == nil || js == nil {
		panic("readParentUpTo: ctx and js must not be nil")
	}
	if fork.ParentRunID == "" || fork.ParentStepID == "" {
		panic("readParentUpTo: fork must name its parent")
	}
	parent := fork.ParentRunID + "/" + fork.ParentStepID
	tail, err := subjectTail(ctx, js, fork.ParentRunID, fork.ParentStepID)
	if err != nil {
		return nil, err
	}
	var records []Record
	if tail == 0 {
		records, err = readPurgedSubject(ctx, js, fork.ParentRunID, fork.ParentStepID, true)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrForkNoJournal, parent)
		}
		tail = records[len(records)-1].Seq
	}
	if fork.ParentSeq == 0 || fork.ParentSeq > tail {
		return nil, fmt.Errorf("%w: seq %d is outside the journal (tail %d)",
			ErrForkPoint, fork.ParentSeq, tail)
	}
	if records == nil {
		return readSubject(ctx, js, fork.ParentRunID, fork.ParentStepID, fork.ParentSeq)
	}
	end := len(records)
	for end > 0 && records[end-1].Seq > fork.ParentSeq {
		end--
	}
	return records[:end], nil
}

// subjectTail is the sequence of the newest record of the step's
// journal, 0 when it has none or the stream does not exist.
func subjectTail(
	ctx context.Context, js jetstream.JetStream, runID, stepID string,
) (uint64, error) {
	if ctx == nil || js == nil {
		panic("subjectTail: ctx and js must not be nil")
	}
	if runID == "" || stepID == "" {
		panic("subjectTail: run and step must not be empty")
	}
	stream, err := js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, ErrNotProvisioned
		}
		return 0, fmt.Errorf("journal: stream: %w", err)
	}
	var raw *jetstream.RawStreamMsg
	err = retryTransient(ctx, func() error {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(ctx, natsutil.JournalSubject(runID, stepID))
		return getErr
	})
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("journal: read tail: %w", err)
	}
	return raw.Sequence, nil
}

// view is what a fork of this segment at its fork point inherits: the
// view a successor of the fork point's claim would open with, given
// the base the segment itself was forked from.
func (s forkSegment) view(prior forkBase) forkBase {
	if len(s.records) == 0 {
		panic("forkSegment.view: a segment ends at its fork point")
	}
	point := s.records[len(s.records)-1]
	if point.Seq != s.fork.ParentSeq {
		panic("forkSegment.view: the last record must be the fork point")
	}
	v := &Journal{task: Task{
		RunID: s.fork.ParentRunID, StepID: s.fork.ParentStepID,
		Iteration: point.Iteration, Attempt: point.Attempt, Delivered: 1,
	}, honoured: map[uint64]bool{}}
	v.rebuild(prior, s.records)
	base := forkBase{state: v.state, placed: v.placed}
	for _, r := range v.records {
		if r.Type == TypeClaim {
			// A parent claim is no claim of the fork's: kept, it would
			// count toward the fork's restart limit.
			continue
		}
		attempt := 1
		if v.priorAttempt(r) {
			attempt = 0
		}
		r.Iteration, r.Attempt = 0, attempt
		base.records = append(base.records, r)
	}
	return base
}

// forkBaseFor reads the base a journal's fork record names, for the
// checker; ok is false for a journal that is not a fork.
func forkBaseFor(
	ctx context.Context, js jetstream.JetStream, records []Record,
) (forkBase, bool, error) {
	if ctx == nil || js == nil {
		panic("forkBaseFor: ctx and js must not be nil")
	}
	fork, forked, err := forkOf(records)
	if err != nil || !forked {
		return forkBase{}, false, err
	}
	base, err := readForkBase(ctx, js, fork)
	if err != nil {
		return forkBase{}, false, fmt.Errorf("journal: read fork parent: %w", err)
	}
	if err := withinReadBound(base, records[1:]); err != nil {
		return forkBase{}, false, err
	}
	return base, true, nil
}

// journal/floor.go
// The protected floor: the lowest AGENT_JOURNAL sequence that a later
// Open of a live step still reads. An archiver that purges the journal
// below what it has archived must also stay below this floor, because
// agent steps read their journal on every delivery (Open replays the
// iteration's model and tool calls), and those reads hold no consumer
// ack floor an archiver could see.
//
// What Open reads (rebuild): the state of the newest commit of an
// earlier iteration, and every record of the iteration it claims. So a
// step on iteration N needs iteration N-1's last commit and N's
// records, and nothing older; a parked chat run does not pin the
// journal from its first turn. A step with no earlier commit, or whose
// records carry no iteration, is protected from its first record. A
// fork is protected from its first record too, and so is every parent
// journal of its chain, which Open reads up to the fork point.
//
// Placements of earlier iterations (input.placed) fall below the floor.
// Open keeps them to refuse placing a request twice, but a placement is
// settled done in the submissions bucket when its iteration commits,
// and a done submission is never placed again.
package journal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrFloorOverBudget: the live journals hold more runs, subjects or
// records than one floor pass reads. No floor is computed; a caller
// that publishes it leaves the published one to go stale.
var ErrFloorOverBudget = errors.New("journal: protected floor pass is over its budget")

// floorBudget bounds one floor pass: live runs listed, and journal
// records (headers only) read across all their steps.
type floorBudget struct {
	runs    int
	records int
}

// defaultFloorBudget is the production bound. Records are read
// headers-only, so a pass at the bound moves about a few hundred bytes
// per record.
var defaultFloorBudget = floorBudget{runs: 10_000, records: 500_000}

// floorSubjectsPerRunMax bounds the agent steps one run may have
// journals for.
const floorSubjectsPerRunMax = 1024

// ProtectedFloor returns the lowest AGENT_JOURNAL sequence a later Open
// of a step of a live run still reads: the minimum over every journal
// of the runs listRuns names. With no such journal it is the stream's
// LastSeq+1, protecting nothing. listRuns is called after the stream's
// LastSeq is read, so a run that goes live after the list writes only
// above the returned floor.
func ProtectedFloor(
	ctx context.Context, js jetstream.JetStream,
	listRuns func(context.Context) ([]string, error),
) (uint64, error) {
	return protectedFloorWithin(ctx, js, listRuns, defaultFloorBudget)
}

func protectedFloorWithin(
	ctx context.Context, js jetstream.JetStream,
	listRuns func(context.Context) ([]string, error), budget floorBudget,
) (uint64, error) {
	if ctx == nil || js == nil || listRuns == nil {
		panic("journal.ProtectedFloor: ctx, js and listRuns must not be nil")
	}
	if budget.runs <= 0 || budget.records <= 0 {
		panic("journal.ProtectedFloor: budget must be positive")
	}
	stream, err := js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, ErrNotProvisioned
		}
		return 0, fmt.Errorf("journal: stream: %w", err)
	}
	floor := stream.CachedInfo().State.LastSeq + 1
	ids, err := listRuns(ctx)
	if err != nil {
		return 0, fmt.Errorf("journal: list live runs: %w", err)
	}
	if len(ids) > budget.runs {
		return 0, fmt.Errorf("%w: %d live runs, bound %d", ErrFloorOverBudget, len(ids), budget.runs)
	}
	recordsLeft := budget.records
	for _, runID := range ids {
		runFloor, read, err := runFloor(ctx, js, stream, runID, recordsLeft)
		if err != nil {
			return 0, err
		}
		recordsLeft -= read
		floor = min(floor, runFloor)
	}
	return floor, nil
}

// runFloor is the floor over one run's step journals, and how many
// records it read. A run with no journal has no floor (MaxUint64).
func runFloor(
	ctx context.Context, js jetstream.JetStream, stream jetstream.Stream,
	runID string, recordsLeft int,
) (uint64, int, error) {
	if stream == nil {
		panic("runFloor: stream must not be nil")
	}
	if recordsLeft < 0 {
		panic("runFloor: recordsLeft must not be negative")
	}
	none := ^uint64(0)
	if runID == "" || strings.ContainsAny(runID, ". \t*>") {
		// Not a run ID any journal subject could carry.
		return none, 0, nil
	}
	subjects, err := runSubjects(ctx, stream, runID)
	if err != nil {
		return 0, 0, err
	}
	floor, read := none, 0
	for _, subject := range subjects.names {
		read += int(subjects.counts[subject])
		if read > recordsLeft {
			return 0, 0, fmt.Errorf("%w: more than %d journal records in live runs",
				ErrFloorOverBudget, recordsLeft)
		}
		seq, err := subjectFloor(ctx, js, stream, subject)
		if err != nil {
			return 0, 0, err
		}
		floor = min(floor, seq)
	}
	return floor, read, nil
}

// journalSubjects are a run's journal subjects, sorted, with their
// record counts.
type journalSubjects struct {
	names  []string
	counts map[string]uint64
}

// runSubjects lists the run's journal subjects from the stream's
// subject state, one stream info request.
func runSubjects(
	ctx context.Context, stream jetstream.Stream, runID string,
) (journalSubjects, error) {
	if stream == nil {
		panic("runSubjects: stream must not be nil")
	}
	if runID == "" {
		panic("runSubjects: runID must not be empty")
	}
	var info *jetstream.StreamInfo
	err := retryTransient(ctx, func() error {
		var infoErr error
		info, infoErr = stream.Info(ctx, jetstream.WithSubjectFilter("journal."+runID+".>"))
		return infoErr
	})
	if err != nil {
		return journalSubjects{}, fmt.Errorf("journal: subjects of run %s: %w", runID, err)
	}
	counts := info.State.Subjects
	if len(counts) > floorSubjectsPerRunMax {
		return journalSubjects{}, fmt.Errorf("%w: run %s has %d journals, bound %d",
			ErrFloorOverBudget, runID, len(counts), floorSubjectsPerRunMax)
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	return journalSubjects{names: names, counts: counts}, nil
}

// subjectFloor is the floor of one step journal: stepFloor over its
// records read headers-only, lowered to the parent chain's for a fork.
// A subject emptied since it was listed has no floor (MaxUint64).
func subjectFloor(
	ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, subject string,
) (uint64, error) {
	if stream == nil || js == nil {
		panic("subjectFloor: stream and js must not be nil")
	}
	if subject == "" {
		panic("subjectFloor: subject must not be empty")
	}
	var tail *jetstream.RawStreamMsg
	err := retryTransient(ctx, func() error {
		var getErr error
		tail, getErr = stream.GetLastMsgForSubject(ctx, subject)
		return getErr
	})
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return ^uint64(0), nil
	}
	if err != nil {
		return 0, fmt.Errorf("journal: tail of %s: %w", subject, err)
	}
	records, err := readHeaders(ctx, js, subject, tail.Sequence)
	if err != nil {
		return 0, err
	}
	floor := stepFloor(records)
	if records[0].Type != TypeFork {
		return floor, nil
	}
	parents, err := forkChainFloor(ctx, stream, records[0].Seq)
	if err != nil {
		return 0, err
	}
	return min(floor, parents), nil
}

// readHeaders reads a step journal's records up to tailSeq without
// their bodies. The read always holds the tail, so it is never empty.
func readHeaders(
	ctx context.Context, js jetstream.JetStream, subject string, tailSeq uint64,
) ([]Record, error) {
	if tailSeq == 0 {
		panic("readHeaders: tailSeq must be positive")
	}
	if subject == "" {
		panic("readHeaders: subject must not be empty")
	}
	var records []Record
	err := retryTransient(ctx, func() error {
		records = make([]Record, 0, readBatch)
		return fetchUpTo(ctx, js, subjectRead{
			stream: natsutil.AgentJournalStream, subject: subject,
			tailSeq: tailSeq, limit: recordsMax, headersOnly: true,
		}, func(seq uint64, msg jetstream.Msg, meta *jetstream.MsgMetadata) error {
			records = append(records, decodeRecord(seq, msg.Headers(), nil, meta.Timestamp))
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("journal: read of %s ended before its tail %d", subject, tailSeq)
	}
	return records, nil
}

// stepFloor is the lowest sequence of records, a step journal in
// order, that a later Open of the step reads. The current iteration is
// the highest one recorded: the next Open claims it again (a
// redelivery or retry) or claims the one after it, which starts from
// that iteration's commit. Either way it reads the newest commit of an
// earlier iteration (rebuild's state) and the current iteration's
// records. When there is no earlier commit, or a record carries no
// iteration, or the journal is a fork, the whole journal is kept.
func stepFloor(records []Record) uint64 {
	if len(records) == 0 {
		panic("stepFloor: records must not be empty")
	}
	if len(records) > recordsMax {
		panic("stepFloor: records exceed the read bound")
	}
	first := records[0].Seq
	if records[0].Type == TypeFork {
		return first
	}
	current := 0
	for _, r := range records {
		if r.Iteration < 0 {
			return first
		}
		current = max(current, r.Iteration)
	}
	var stateSeq, currentSeq uint64
	for _, r := range records {
		if r.Iteration < current && r.Type == TypeCommit {
			stateSeq = r.Seq
		}
		if r.Iteration == current && currentSeq == 0 {
			currentSeq = r.Seq
		}
	}
	if stateSeq == 0 {
		return first
	}
	return min(stateSeq, currentSeq)
}

// forkChainFloor follows a fork record back through its parents and
// returns the lowest first record among them: Open of a fork reads
// each parent from its start up to the fork point. A parent that is
// gone protects nothing. Iterative, bounded by forkChainMax.
func forkChainFloor(ctx context.Context, stream jetstream.Stream, forkSeq uint64) (uint64, error) {
	if stream == nil {
		panic("forkChainFloor: stream must not be nil")
	}
	if forkSeq == 0 {
		panic("forkChainFloor: forkSeq must be positive")
	}
	floor := ^uint64(0)
	raw, err := getMsg(ctx, stream, forkSeq, "")
	for hop := 0; err == nil && raw != nil; hop++ {
		if hop >= forkChainMax {
			return 0, fmt.Errorf("journal: fork chain longer than %d", forkChainMax)
		}
		fork, forked, decodeErr := forkOf([]Record{
			decodeRecord(raw.Sequence, raw.Header, raw.Data, raw.Time),
		})
		if decodeErr != nil || !forked {
			return floor, decodeErr
		}
		parent := natsutil.JournalSubject(fork.ParentRunID, fork.ParentStepID)
		raw, err = getMsg(ctx, stream, 1, parent)
		if err == nil && raw != nil {
			floor = min(floor, raw.Sequence)
		}
	}
	return floor, err
}

// getMsg reads one message with its body: the one at seq, or with a
// subject the subject's first at or after seq. A missing one is nil.
func getMsg(
	ctx context.Context, stream jetstream.Stream, seq uint64, subject string,
) (*jetstream.RawStreamMsg, error) {
	if stream == nil {
		panic("getMsg: stream must not be nil")
	}
	if seq == 0 {
		panic("getMsg: seq must be positive")
	}
	var opts []jetstream.GetMsgOpt
	if subject != "" {
		opts = append(opts, jetstream.WithGetMsgSubject(subject))
	}
	var raw *jetstream.RawStreamMsg
	err := retryTransient(ctx, func() error {
		var getErr error
		raw, getErr = stream.GetMsg(ctx, seq, opts...)
		return getErr
	})
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("journal: get %d %s: %w", seq, subject, err)
	}
	return raw, nil
}

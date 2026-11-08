package journal

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/dagnatsext/archivenats"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Endless JetStream: an archiver (lode) may purge AGENT_JOURNAL below
// what it archived. Every record names its predecessor on the subject
// (Nats-Expected-Last-Subject-Sequence, which the server enforced when
// it stored the record), so a read knows exactly when records it needs
// are no longer in JetStream: the first record it holds names a
// predecessor it does not. Only then does it read the archive, and the
// same chain proves the archive and JetStream meet with no hole and no
// duplicate. A journal JetStream holds in full never asks the archive.

var (
	// ErrArchive: a journal read needed records below the stream's
	// FirstSeq and the archive could not supply all of them (no
	// archive, not archived, unknown epoch, a gap, busy, timeout).
	ErrArchive = errors.New("journal: records purged from JetStream could not be read " +
		"from the archive")
	// ErrDiscontinuous: a record names a predecessor the read does not
	// hold, or the records are out of order. Replay cannot be trusted.
	ErrDiscontinuous = errors.New("journal: record chain is broken")
)

// archiveOptions tunes the archive reader; zero takes its defaults. A
// var so tests can shorten the timeouts.
var archiveOptions = archivenats.Options{}

// predecessor reads a stored record's expected-last-subject-sequence:
// the subject's sequence before it, 0 for its first record. ok is false
// for a message that does not carry the header.
func predecessor(hdr nats.Header) (uint64, bool) {
	if hdr == nil {
		return 0, false
	}
	value := hdr.Get(headerExpectedLastSeq)
	if value == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// CheckContinuity verifies that records, read from sequence from, form
// an unbroken chain on their subject: each record's predecessor is the
// record before it, and the first one's is below from. A record that
// carries no predecessor header is not checked against its neighbour.
func CheckContinuity(records []Record, from uint64) error {
	if len(records) > recordsMax+1 {
		panic("journal.CheckContinuity: records exceed the read bound")
	}
	for i, r := range records {
		if i > 0 && r.Seq <= records[i-1].Seq {
			return fmt.Errorf("%w: seq %d follows seq %d", ErrDiscontinuous, r.Seq, records[i-1].Seq)
		}
		if !r.hasPrev {
			continue
		}
		if i == 0 {
			if r.prev > 0 && r.prev >= from {
				return fmt.Errorf("%w: seq %d follows seq %d, which the read does not hold",
					ErrDiscontinuous, r.Seq, r.prev)
			}
			continue
		}
		if r.prev != records[i-1].Seq {
			return fmt.Errorf("%w: seq %d follows seq %d, not seq %d",
				ErrDiscontinuous, r.Seq, r.prev, records[i-1].Seq)
		}
	}
	return nil
}

// ArchivedBefore returns the archived messages of subject from sequence
// from up to the live record live, which JetStream still holds: nothing
// when live's predecessor is below from, so the read needs no archive.
// limit bounds the messages (0: recordsMax). Without a limit cutting it
// short the last one must be live's predecessor. For the engine relay,
// which answers a program's journal reads; Read and Open use it inside.
func ArchivedBefore(
	ctx context.Context, js jetstream.JetStream, subject string, from uint64,
	live *jetstream.RawStreamMsg, limit int,
) ([]dagnatsext.ArchivedMessage, error) {
	if ctx == nil || js == nil || live == nil {
		panic("journal.ArchivedBefore: ctx, js and live must be set")
	}
	if from == 0 || subject == "" || limit < 0 {
		panic("journal.ArchivedBefore: from, subject and limit out of range")
	}
	prev, ok := predecessor(live.Header)
	if !ok || prev == 0 || prev < from {
		return nil, nil
	}
	return archivedBefore(ctx, js, subject, from, live.Sequence, prev, limit)
}

// archivedBefore reads subject's archived messages in [from, liveSeq)
// and checks that they chain and end at prev, the live record's
// predecessor, unless limit cut the read short.
func archivedBefore(
	ctx context.Context, js jetstream.JetStream, subject string,
	from, liveSeq, prev uint64, limit int,
) ([]dagnatsext.ArchivedMessage, error) {
	if prev < from || prev >= liveSeq {
		panic("journal.archivedBefore: prev must lie in [from, liveSeq)")
	}
	if limit == 0 || limit > recordsMax {
		limit = recordsMax + 1
	}
	msgs, err := readArchive(ctx, js, subject, from, liveSeq, limit)
	if err != nil {
		return nil, err
	}
	if len(msgs) > recordsMax {
		return nil, fmt.Errorf("journal: more than %d archived records on %s", recordsMax, subject)
	}
	records := make([]Record, 0, len(msgs))
	for _, m := range msgs {
		records = append(records, archivedRecord(m))
	}
	if len(msgs) < limit {
		if len(records) == 0 || records[len(records)-1].Seq != prev {
			return nil, fmt.Errorf("%w: %s seq %d follows seq %d, which the archive does not hold",
				ErrDiscontinuous, subject, liveSeq, prev)
		}
	}
	if err := CheckContinuity(records, from); err != nil {
		return nil, fmt.Errorf("%s: archive: %w", subject, err)
	}
	return msgs, nil
}

// readArchive asks the archive for [from, min(FirstSeq, before)) of
// subject. The read gets the archive's own timeout, not the caller's
// deadline: object storage is far slower than a JetStream call. An
// explicit cancel of ctx still stops it.
func readArchive(
	ctx context.Context, js jetstream.JetStream, subject string, from, before uint64, limit int,
) ([]dagnatsext.ArchivedMessage, error) {
	if from == 0 || before <= from || limit <= 0 {
		panic("journal.readArchive: need 0 < from < before and a positive limit")
	}
	readCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() {
		if errors.Is(ctx.Err(), context.Canceled) {
			cancel()
		}
	})
	defer stop()
	info, domain, err := archiveTarget(readCtx, js)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrArchive, subject, err)
	}
	to := min(before, info.State.FirstSeq)
	if to <= from {
		return nil, fmt.Errorf("%w: %s needs seq %d, below seq %d, but the stream starts at %d",
			ErrDiscontinuous, subject, from, before, info.State.FirstSeq)
	}
	reader := archivenats.NewReader(js.Conn(), domain, natsutil.AgentJournalStream,
		archiveOptions)
	got, err := reader.ReadRange(readCtx, dagnatsext.ArchiveRange{
		Epoch: info.Created.UnixNano(), Subject: subject, FromSeq: from, ToSeq: to, Limit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %s below seq %d: %w", ErrArchive, subject, to, err)
	}
	if len(got.Gaps) > 0 {
		return nil, fmt.Errorf("%w: %s: the archive lost seq %d-%d of the stream, "+
			"so the journal cannot be replayed", ErrArchive, subject, got.Gaps[0].From, got.Gaps[0].To)
	}
	return got.Messages, nil
}

// archiveTarget is the journal stream's state (FirstSeq, and Created,
// the archive epoch) and the JetStream domain its archive answers in.
func archiveTarget(
	ctx context.Context, js jetstream.JetStream,
) (*jetstream.StreamInfo, string, error) {
	if ctx == nil || js == nil {
		panic("journal.archiveTarget: ctx and js must not be nil")
	}
	stream, err := js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		return nil, "", fmt.Errorf("stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("stream info: %w", err)
	}
	account, err := js.AccountInfo(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("account info: %w", err)
	}
	if info.Created.IsZero() {
		return nil, "", errors.New("stream reports no creation time, so no archive epoch")
	}
	return info, account.Domain, nil
}

// archivedRecord decodes an archived message as a journal record.
func archivedRecord(m dagnatsext.ArchivedMessage) Record {
	if m.Seq == 0 {
		panic("journal.archivedRecord: seq must be positive")
	}
	return decodeRecord(m.Seq, nats.Header(m.Headers), m.Data, m.Time)
}

// withArchived completes live, a subject's records as JetStream holds
// them, with the archived records before them when the first one names
// a predecessor, checks the chain, and drops records past tailSeq.
func withArchived(
	ctx context.Context, js jetstream.JetStream, runID, stepID string,
	tailSeq uint64, live []Record,
) ([]Record, error) {
	if tailSeq == 0 || len(live) > recordsMax {
		panic("journal.withArchived: tailSeq must be positive and live bounded")
	}
	if len(live) == 0 || !live[0].hasPrev || live[0].prev == 0 {
		if err := CheckContinuity(live, 1); err != nil {
			return nil, fmt.Errorf("run %s step %s: %w", runID, stepID, err)
		}
		return live, nil
	}
	subject := natsutil.JournalSubject(runID, stepID)
	msgs, err := archivedBefore(ctx, js, subject, 1, live[0].Seq, live[0].prev, 0)
	if err != nil {
		return nil, fmt.Errorf("run %s step %s: %w", runID, stepID, err)
	}
	if len(msgs)+len(live) > recordsMax {
		return nil, fmt.Errorf("journal: more than %d records on %s", recordsMax, subject)
	}
	all := make([]Record, 0, len(msgs)+len(live))
	for _, m := range msgs {
		all = append(all, archivedRecord(m))
	}
	all = append(all, live...)
	if err := CheckContinuity(all, 1); err != nil {
		return nil, fmt.Errorf("run %s step %s: %w", runID, stepID, err)
	}
	end := len(all)
	for end > 0 && all[end-1].Seq > tailSeq {
		end--
	}
	return all[:end], nil
}

// ArchivedPrefix returns the step's records from sequence from that are
// only in the archive: empty when the first record at or after from is
// still in JetStream with no purged predecessor at or after from, or
// when JetStream holds nothing there. At most limit records (0:
// recordsMax). The client journal read (GET .../journal) serves them
// before its JetStream page.
func ArchivedPrefix(
	ctx context.Context, js jetstream.JetStream, runID, stepID string, from uint64, limit int,
) ([]Record, error) {
	if ctx == nil || js == nil {
		panic("journal.ArchivedPrefix: ctx and js must not be nil")
	}
	if runID == "" || stepID == "" || limit < 0 {
		panic("journal.ArchivedPrefix: run, step and limit out of range")
	}
	from = max(from, 1)
	stream, err := js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		return nil, fmt.Errorf("journal: stream: %w", err)
	}
	subject := natsutil.JournalSubject(runID, stepID)
	var live *jetstream.RawStreamMsg
	err = retryTransient(ctx, func() error {
		var getErr error
		live, getErr = stream.GetMsg(ctx, from, jetstream.WithGetMsgSubject(subject))
		return getErr
	})
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("journal: read %s from %d: %w", subject, from, err)
	}
	msgs, err := ArchivedBefore(ctx, js, subject, from, live, limit)
	if err != nil {
		return nil, fmt.Errorf("run %s step %s: %w", runID, stepID, err)
	}
	records := make([]Record, 0, len(msgs))
	for _, m := range msgs {
		records = append(records, archivedRecord(m))
	}
	return records, nil
}

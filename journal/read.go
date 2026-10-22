package journal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Read returns every record of the step's journal, in order. An
// unprovisioned stream is ErrNotProvisioned; an empty journal is an
// empty slice.
func Read(
	ctx context.Context, js jetstream.JetStream, runID, stepID string,
) ([]Record, error) {
	if ctx == nil {
		panic("journal.Read: ctx must not be nil")
	}
	if runID == "" || stepID == "" {
		panic("journal.Read: run and step must not be empty")
	}
	stream, err := js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, ErrNotProvisioned
		}
		return nil, fmt.Errorf("journal: stream: %w", err)
	}
	var raw *jetstream.RawStreamMsg
	err = retryTransient(ctx, func() error {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(ctx, natsutil.JournalSubject(runID, stepID))
		return getErr
	})
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return []Record{}, nil
		}
		return nil, fmt.Errorf("journal: read tail: %w", err)
	}
	return readSubject(ctx, js, runID, stepID, raw.Sequence)
}

// readSubject reads the subject's records up to tailSeq.
func readSubject(
	ctx context.Context, js jetstream.JetStream, runID, stepID string, tailSeq uint64,
) ([]Record, error) {
	if tailSeq == 0 {
		panic("readSubject: tailSeq must be positive")
	}
	if js == nil {
		panic("readSubject: js must not be nil")
	}
	var records []Record
	// Each attempt reads from the start, so a retried read never holds
	// records twice.
	err := retryTransient(ctx, func() error {
		records = make([]Record, 0, readBatch)
		return fetchUpTo(ctx, js, natsutil.AgentJournalStream,
			natsutil.JournalSubject(runID, stepID), tailSeq, recordsMax,
			func(seq uint64, msg jetstream.Msg, meta *jetstream.MsgMetadata) error {
				records = append(records,
					decodeRecord(seq, msg.Headers(), msg.Data(), meta.Timestamp))
				return nil
			})
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// fetchUpTo reads a subject from the start up to and including tailSeq
// through a bounded ordered consumer and hands each message to visit.
// Every fetch is bounded by readWait, the whole read by limit messages
// and the caller's ctx. Knowing the tail is what lets the read end
// without waiting a fetch window on an idle subject.
func fetchUpTo(
	ctx context.Context, js jetstream.JetStream, stream, subject string,
	tailSeq uint64, limit int,
	visit func(seq uint64, msg jetstream.Msg, meta *jetstream.MsgMetadata) error,
) error {
	if tailSeq == 0 || limit <= 0 {
		panic("fetchUpTo: tailSeq and limit must be positive")
	}
	if visit == nil {
		panic("fetchUpTo: visit must not be nil")
	}
	consumer, err := natsutil.OpenConsumer(ctx, js, stream, natsutil.OrderedConsumerSpec{
		FilterSubjects: []string{subject}, DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("journal: open reader on %s: %w", subject, err)
	}
	defer consumer.Close()
	for read := 0; read < limit; {
		batch, err := consumer.Fetch(readBatch, jetstream.FetchMaxWait(readWait))
		if err != nil {
			return fmt.Errorf("journal: fetch %s: %w", subject, err)
		}
		received := 0
		for msg := range batch.Messages() {
			received++
			meta, err := msg.Metadata()
			if err != nil {
				return fmt.Errorf("journal: message metadata: %w", err)
			}
			if err := visit(meta.Sequence.Stream, msg, meta); err != nil {
				return err
			}
			if meta.Sequence.Stream >= tailSeq {
				return nil
			}
		}
		if err := batch.Error(); err != nil {
			return fmt.Errorf("journal: fetch %s: %w", subject, err)
		}
		if received == 0 {
			return fmt.Errorf("journal: read of %s stalled before sequence %d", subject, tailSeq)
		}
		read += received
	}
	return fmt.Errorf("journal: more than %d messages on %s", limit, subject)
}

// headerInt parses an integer header; -1 marks a missing or foreign
// value so it never collides with a real iteration, attempt or index.
func headerInt(hdr nats.Header, key string) int {
	if hdr == nil {
		panic("headerInt: hdr must not be nil")
	}
	if key == "" {
		panic("headerInt: key must not be empty")
	}
	n, err := strconv.Atoi(hdr.Get(key))
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// transientReadMax bounds the attempts of a journal read that failed
// because the stream briefly had no leader. A leader election or a node
// restart makes JetStream answer "no responders" or time out; the read
// is safe to repeat, so it is retried rather than surfaced.
const transientReadMax = 6

// transientReadBackoff is the base wait between attempts; attempt n
// waits n times this, about 5s in total across transientReadMax.
const transientReadBackoff = 250 * time.Millisecond

// isTransientRead reports whether err means the stream had no leader to
// answer, as opposed to a real failure: no responders, a timeout, or
// the server's own "JetStream system temporarily unavailable" (10008),
// which a meta or stream leader election answers with.
func isTransientRead(err error) bool {
	if err == nil {
		panic("isTransientRead: err must not be nil")
	}
	if errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrTimeout) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == errCodeJetStreamUnavailable
}

// errCodeJetStreamUnavailable is nats-server's JSClusterNotAvailErr,
// which nats.go does not name.
const errCodeJetStreamUnavailable jetstream.ErrorCode = 10008

// retryTransient runs op until it succeeds, fails with a non-transient
// error, ctx ends, or transientReadMax attempts are spent. op must be
// safe to repeat from scratch.
func retryTransient(ctx context.Context, op func() error) error {
	if ctx == nil {
		panic("retryTransient: ctx must not be nil")
	}
	if op == nil {
		panic("retryTransient: op must not be nil")
	}
	var err error
	for attempt := 1; attempt <= transientReadMax; attempt++ {
		err = op()
		if err == nil || !isTransientRead(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt) * transientReadBackoff):
		}
	}
	return err
}

// DecodeMsg reads one journal message, as delivered by a consumer on
// the journal stream, into a Record. The record's Seq is the message's
// stream sequence, the cursor clients resume from.
func DecodeMsg(msg jetstream.Msg) (Record, error) {
	if msg == nil {
		panic("journal.DecodeMsg: msg must not be nil")
	}
	meta, err := msg.Metadata()
	if err != nil {
		return Record{}, fmt.Errorf("journal: message metadata: %w", err)
	}
	return decodeRecord(meta.Sequence.Stream, msg.Headers(), msg.Data(), meta.Timestamp), nil
}

// headerUint parses an unsigned header; a missing or foreign value is
// 0, the "unknown" of an identity field a writer did not stamp.
func headerUint(hdr nats.Header, key string) uint64 {
	if hdr == nil {
		panic("headerUint: hdr must not be nil")
	}
	if key == "" {
		panic("headerUint: key must not be empty")
	}
	n, err := strconv.ParseUint(hdr.Get(key), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

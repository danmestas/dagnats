package journal

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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
	raw, err := stream.GetLastMsgForSubject(ctx, natsutil.JournalSubject(runID, stepID))
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
	records := make([]Record, 0, readBatch)
	err := fetchUpTo(ctx, js, natsutil.AgentJournalStream,
		natsutil.JournalSubject(runID, stepID), tailSeq, recordsMax,
		func(seq uint64, msg jetstream.Msg, meta *jetstream.MsgMetadata) error {
			records = append(records, decodeRecord(seq, msg.Headers(), msg.Data(), meta.Timestamp))
			return nil
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

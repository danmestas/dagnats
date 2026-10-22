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

// readSubject reads the subject's records up to tailSeq through a
// bounded ordered consumer. Every fetch is bounded by readWait and the
// whole read by recordsMax; the caller's ctx bounds the total.
func readSubject(
	ctx context.Context, js jetstream.JetStream, runID, stepID string, tailSeq uint64,
) ([]Record, error) {
	if tailSeq == 0 {
		panic("readSubject: tailSeq must be positive")
	}
	if js == nil {
		panic("readSubject: js must not be nil")
	}
	consumer, err := natsutil.OpenConsumer(ctx, js, natsutil.AgentJournalStream,
		natsutil.OrderedConsumerSpec{
			FilterSubjects: []string{natsutil.JournalSubject(runID, stepID)},
			DeliverPolicy:  jetstream.DeliverAllPolicy,
		})
	if err != nil {
		return nil, fmt.Errorf("journal: open reader: %w", err)
	}
	records := make([]Record, 0, readBatch)
	for len(records) < recordsMax {
		batch, err := consumer.Fetch(readBatch, jetstream.FetchMaxWait(readWait))
		if err != nil {
			return nil, fmt.Errorf("journal: fetch: %w", err)
		}
		reached, err := appendBatch(&records, batch, tailSeq)
		if err != nil {
			return nil, err
		}
		if reached {
			return records, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("journal: read: %w", err)
		}
	}
	return nil, fmt.Errorf("journal: more than %d records on %s", recordsMax,
		natsutil.JournalSubject(runID, stepID))
}

// appendBatch decodes one fetch into records and reports whether the
// tail was reached. An empty fetch before the tail is a read that
// stalled, surfaced as an error rather than spun on.
func appendBatch(
	records *[]Record, batch jetstream.MessageBatch, tailSeq uint64,
) (bool, error) {
	if records == nil {
		panic("appendBatch: records must not be nil")
	}
	if tailSeq == 0 {
		panic("appendBatch: tailSeq must be positive")
	}
	received := 0
	for msg := range batch.Messages() {
		received++
		meta, err := msg.Metadata()
		if err != nil {
			return false, fmt.Errorf("journal: record metadata: %w", err)
		}
		*records = append(*records, decodeRecord(
			meta.Sequence.Stream, msg.Headers(), msg.Data(), meta.Timestamp,
		))
		if meta.Sequence.Stream >= tailSeq {
			return true, nil
		}
	}
	if err := batch.Error(); err != nil {
		return false, fmt.Errorf("journal: fetch: %w", err)
	}
	if received == 0 {
		return false, fmt.Errorf("journal: read stalled before sequence %d", tailSeq)
	}
	return false, nil
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

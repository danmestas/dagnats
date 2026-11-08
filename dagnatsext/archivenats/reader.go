// Package archivenats speaks the archive range protocol of
// dagnatsext.ArchiveReader over NATS request/reply. It is a package of
// its own so dagnatsext stays free of the NATS client for add-ons that
// only need its types.
package archivenats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/nats-io/nats.go"
)

// Bounds of one archive read.
const (
	// archivePagesMax bounds the replies of one read.
	archivePagesMax = 1 << 16
	// archiveMessagesMax bounds the messages of one read.
	archiveMessagesMax = 1 << 17
	// archiveMessageBytesMax bounds one reassembled message.
	archiveMessageBytesMax = 64 << 20
)

// Options tune NewReader; a zero field takes its
// default. Archive reads come from object storage, hundreds of
// milliseconds per part, so the defaults are far longer than a
// JetStream API call's.
type Options struct {
	// RequestTimeout bounds one request. Default 15s.
	RequestTimeout time.Duration
	// ReadTimeout bounds a whole ReadRange, every page and retry.
	// Default 2m.
	ReadTimeout time.Duration
	// BusyRetryMax is how many busy replies a page is retried after.
	// Default 5.
	BusyRetryMax int
	// BusyBackoff is the base wait after a busy reply; retry n waits n
	// times it. Default 500ms.
	BusyBackoff time.Duration
}

func (o Options) withDefaults() Options {
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 15 * time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 2 * time.Minute
	}
	if o.BusyRetryMax <= 0 {
		o.BusyRetryMax = 5
	}
	if o.BusyBackoff <= 0 {
		o.BusyBackoff = 500 * time.Millisecond
	}
	return o
}

type natsArchiveReader struct {
	nc      *nats.Conn
	subject string
	opts    Options
}

// NewReader reads stream's archive over nc, on
// dagnatsext.ArchiveRangeSubject(domain, stream). domain is the JetStream domain of
// the server that holds the stream ("" for none).
func NewReader(
	nc *nats.Conn, domain, stream string, opts Options,
) dagnatsext.ArchiveReader {
	if nc == nil {
		panic("archivenats.NewReader: nc must not be nil")
	}
	subject := dagnatsext.ArchiveRangeSubject(domain, stream)
	if subject == "" {
		panic("archivenats.NewReader: empty subject")
	}
	return &natsArchiveReader{nc: nc, subject: subject, opts: opts.withDefaults()}
}

// pageCursor is where the next page starts.
type pageCursor struct {
	seq    uint64
	offset int
}

// ReadRange pages through the range until the archive says done, the
// limit is reached, or a bound or the read timeout is hit.
func (r *natsArchiveReader) ReadRange(
	ctx context.Context, rng dagnatsext.ArchiveRange,
) (dagnatsext.ArchivedRange, error) {
	if ctx == nil {
		panic("natsArchiveReader.ReadRange: ctx must not be nil")
	}
	if rng.FromSeq == 0 || rng.ToSeq <= rng.FromSeq || rng.Limit < 0 {
		panic("natsArchiveReader.ReadRange: need 0 < from_seq < to_seq and limit >= 0")
	}
	ctx, cancel := context.WithTimeout(ctx, r.opts.ReadTimeout)
	defer cancel()
	var out dagnatsext.ArchivedRange
	var carry *dagnatsext.ArchivedMessage
	cursor := pageCursor{seq: rng.FromSeq}
	for page := 0; page < archivePagesMax; page++ {
		reply, err := r.requestPage(ctx, rng, cursor)
		if err != nil {
			return dagnatsext.ArchivedRange{}, err
		}
		out.Gaps = append(out.Gaps, reply.Gaps...)
		carry, err = absorbPage(&out, carry, reply, rng)
		if err != nil {
			return dagnatsext.ArchivedRange{}, err
		}
		if rng.Limit > 0 && len(out.Messages) >= rng.Limit {
			out.Messages = out.Messages[:rng.Limit]
			return out, nil
		}
		next, finished, err := advance(cursor, reply, rng, carry != nil)
		if err != nil || finished {
			return out, err
		}
		cursor = next
	}
	return dagnatsext.ArchivedRange{}, fmt.Errorf("%w: more than %d pages",
		dagnatsext.ErrArchiveProtocol, archivePagesMax)
}

// advance moves the cursor past a page and reports whether the range is
// read. It refuses a reply that would not make progress.
func advance(
	cursor pageCursor, reply dagnatsext.ArchiveRangeReply, rng dagnatsext.ArchiveRange, carrying bool,
) (pageCursor, bool, error) {
	if cursor.seq == 0 {
		panic("archivenats.advance: cursor must be positive")
	}
	if reply.Done {
		if carrying {
			return cursor, true, fmt.Errorf("%w: done inside a split message", dagnatsext.ErrArchiveProtocol)
		}
		return cursor, true, nil
	}
	next := pageCursor{seq: reply.NextSeq}
	if reply.More {
		next.offset = reply.NextOffset
	}
	if !reply.More && next.seq >= rng.ToSeq {
		return cursor, true, nil
	}
	moved := next.seq > cursor.seq || (next.seq == cursor.seq && next.offset > cursor.offset)
	if !moved {
		return cursor, true, fmt.Errorf("%w: page at seq %d offset %d made no progress",
			dagnatsext.ErrArchiveProtocol, cursor.seq, cursor.offset)
	}
	return next, false, nil
}

// absorbPage appends a page's complete messages to out and returns the
// message the page ends inside, joining carry with its continuation.
func absorbPage(
	out *dagnatsext.ArchivedRange, carry *dagnatsext.ArchivedMessage,
	reply dagnatsext.ArchiveRangeReply, rng dagnatsext.ArchiveRange,
) (*dagnatsext.ArchivedMessage, error) {
	if out == nil {
		panic("archivenats.absorbPage: out must not be nil")
	}
	if reply.More && len(reply.Messages) == 0 {
		return nil, fmt.Errorf("%w: more without a message", dagnatsext.ErrArchiveProtocol)
	}
	for i := range reply.Messages {
		m := reply.Messages[i]
		if carry != nil {
			joined, err := join(*carry, m, i)
			if err != nil {
				return nil, err
			}
			m, carry = joined, nil
		}
		if err := checkMessage(out, m, rng); err != nil {
			return nil, err
		}
		if i == len(reply.Messages)-1 && reply.More {
			if reply.NextSeq != m.Seq || reply.NextOffset != len(m.Data) {
				return nil, fmt.Errorf("%w: split of seq %d resumes at seq %d offset %d",
					dagnatsext.ErrArchiveProtocol, m.Seq, reply.NextSeq, reply.NextOffset)
			}
			return &m, nil
		}
		out.Messages = append(out.Messages, m)
	}
	if carry != nil {
		return nil, fmt.Errorf("%w: split seq %d not continued", dagnatsext.ErrArchiveProtocol, carry.Seq)
	}
	return nil, nil
}

// join appends a continuation to a split message, keeping the first
// part's subject, time and headers.
func join(carry, part dagnatsext.ArchivedMessage, index int) (dagnatsext.ArchivedMessage, error) {
	if carry.Seq == 0 {
		panic("archivenats.join: carry must have a seq")
	}
	if index != 0 || part.Seq != carry.Seq {
		return dagnatsext.ArchivedMessage{}, fmt.Errorf("%w: split seq %d continued by seq %d",
			dagnatsext.ErrArchiveProtocol, carry.Seq, part.Seq)
	}
	if len(carry.Data)+len(part.Data) > archiveMessageBytesMax {
		return dagnatsext.ArchivedMessage{}, fmt.Errorf("%w: seq %d exceeds %d bytes",
			dagnatsext.ErrArchiveProtocol, carry.Seq, archiveMessageBytesMax)
	}
	carry.Data = append(append([]byte(nil), carry.Data...), part.Data...)
	return carry, nil
}

// checkMessage holds a message to the range: in bounds, after the last
// one, on the filtered subject, within the read bound.
func checkMessage(
	out *dagnatsext.ArchivedRange, m dagnatsext.ArchivedMessage, rng dagnatsext.ArchiveRange,
) error {
	if out == nil {
		panic("archivenats.checkMessage: out must not be nil")
	}
	if m.Seq < rng.FromSeq || m.Seq >= rng.ToSeq {
		return fmt.Errorf("%w: seq %d outside [%d, %d)",
			dagnatsext.ErrArchiveProtocol, m.Seq, rng.FromSeq, rng.ToSeq)
	}
	if n := len(out.Messages); n > 0 && m.Seq <= out.Messages[n-1].Seq {
		return fmt.Errorf("%w: seq %d after seq %d",
			dagnatsext.ErrArchiveProtocol, m.Seq, out.Messages[n-1].Seq)
	}
	if rng.Subject != "" && m.Subject != rng.Subject {
		return fmt.Errorf("%w: seq %d is on %q, not %q",
			dagnatsext.ErrArchiveProtocol, m.Seq, m.Subject, rng.Subject)
	}
	if len(out.Messages) >= archiveMessagesMax {
		return fmt.Errorf("%w: more than %d messages", dagnatsext.ErrArchiveProtocol, archiveMessagesMax)
	}
	return nil
}

// requestPage asks for one page, retrying a busy archive a bounded
// number of times with a growing wait.
func (r *natsArchiveReader) requestPage(
	ctx context.Context, rng dagnatsext.ArchiveRange, cursor pageCursor,
) (dagnatsext.ArchiveRangeReply, error) {
	if cursor.seq == 0 || cursor.offset < 0 {
		panic("natsArchiveReader.requestPage: cursor out of range")
	}
	data, err := json.Marshal(dagnatsext.ArchiveRangeRequest{
		Epoch: rng.Epoch, Subject: rng.Subject, FromSeq: cursor.seq, ToSeq: rng.ToSeq,
		Offset: cursor.offset, MaxBytes: dagnatsext.ArchiveMaxBytes,
	})
	if err != nil {
		panic("natsArchiveReader.requestPage: marshal: " + err.Error())
	}
	var lastErr error
	for attempt := 0; attempt <= r.opts.BusyRetryMax; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return dagnatsext.ArchiveRangeReply{}, fmt.Errorf("archive: %s: %w (%v)",
					r.subject, ctx.Err(), lastErr)
			case <-time.After(time.Duration(attempt) * r.opts.BusyBackoff):
			}
		}
		reply, err := r.request(ctx, data)
		if err == nil || !errors.Is(err, dagnatsext.ErrArchiveBusy) {
			return reply, err
		}
		lastErr = err
	}
	return dagnatsext.ArchiveRangeReply{}, fmt.Errorf("archive: %s still busy after %d retries: %w",
		r.subject, r.opts.BusyRetryMax, lastErr)
}

// request is one request/reply under RequestTimeout.
func (r *natsArchiveReader) request(
	ctx context.Context, data []byte,
) (dagnatsext.ArchiveRangeReply, error) {
	if len(data) == 0 {
		panic("natsArchiveReader.request: data must not be empty")
	}
	reqCtx, cancel := context.WithTimeout(ctx, r.opts.RequestTimeout)
	defer cancel()
	msg, err := r.nc.RequestWithContext(reqCtx, r.subject, data)
	if errors.Is(err, nats.ErrNoResponders) {
		return dagnatsext.ArchiveRangeReply{}, fmt.Errorf("%w on %s",
			dagnatsext.ErrArchiveUnavailable, r.subject)
	}
	if err != nil {
		return dagnatsext.ArchiveRangeReply{}, fmt.Errorf("archive: request %s: %w", r.subject, err)
	}
	if msg == nil {
		panic("natsArchiveReader.request: nil reply without error")
	}
	return decodeReply(msg.Data)
}

// decodeReply parses a reply and turns an error reply into
// *dagnatsext.ArchiveError.
func decodeReply(data []byte) (dagnatsext.ArchiveRangeReply, error) {
	if data == nil {
		return dagnatsext.ArchiveRangeReply{}, fmt.Errorf("%w: empty reply",
			dagnatsext.ErrArchiveProtocol)
	}
	var reply dagnatsext.ArchiveRangeReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return dagnatsext.ArchiveRangeReply{}, fmt.Errorf("%w: decode reply: %v",
			dagnatsext.ErrArchiveProtocol, err)
	}
	if reply.Error != "" || reply.Code != "" {
		return reply, &dagnatsext.ArchiveError{
			Code: reply.Code, Message: reply.Error, Floor: reply.Floor,
		}
	}
	return reply, nil
}

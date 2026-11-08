// Package archivetest is a fake archive for tests: a subscriber on the
// archive range subject (dagnatsext.ArchiveRangeSubject) that implements
// the range protocol over an in-memory copy of a stream's messages, plus
// helpers to snapshot and purge a stream the way an archiver would.
package archivetest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// opTimeout bounds each helper's JetStream call.
const opTimeout = 10 * time.Second

// snapshotMax bounds Snapshot.
const snapshotMax = 100_000

// Responder answers the range protocol. Set the knobs before the first
// request; Requests counts requests received.
type Responder struct {
	Requests atomic.Int64

	mu       sync.Mutex
	epoch    int64
	messages []dagnatsext.ArchivedMessage
	last     []byte

	// PageBytes caps the raw data bytes of one reply (default
	// dagnatsext.ArchiveMaxBytes), so a small value splits messages.
	PageBytes int
	// PageMessages caps the messages of one reply (0 = no cap).
	PageMessages int
	// Code, when set, answers every request with that error code.
	Code string
	// Floor is what a not_archived reply carries.
	Floor uint64
	// BusyFor answers busy to the first BusyFor requests.
	BusyFor int64
	// Gaps are reported when they overlap the requested range.
	Gaps []dagnatsext.ArchiveGap
	// Drop omits the message with this seq, as a hole the archive does
	// not report.
	Drop uint64
	// Silent never replies, so requests time out.
	Silent bool
}

// Serve subscribes a Responder on stream's range subject in domain,
// holding messages (in seq order) for the stream epoch.
func Serve(
	t testing.TB, nc *nats.Conn, domain, stream string, epoch int64,
	messages []dagnatsext.ArchivedMessage,
) *Responder {
	t.Helper()
	r := &Responder{epoch: epoch, messages: messages}
	sub, err := nc.Subscribe(dagnatsext.ArchiveRangeSubject(domain, stream), r.handle)
	if err != nil {
		t.Fatalf("archivetest.Serve: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("archivetest.Serve flush: %v", err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			t.Logf("archivetest: unsubscribe: %v", err)
		}
	})
	return r
}

// LastRequest is the raw body of the most recent request.
func (r *Responder) LastRequest() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.last...)
}

func (r *Responder) handle(msg *nats.Msg) {
	count := r.Requests.Add(1)
	r.mu.Lock()
	r.last = append([]byte(nil), msg.Data...)
	r.mu.Unlock()
	if r.Silent {
		return
	}
	reply := r.answer(msg.Data, count)
	data, err := json.Marshal(reply)
	if err != nil {
		panic("archivetest: marshal reply: " + err.Error())
	}
	if err := msg.Respond(data); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		panic("archivetest: respond: " + err.Error())
	}
}

func (r *Responder) answer(body []byte, count int64) dagnatsext.ArchiveRangeReply {
	var req dagnatsext.ArchiveRangeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return dagnatsext.ArchiveRangeReply{Error: err.Error(), Code: "bad_request"}
	}
	switch {
	case count <= r.BusyFor:
		return dagnatsext.ArchiveRangeReply{Error: "busy", Code: dagnatsext.ArchiveCodeBusy}
	case r.Code != "":
		return dagnatsext.ArchiveRangeReply{Error: r.Code, Code: r.Code, Floor: r.Floor}
	case req.Epoch != r.epoch:
		return dagnatsext.ArchiveRangeReply{
			Error: "no such epoch", Code: dagnatsext.ArchiveCodeUnknownEpoch,
		}
	}
	return r.page(req)
}

// page serves from req.FromSeq at req.Offset up to the page caps.
func (r *Responder) page(req dagnatsext.ArchiveRangeRequest) dagnatsext.ArchiveRangeReply {
	budget := min(req.MaxBytes, dagnatsext.ArchiveMaxBytes)
	if r.PageBytes > 0 {
		budget = min(budget, r.PageBytes)
	}
	reply := dagnatsext.ArchiveRangeReply{Messages: []dagnatsext.ArchivedMessage{}}
	for _, g := range r.Gaps {
		if g.To >= req.FromSeq && g.From < req.ToSeq {
			reply.Gaps = append(reply.Gaps, g)
		}
	}
	offset := req.Offset
	for _, m := range r.messages {
		if m.Seq < req.FromSeq || m.Seq >= req.ToSeq || m.Seq == r.Drop ||
			(req.Subject != "" && m.Subject != req.Subject) {
			continue
		}
		full := r.PageMessages > 0 && len(reply.Messages) >= r.PageMessages
		if full || (budget <= 0 && len(reply.Messages) > 0) {
			reply.NextSeq = m.Seq
			return reply
		}
		rest := m.Data[min(offset, len(m.Data)):]
		part := m
		part.Data = rest
		if len(rest) > budget && budget > 0 {
			part.Data = rest[:budget]
			reply.Messages = append(reply.Messages, part)
			reply.NextSeq, reply.NextOffset, reply.More = m.Seq, offset+budget, true
			return reply
		}
		reply.Messages = append(reply.Messages, part)
		budget -= len(rest)
		offset = 0
	}
	reply.NextSeq, reply.Done = req.ToSeq, true
	return reply
}

// Snapshot copies stream's messages below seq "below", as an archiver
// would hold them, with times truncated to the protocol's microseconds.
func Snapshot(
	t testing.TB, js jetstream.JetStream, stream string, below uint64,
) []dagnatsext.ArchivedMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("archivetest.Snapshot: stream: %v", err)
	}
	var out []dagnatsext.ArchivedMessage
	for seq := uint64(1); seq < below && seq < snapshotMax; seq++ {
		raw, err := s.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("archivetest.Snapshot: get %d: %v", seq, err)
		}
		out = append(out, dagnatsext.ArchivedMessage{
			Seq: raw.Sequence, Subject: raw.Subject, Time: raw.Time.UTC().Truncate(time.Microsecond),
			Headers: map[string][]string(raw.Header), Data: raw.Data,
		})
	}
	return out
}

// PurgeBelow purges stream below seq and returns the stream epoch
// (StreamInfo.Created.UnixNano()) and its new FirstSeq.
func PurgeBelow(t testing.TB, js jetstream.JetStream, stream string, seq uint64) (int64, uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("archivetest.PurgeBelow: stream: %v", err)
	}
	if err := s.Purge(ctx, jetstream.WithPurgeSequence(seq)); err != nil {
		t.Fatalf("archivetest.PurgeBelow: purge: %v", err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("archivetest.PurgeBelow: info: %v", err)
	}
	return info.Created.UnixNano(), info.State.FirstSeq
}

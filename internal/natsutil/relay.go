package natsutil

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The engine relay (#885). A program may not publish history.> or
// journal.>, nor read the journal stream, itself: it could forge or read
// another program's step events and journal records. Its worker sends
// them to the engine on api.p.<name>.<op>, and the engine relays one
// only for the program's own step and current dispatch.
//
// Wire format: a request carries the record's own body and headers
// unchanged, plus RelaySubjectHeader naming the subject it is for, so
// payload limits are those of a direct publish. Replies carry their
// result in headers (and, for a view, the record's body).
const (
	RelayEventsOp  = "events"
	RelayJournalOp = "journal.append"
	RelayViewOp    = "journal.view"
	// The stores a program reaches only through the engine: its step's
	// checkpoint, signals to runs it takes part in, and its step's agent
	// inputs. The operation is RelayOpHeader.
	RelayCheckpointOp = "checkpoint"
	RelaySignalOp     = "signal"
	RelayInputsOp     = "inputs"

	RelaySubjectHeader   = "Dagnats-Relay-Subject"
	RelayOpHeader        = "Dagnats-Relay-Op"
	RelayRunHeader       = "Dagnats-Relay-Run"
	RelayStepHeader      = "Dagnats-Relay-Step"
	RelayKeyHeader       = "Dagnats-Relay-Key"
	RelayRevisionHeader  = "Dagnats-Relay-Revision"
	RelayFromSeqHeader   = "Dagnats-Relay-From-Seq"
	RelayStreamHeader    = "Dagnats-Relay-Stream"
	RelaySeqHeader       = "Dagnats-Relay-Seq"
	RelayTimeHeader      = "Dagnats-Relay-Time"
	RelayDuplicateHeader = "Dagnats-Relay-Duplicate"
	RelayEmptyHeader     = "Dagnats-Relay-Empty"
	RelayRefusalHeader   = "Dagnats-Relay-Refusal"
	RelayDetailHeader    = "Dagnats-Relay-Detail"
	RelayAPICodeHeader   = "Dagnats-Relay-Api-Code"
	RelayAPIErrHeader    = "Dagnats-Relay-Api-Err-Code"
)

// Relay refusal codes, one per check, so a caller (and a test) can tell
// which check fired.
const (
	RefusalBadRequest = "bad_request"
	RefusalSubject    = "bad_subject"
	RefusalEventType  = "not_worker_event"
	RefusalIdentity   = "no_identity"
	RefusalMsgID      = "bad_msg_id"
	RefusalNotYours   = "not_yours"
	RefusalBusy       = "busy"
)

// relayTimeout bounds one relay round trip when ctx has no deadline.
const relayTimeout = 10 * time.Second

// ErrRelayRefused matches every *RelayRefusal.
var ErrRelayRefused = errors.New("relay refused")

// RelayRefusal is the engine declining to relay, with the check that
// fired.
type RelayRefusal struct {
	Code   string
	Detail string
}

func (r *RelayRefusal) Error() string {
	return fmt.Sprintf("relay refused: %s: %s", r.Code, r.Detail)
}

// Is makes errors.Is(err, ErrRelayRefused) hold for any refusal.
func (r *RelayRefusal) Is(target error) bool { return target == ErrRelayRefused }

// RelayedRecord is one stored journal message, as journal.view returns it.
type RelayedRecord struct {
	Seq    uint64
	Header nats.Header
	Data   []byte
	Time   time.Time
}

// RelayOp returns the op that relays a publish to subject, or "" when
// subject is published directly.
func RelayOp(subject string) string {
	switch {
	case strings.HasPrefix(subject, "history."):
		return RelayEventsOp
	case strings.HasPrefix(subject, "journal."):
		return RelayJournalOp
	}
	return ""
}

// WithRelay returns a copy of tp that relays history.> and journal.>
// publishes through prefix ("api.p.<name>."), and every other publish
// as before.
func (tp *TracingPublisher) WithRelay(prefix string) *TracingPublisher {
	if tp == nil || tp.nc == nil {
		panic("TracingPublisher.WithRelay: tp needs a core connection")
	}
	if !strings.HasPrefix(prefix, "api.p.") || !strings.HasSuffix(prefix, ".") {
		panic("TracingPublisher.WithRelay: prefix must be \"api.p.<name>.\"")
	}
	relayed := *tp
	relayed.relay = prefix
	return &relayed
}

// RelayProgram returns the program name tp relays for, or "" when it is
// not relayed.
func (tp *TracingPublisher) RelayProgram() string {
	if tp == nil {
		panic("TracingPublisher.RelayProgram: tp must not be nil")
	}
	name := strings.TrimPrefix(tp.relay, "api.p.")
	return strings.TrimSuffix(name, ".")
}

// RelayCall sends one store operation to the relay op and returns the
// reply, or the refusal or JetStream error it carries. It is the
// building block for the relayed checkpoint, signal and input stores.
func (tp *TracingPublisher) RelayCall(
	ctx context.Context, op string, header nats.Header, data []byte,
) (*nats.Msg, error) {
	if tp == nil || tp.relay == "" {
		panic("TracingPublisher.RelayCall: tp is not relayed")
	}
	if op == "" || header == nil {
		panic("TracingPublisher.RelayCall: op and header must be set")
	}
	reply, err := tp.relayRequest(ctx, op, header, data)
	if err != nil {
		return nil, fmt.Errorf("relay %s: %w", op, err)
	}
	if err := relayError(reply.Header); err != nil {
		return nil, err
	}
	return reply, nil
}

// Relayed reports whether tp goes through the engine relay.
func (tp *TracingPublisher) Relayed() bool {
	if tp == nil {
		panic("TracingPublisher.Relayed: tp must not be nil")
	}
	return tp.relay != ""
}

// relayPublish sends msg to the relay and turns the reply back into
// what a direct JetStream publish returns: a PubAck, the JetStream API
// error (so the journal's compare-and-set sees the same code), or a
// *RelayRefusal.
func (tp *TracingPublisher) relayPublish(
	ctx context.Context, msg *nats.Msg, op string, optCount int,
) (*jetstream.PubAck, error) {
	if tp.relay == "" || op == "" {
		panic("relayPublish: only for a relayed subject")
	}
	if optCount != 0 {
		panic("relayPublish: publish options cannot cross the relay; put them in headers")
	}
	header := cloneHeader(msg.Header)
	header.Set(RelaySubjectHeader, msg.Subject)
	reply, err := tp.relayRequest(ctx, op, header, msg.Data)
	if err != nil {
		return nil, fmt.Errorf("relay %s: %w", msg.Subject, err)
	}
	if err := relayError(reply.Header); err != nil {
		return nil, err
	}
	seq, err := strconv.ParseUint(reply.Header.Get(RelaySeqHeader), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("relay %s: reply has no sequence", msg.Subject)
	}
	return &jetstream.PubAck{
		Stream: reply.Header.Get(RelayStreamHeader), Sequence: seq,
		Duplicate: reply.Header.Get(RelayDuplicateHeader) == "true",
	}, nil
}

// RelayView reads one journal record on subject for the dispatch nonce
// holds: the first at or after fromSeq, or the last when fromSeq is 0.
// found is false when there is none.
func (tp *TracingPublisher) RelayView(
	ctx context.Context, subject, nonce string, fromSeq uint64,
) (record RelayedRecord, found bool, err error) {
	if tp.relay == "" {
		panic("RelayView: tp is not relayed")
	}
	if subject == "" || nonce == "" {
		panic("RelayView: subject and nonce must be set")
	}
	header := nats.Header{}
	header.Set(RelaySubjectHeader, subject)
	header.Set("Dagnats-Dispatch-Nonce", nonce)
	header.Set(RelayFromSeqHeader, strconv.FormatUint(fromSeq, 10))
	reply, err := tp.relayRequest(ctx, RelayViewOp, header, nil)
	if err != nil {
		return RelayedRecord{}, false, fmt.Errorf("relay view %s: %w", subject, err)
	}
	if err := relayError(reply.Header); err != nil {
		return RelayedRecord{}, false, err
	}
	if reply.Header.Get(RelayEmptyHeader) == "true" {
		return RelayedRecord{}, false, nil
	}
	return decodeRelayedRecord(subject, reply)
}

func decodeRelayedRecord(subject string, reply *nats.Msg) (RelayedRecord, bool, error) {
	seq, err := strconv.ParseUint(reply.Header.Get(RelaySeqHeader), 10, 64)
	if err != nil || seq == 0 {
		return RelayedRecord{}, false, fmt.Errorf("relay view %s: reply has no sequence", subject)
	}
	stamp, err := time.Parse(time.RFC3339Nano, reply.Header.Get(RelayTimeHeader))
	if err != nil {
		return RelayedRecord{}, false, fmt.Errorf("relay view %s: reply has no time", subject)
	}
	header := nats.Header{}
	for key, values := range reply.Header {
		if !strings.HasPrefix(key, "Dagnats-Relay-") {
			header[key] = values
		}
	}
	return RelayedRecord{Seq: seq, Header: header, Data: reply.Data, Time: stamp}, true, nil
}

func (tp *TracingPublisher) relayRequest(
	ctx context.Context, op string, header nats.Header, data []byte,
) (*nats.Msg, error) {
	if op == "" || header == nil {
		panic("relayRequest: op and header must be set")
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, relayTimeout)
		defer cancel()
	}
	request := &nats.Msg{Subject: tp.relay + op, Header: header, Data: data}
	return tp.nc.RequestMsgWithContext(ctx, request)
}

// relayError decodes a refusal or a JetStream API error from a reply's
// headers, or returns nil.
func relayError(header nats.Header) error {
	if code := header.Get(RelayRefusalHeader); code != "" {
		return &RelayRefusal{Code: code, Detail: header.Get(RelayDetailHeader)}
	}
	if raw := header.Get(RelayAPIErrHeader); raw != "" {
		errCode, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("relay: malformed API error %q", raw)
		}
		status, err := strconv.Atoi(header.Get(RelayAPICodeHeader))
		if err != nil {
			status = 0 // the HTTP-style status is informational; the error code decides
		}
		return &jetstream.APIError{
			Code: status, ErrorCode: jetstream.ErrorCode(errCode),
			Description: header.Get(RelayDetailHeader),
		}
	}
	return nil
}

func cloneHeader(header nats.Header) nats.Header {
	out := nats.Header{}
	for key, values := range header {
		out[key] = append([]string(nil), values...)
	}
	return out
}

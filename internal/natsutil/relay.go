package natsutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Relayed subject families and the api.p.<name>.<op> each goes through.
// A program may not publish history.> or journal.> itself (it could
// forge another program's step events or journal records); the engine
// relays them after checking the step is the program's and the dispatch
// nonce is current (#885).
const (
	relayEventsOp  = "events"
	relayJournalOp = "journal.append"
)

// relayTimeout bounds one relayed publish when ctx has no deadline. The
// engine relays with one JetStream publish, so this is that publish's
// timeout plus a round trip.
const relayTimeout = 10 * time.Second

// RelayRequest is a publish the engine relays for a program: the subject,
// headers and body the program would have published itself.
type RelayRequest struct {
	Subject string      `json:"subject"`
	Header  nats.Header `json:"header,omitempty"`
	Data    []byte      `json:"data"`
}

// RelayReply is the relay's answer: the PubAck of the relayed publish,
// or the JetStream API error it got (so the journal's compare-and-set
// handling sees the same error code), or a refusal.
type RelayReply struct {
	Stream    string              `json:"stream,omitempty"`
	Sequence  uint64              `json:"seq,omitempty"`
	Duplicate bool                `json:"duplicate,omitempty"`
	APIError  *jetstream.APIError `json:"api_error,omitempty"`
	Error     string              `json:"error,omitempty"`
}

// ErrRelayRefused is returned when the engine refuses to relay a publish.
var ErrRelayRefused = errors.New("relay refused")

// RelayOp returns the api.p.<name> operation that relays subject, or ""
// when subject is published directly.
func RelayOp(subject string) string {
	switch {
	case strings.HasPrefix(subject, "history."):
		return relayEventsOp
	case strings.HasPrefix(subject, "journal."):
		return relayJournalOp
	}
	return ""
}

// WithRelay returns a copy of tp that sends history.> and journal.>
// publishes as requests to prefix+"events" and prefix+"journal.append"
// ("api.p.<name>."), and every other publish as before.
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

// Relayed reports whether tp sends history.> and journal.> through the
// engine relay.
func (tp *TracingPublisher) Relayed() bool {
	if tp == nil {
		panic("TracingPublisher.Relayed: tp must not be nil")
	}
	return tp.relay != ""
}

// relayPublish sends msg to the engine relay and turns its reply back
// into what a direct JetStream publish would have returned.
func (tp *TracingPublisher) relayPublish(
	ctx context.Context, msg *nats.Msg, op string, optCount int,
) (*jetstream.PubAck, error) {
	if tp.relay == "" || op == "" {
		panic("relayPublish: only for a relayed subject")
	}
	if optCount != 0 {
		panic("relayPublish: publish options cannot cross the relay; put them in headers")
	}
	request := RelayRequest{Subject: msg.Subject, Header: msg.Header, Data: msg.Data}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode relay request: %w", err)
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, relayTimeout)
		defer cancel()
	}
	reply, err := tp.nc.RequestWithContext(ctx, tp.relay+op, body)
	if err != nil {
		return nil, fmt.Errorf("relay %s: %w", msg.Subject, err)
	}
	var answer RelayReply
	if err := json.Unmarshal(reply.Data, &answer); err != nil {
		return nil, fmt.Errorf("relay %s: reply: %w", msg.Subject, err)
	}
	switch {
	case answer.APIError != nil:
		return nil, answer.APIError
	case answer.Error != "":
		return nil, fmt.Errorf("%w: %s: %s", ErrRelayRefused, msg.Subject, answer.Error)
	}
	return &jetstream.PubAck{
		Stream: answer.Stream, Sequence: answer.Sequence, Duplicate: answer.Duplicate,
	}, nil
}

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

// relayTimeout bounds one relay: a run load, a def load and a publish.
const relayTimeout = 10 * time.Second

// errNotYours is the one refusal for every ownership failure, so a
// program cannot tell another program's run from a run that does not
// exist, or a stale nonce from a foreign step.
var errNotYours = errors.New("no step of this program holds that dispatch")

// relayedEventTypes are the step events a worker publishes. Everything
// else on history.> is the engine's own.
var relayedEventTypes = map[protocol.EventType]bool{
	protocol.EventStepStarted:   true,
	protocol.EventStepContinue:  true,
	protocol.EventStepCompleted: true,
	protocol.EventStepFailed:    true,
}

// relayEndpoints are the publishes a program may not make itself (#885).
func (n *NATSAPI) relayEndpoints() []endpointSpec {
	return []endpointSpec{
		{"program-events", "api.p.*.events",
			n.relayHandler(n.checkEvent), programQueueGroup},
		{"program-journal-append", "api.p.*.journal.append",
			n.relayHandler(n.checkJournal), programQueueGroup},
	}
}

// relayCheck validates one relayed publish for the program owning prefix
// and returns the headers to publish it with.
type relayCheck func(
	ctx context.Context, prefix string, r natsutil.RelayRequest,
) (nats.Header, error)

func (n *NATSAPI) relayHandler(check relayCheck) micro.HandlerFunc {
	if check == nil {
		panic("relayHandler: check must not be nil")
	}
	return func(req micro.Request) {
		n.reply(req, n.relay(req, check))
	}
}

func (n *NATSAPI) relay(req micro.Request, check relayCheck) natsutil.RelayReply {
	if req == nil || n.svc == nil || n.svc.tp == nil {
		panic("relay: req and svc must be set")
	}
	prefix, err := programPrefix(req.Subject())
	if err != nil {
		return natsutil.RelayReply{Error: err.Error()}
	}
	var r natsutil.RelayRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		return natsutil.RelayReply{Error: "relay request: " + err.Error()}
	}
	// The program's own trace context, so the relayed record joins the
	// program's trace and the publisher re-injects the same parent.
	traced := observe.ExtractTraceContextHeader(r.Header)
	ctx, cancel := context.WithTimeout(traced, relayTimeout)
	defer cancel()
	header, err := check(ctx, prefix, r)
	if err != nil {
		return natsutil.RelayReply{Error: err.Error()}
	}
	msg := &nats.Msg{Subject: r.Subject, Header: header, Data: r.Data}
	ack, err := n.svc.tp.JSPublishMsg(ctx, msg)
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return natsutil.RelayReply{APIError: apiErr}
	}
	if err != nil {
		return natsutil.RelayReply{Error: err.Error()}
	}
	return natsutil.RelayReply{Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}
}

// checkEvent admits a worker's step event: one of the four worker event
// types, for history.<its run>, with dispatch identity (the relay never
// takes the legacy identity-less path), a dedup id that is the event's
// own, on a step whose task is the program's and whose current nonce it
// carries.
func (n *NATSAPI) checkEvent(
	ctx context.Context, prefix string, r natsutil.RelayRequest,
) (nats.Header, error) {
	var evt protocol.Event
	if err := json.Unmarshal(r.Data, &evt); err != nil {
		return nil, fmt.Errorf("event: %w", err)
	}
	if !relayedEventTypes[evt.Type] {
		return nil, fmt.Errorf("event type %q is not a worker event", evt.Type)
	}
	if evt.RunID == "" || evt.StepID == "" || r.Subject != evt.NATSSubject() {
		return nil, fmt.Errorf("event subject %q does not match its run and step", r.Subject)
	}
	identified := evt.Type == protocol.EventStepStarted || evt.HasIdentity()
	if !identified || evt.DispatchNonce == "" {
		return nil, errors.New("event carries no dispatch identity")
	}
	if r.Header.Get(jetstream.MsgIDHeader) != evt.NATSMsgID() {
		return nil, errors.New("event dedup id is not the event's own")
	}
	if err := n.ownsDispatch(ctx, prefix, evt.RunID, evt.StepID, evt.DispatchNonce); err != nil {
		return nil, err
	}
	return keepHeaders(r.Header, func(key string) bool { return false }), nil
}

// checkJournal admits a journal record for journal.<run>.<step token>
// on a step whose task is the program's, carrying its current nonce.
// The compare-and-set header passes through, so the stream still orders
// the journal's writers.
func (n *NATSAPI) checkJournal(
	ctx context.Context, prefix string, r natsutil.RelayRequest,
) (nats.Header, error) {
	parts := strings.SplitN(r.Subject, ".", 3)
	if len(parts) != 3 || parts[0] != "journal" || parts[1] == "" || parts[2] == "" {
		return nil, fmt.Errorf("journal subject %q is not journal.<run>.<step>", r.Subject)
	}
	nonce := r.Header.Get(journal.HeaderNonce)
	if nonce == "" || r.Header.Get(jetstream.MsgIDHeader) == "" {
		return nil, errors.New("journal record carries no dispatch nonce or dedup id")
	}
	stepID, err := n.stepByToken(ctx, parts[1], parts[2])
	if err != nil {
		return nil, err
	}
	if err := n.ownsDispatch(ctx, prefix, parts[1], stepID, nonce); err != nil {
		return nil, err
	}
	return keepHeaders(r.Header, func(key string) bool {
		return key == jetstream.ExpectedLastSubjSeqHeader || strings.HasPrefix(key, "Dagnats-")
	}), nil
}

// keepHeaders copies the dedup id and trace context, plus any header
// extra admits. Every other header (expected stream, rollup, TTL) could
// change how the relayed publish is stored, so it is dropped.
func keepHeaders(header nats.Header, extra func(string) bool) nats.Header {
	if extra == nil {
		panic("keepHeaders: extra must not be nil")
	}
	out := nats.Header{}
	for key, values := range header {
		canonical := key
		switch {
		case canonical == jetstream.MsgIDHeader, strings.EqualFold(canonical, "traceparent"),
			strings.EqualFold(canonical, "tracestate"), extra(canonical):
			out[canonical] = append([]string(nil), values...)
		}
	}
	return out
}

// ownsDispatch reports nil only when runID's stepID dispatches a task
// under prefix and nonce is that step's current dispatch nonce.
func (n *NATSAPI) ownsDispatch(ctx context.Context, prefix, runID, stepID, nonce string) error {
	if prefix == "" || nonce == "" {
		panic("ownsDispatch: prefix and nonce must be set")
	}
	def, run, err := n.svc.runAndDef(ctx, runID)
	if err != nil {
		return errNotYours
	}
	for _, step := range def.Steps {
		if step.ID != stepID {
			continue
		}
		state, ok := run.Steps[stepID]
		if !ok || !underPrefix(step.Task, prefix) || state.DispatchNonce != nonce {
			return errNotYours
		}
		return nil
	}
	return errNotYours
}

// stepByToken maps a journal subject's step token back to the step ID:
// natsutil.SubjectToken is lossy, so two steps sharing a token are
// ambiguous and refused.
func (n *NATSAPI) stepByToken(ctx context.Context, runID, token string) (string, error) {
	if runID == "" || token == "" {
		panic("stepByToken: runID and token must be set")
	}
	def, _, err := n.svc.runAndDef(ctx, runID)
	if err != nil {
		return "", errNotYours
	}
	found := ""
	for _, step := range def.Steps {
		if natsutil.SubjectToken(step.ID) != token {
			continue
		}
		if found != "" {
			return "", fmt.Errorf("journal step token %q is ambiguous", token)
		}
		found = step.ID
	}
	if found == "" {
		return "", errNotYours
	}
	return found, nil
}

// runAndDef loads a run and the definition it is pinned to (the version
// key when the run carries a def hash), with planner-added steps folded
// in. Unlike the engine it never self-heals a missing version: the
// relay fails closed.
func (s *Service) runAndDef(
	ctx context.Context, runID string,
) (dag.WorkflowDef, dag.WorkflowRun, error) {
	if s.store == nil || s.defKV == nil {
		panic("runAndDef: store and defKV must be set")
	}
	run, err := s.store.Load(ctx, runID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{}, err
	}
	key := run.WorkflowID
	if run.DefHash != "" {
		key = dag.DefVersionKey(run.WorkflowID, run.DefHash)
	}
	entry, err := s.defKV.Get(ctx, key)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{}, err
	}
	var def dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{}, err
	}
	return dag.EffectiveDef(def, run), run, nil
}

package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime/debug"
	"strconv"
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

// relayTimeout bounds one relay: one run load, one def load, one
// JetStream call.
const relayTimeout = 10 * time.Second

// relayedEventTypes are the step events a worker publishes. Every other
// history event type is the engine's own (spawn, cancel, approval, ...)
// and is never relayed.
var relayedEventTypes = map[protocol.EventType]bool{
	protocol.EventStepStarted:   true,
	protocol.EventStepContinue:  true,
	protocol.EventStepCompleted: true,
	protocol.EventStepFailed:    true,
}

// refusal is a typed relay refusal; code is one of natsutil.Refusal*.
type refusal struct{ code, detail string }

func (r *refusal) Error() string { return r.code + ": " + r.detail }

func refuse(code, detail string) error { return &refusal{code: code, detail: detail} }

// errNotYours is the one refusal for every ownership failure, so a
// program cannot tell another program's run from a run that does not
// exist, or a stale nonce from a foreign step.
var errNotYours = refuse(natsutil.RefusalNotYours, "no step of this program holds that dispatch")

// relayResult is what a relay replies: headers and, for a view, a body.
type relayResult struct {
	header nats.Header
	data   []byte
}

// relayOp serves one relay request for the program owning prefix.
type relayOp func(ctx context.Context, prefix string, req micro.Request) (relayResult, error)

// relayEndpoints are the publishes and reads a program may not make
// itself (#885). Each is its own subscription, so a flood on one does
// not queue the others.
func (n *NATSAPI) relayEndpoints() []endpointSpec {
	return []endpointSpec{
		{"program-events", "api.p.*.events",
			n.relayHandler(relayClassEvents, n.relayEvent), programQueueGroup},
		{"program-journal-append", "api.p.*.journal.append",
			n.relayHandler(relayClassOther, n.relayJournalAppend), programQueueGroup},
		{"program-journal-view", "api.p.*.journal.view",
			n.relayHandler(relayClassOther, n.relayJournalView), programQueueGroup},
	}
}

// relayHandler runs op off the subscription's goroutine, within the
// program's share of relayGate for class, so one program flooding the
// relay is refused busy while the others keep their slots, and its own
// step events keep theirs while its journal or stores are busy.
func (n *NATSAPI) relayHandler(class relayClass, op relayOp) micro.HandlerFunc {
	if op == nil {
		panic("relayHandler: op must not be nil")
	}
	return func(req micro.Request) {
		prefix, err := programPrefix(req.Subject())
		if err != nil {
			respondRelay(req, relayResult{}, refuse(natsutil.RefusalBadRequest, err.Error()))
			return
		}
		if !n.relayGate.acquire(prefix, class) {
			respondRelay(req, relayResult{}, refuse(natsutil.RefusalBusy, "relay at its bound"))
			return
		}
		go func() {
			defer n.relayGate.release(prefix, class)
			// No program request may take the node down, whatever the
			// bug: a panic becomes an "internal" refusal and a log line.
			defer recoverRelay(req)
			ctx, cancel := context.WithTimeout(
				observe.ExtractTraceContextHeader(nats.Header(req.Headers())), relayTimeout)
			defer cancel()
			result, err := op(ctx, prefix, req)
			respondRelay(req, result, err)
		}()
	}
}

// recoverRelay turns a panic in a relay op into an internal refusal.
func recoverRelay(req micro.Request) {
	recovered := recover()
	if recovered == nil {
		return
	}
	slog.Error("program relay op panicked; refused it",
		"subject", req.Subject(), "panic", recovered, "stack", string(debug.Stack()))
	respondRelay(req, relayResult{}, refuse(natsutil.RefusalInternal, "relay failed"))
}

func respondRelay(req micro.Request, result relayResult, err error) {
	header := result.header
	if header == nil {
		header = nats.Header{}
	}
	var r *refusal
	var apiErr *jetstream.APIError
	switch {
	case errors.As(err, &r):
		header.Set(natsutil.RelayRefusalHeader, r.code)
		header.Set(natsutil.RelayDetailHeader, r.detail)
	case errors.As(err, &apiErr):
		header.Set(natsutil.RelayAPICodeHeader, strconv.Itoa(apiErr.Code))
		header.Set(natsutil.RelayAPIErrHeader, strconv.Itoa(int(apiErr.ErrorCode)))
		header.Set(natsutil.RelayDetailHeader, apiErr.Description)
	case err != nil:
		header.Set(natsutil.RelayRefusalHeader, natsutil.RefusalBadRequest)
		header.Set(natsutil.RelayDetailHeader, err.Error())
	}
	if err := req.Respond(result.data, micro.WithHeaders(micro.Headers(header))); err != nil {
		return // the requester gave up; nothing to tell it
	}
}

// relaySubject is the subject a relay request is for.
func relaySubject(req micro.Request) string {
	return nats.Header(req.Headers()).Get(natsutil.RelaySubjectHeader)
}

// relayEvent admits a worker's step event: one of the four worker event
// types, for history.<its run>, with its own dedup id and dispatch
// identity (the relay never takes the legacy identity-less path), on a
// step whose task is the program's and whose current nonce it carries.
func (n *NATSAPI) relayEvent(
	ctx context.Context, prefix string, req micro.Request,
) (relayResult, error) {
	var evt protocol.Event
	if err := json.Unmarshal(req.Data(), &evt); err != nil {
		return relayResult{}, refuse(natsutil.RefusalBadRequest, "event: "+err.Error())
	}
	if !relayedEventTypes[evt.Type] {
		return relayResult{}, refuse(natsutil.RefusalEventType, string(evt.Type))
	}
	if evt.RunID == "" || evt.StepID == "" || relaySubject(req) != evt.NATSSubject() {
		return relayResult{}, refuse(natsutil.RefusalSubject, relaySubject(req))
	}
	identified := evt.Type == protocol.EventStepStarted || evt.HasIdentity()
	if !identified || evt.DispatchNonce == "" {
		return relayResult{}, refuse(natsutil.RefusalIdentity, string(evt.Type))
	}
	header := nats.Header(req.Headers())
	if header.Get(jetstream.MsgIDHeader) != evt.NATSMsgID() {
		return relayResult{}, refuse(natsutil.RefusalMsgID, "not the event's own dedup id")
	}
	owned, err := n.ownedStep(ctx, prefix, evt.RunID, byID(evt.StepID))
	if err != nil {
		return relayResult{}, err
	}
	if owned.state.DispatchNonce != evt.DispatchNonce {
		return relayResult{}, errNotYours
	}
	return n.relayPublish(ctx, evt.NATSSubject(), keepHeaders(header, noExtra), req.Data())
}

// relayJournalAppend admits a journal record on the program's own step
// with its current nonce. Its dedup id must be the one journal.MsgID
// gives that step's current attempt and iteration: JetStream dedup is
// stream-wide, so a record under another step's next id would make that
// step's real append come back Duplicate and be lost.
func (n *NATSAPI) relayJournalAppend(
	ctx context.Context, prefix string, req micro.Request,
) (relayResult, error) {
	header := nats.Header(req.Headers())
	owned, err := n.ownedJournal(ctx, prefix, relaySubject(req), header.Get(journal.HeaderNonce))
	if err != nil {
		return relayResult{}, err
	}
	if err := checkJournalMsgID(header, owned); err != nil {
		return relayResult{}, err
	}
	kept := keepHeaders(header, func(key string) bool {
		return key == jetstream.ExpectedLastSubjSeqHeader ||
			(strings.HasPrefix(key, "Dagnats-") && !strings.HasPrefix(key, "Dagnats-Relay-"))
	})
	return n.relayPublish(ctx, relaySubject(req), kept, req.Data())
}

// relayJournalView returns one record of the program's own step's
// journal: the last (from 0) or the first at or after a sequence.
func (n *NATSAPI) relayJournalView(
	ctx context.Context, prefix string, req micro.Request,
) (relayResult, error) {
	header := nats.Header(req.Headers())
	subject := relaySubject(req)
	owned, err := n.ownedJournal(ctx, prefix, subject, header.Get(journal.HeaderNonce))
	if err != nil {
		return relayResult{}, err
	}
	from, err := strconv.ParseUint(header.Get(natsutil.RelayFromSeqHeader), 10, 64)
	if err != nil {
		return relayResult{}, refuse(natsutil.RefusalBadRequest, "from sequence")
	}
	stream, err := n.journalStream(ctx)
	if err != nil {
		return relayResult{}, err
	}
	var raw *jetstream.RawStreamMsg
	if from == 0 {
		raw, err = stream.GetLastMsgForSubject(ctx, subject)
	} else {
		raw, err = stream.GetMsg(ctx, from, jetstream.WithGetMsgSubject(subject))
	}
	if errors.Is(err, jetstream.ErrMsgNotFound) && from > 0 {
		return n.archivedOnly(ctx, subject, from, owned.state)
	}
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return emptyView(), nil
	}
	if err != nil {
		return relayResult{}, err
	}
	if owned.state.Iterations < 0 {
		panic("relayJournalView: the step's iteration must not be negative")
	}
	// Open needs nothing before an earlier iteration's commit (the
	// protected floor), so the archive is not asked below one.
	if from > 0 && !journal.OpenNeedsNothingBefore(raw.Header, owned.state.Iterations) {
		return n.archivedOrLive(ctx, subject, from, raw)
	}
	return viewResult(raw.Sequence, raw.Header, raw.Data, raw.Time), nil
}

// archivedOrLive answers a view from sequence from: live, the first
// record JetStream holds at or after from, unless the record before it
// was purged and is at or after from too. Then the engine reads that
// one from the archive, as root and only after the ownership check: a
// program never asks the archive itself.
func (n *NATSAPI) archivedOrLive(
	ctx context.Context, subject string, from uint64, live *jetstream.RawStreamMsg,
) (relayResult, error) {
	if live == nil || from == 0 {
		panic("archivedOrLive: live and from must be set")
	}
	archived, err := journal.ArchivedBefore(ctx, n.svc.js, subject, from, live, 1)
	if err != nil {
		return relayResult{}, refuse(natsutil.RefusalArchive, err.Error())
	}
	if len(archived) > 1 {
		panic("archivedOrLive: the archive read returned more than its limit")
	}
	if len(archived) == 1 {
		m := archived[0]
		return viewResult(m.Seq, nats.Header(m.Headers), m.Data, m.Time), nil
	}
	return viewResult(live.Sequence, live.Header, live.Data, live.Time), nil
}

// archivedOnly answers a view from sequence from that JetStream holds
// nothing at or after: the archive's first record there, when the
// stream was purged past from. A step the snapshot says ran a later
// iteration starts from an earlier commit, so a view of it from the
// start that the archive cannot answer is refused, not empty.
func (n *NATSAPI) archivedOnly(
	ctx context.Context, subject string, from uint64, state dag.StepState,
) (relayResult, error) {
	if from == 0 || subject == "" {
		panic("archivedOnly: from and subject must be set")
	}
	archived, purged, err := journal.ArchivedFrom(ctx, n.svc.js, subject, from, 1)
	if err != nil {
		return relayResult{}, refuse(natsutil.RefusalArchive, err.Error())
	}
	if len(archived) > 1 {
		panic("archivedOnly: the archive read returned more than its limit")
	}
	if len(archived) == 1 {
		m := archived[0]
		return viewResult(m.Seq, nats.Header(m.Headers), m.Data, m.Time), nil
	}
	if purged && from == 1 && state.Iterations > 0 {
		return relayResult{}, refuse(natsutil.RefusalArchive, subject+
			" had history, but neither JetStream nor the archive holds a record of it")
	}
	return emptyView(), nil
}

// emptyView is the journal.view reply for no record.
func emptyView() relayResult {
	empty := nats.Header{}
	empty.Set(natsutil.RelayEmptyHeader, "true")
	return relayResult{header: empty}
}

// viewResult is a journal.view reply carrying one stored record.
func viewResult(seq uint64, header nats.Header, data []byte, at time.Time) relayResult {
	if seq == 0 {
		panic("viewResult: seq must be positive")
	}
	out := nats.Header{}
	for key, values := range header {
		out[key] = append([]string(nil), values...)
	}
	out.Set(natsutil.RelaySeqHeader, strconv.FormatUint(seq, 10))
	out.Set(natsutil.RelayTimeHeader, at.UTC().Format(time.RFC3339Nano))
	return relayResult{header: out, data: data}
}

// relayPublish publishes the admitted record and replies with its ack.
func (n *NATSAPI) relayPublish(
	ctx context.Context, subject string, header nats.Header, data []byte,
) (relayResult, error) {
	if n.svc == nil || n.svc.tp == nil {
		panic("relayPublish: svc must be set")
	}
	ack, err := n.svc.tp.JSPublishMsg(ctx, &nats.Msg{Subject: subject, Header: header, Data: data})
	if err != nil {
		return relayResult{}, err
	}
	out := nats.Header{}
	out.Set(natsutil.RelayStreamHeader, ack.Stream)
	out.Set(natsutil.RelaySeqHeader, strconv.FormatUint(ack.Sequence, 10))
	out.Set(natsutil.RelayDuplicateHeader, strconv.FormatBool(ack.Duplicate))
	return relayResult{header: out}, nil
}

func noExtra(string) bool { return false }

// keepHeaders copies the dedup id and trace context, plus any header
// extra admits. Every other header (expected stream, rollup, TTL) could
// change how the relayed record is stored, so it is dropped.
func keepHeaders(header nats.Header, extra func(string) bool) nats.Header {
	if extra == nil {
		panic("keepHeaders: extra must not be nil")
	}
	out := nats.Header{}
	for key, values := range header {
		switch {
		case key == jetstream.MsgIDHeader, strings.EqualFold(key, "traceparent"),
			strings.EqualFold(key, "tracestate"), extra(key):
			out[key] = append([]string(nil), values...)
		}
	}
	return out
}

// ownedStepState is a step a relay request has been checked against.
type ownedStepState struct {
	runID string
	step  dag.StepDef
	state dag.StepState
}

func byID(stepID string) func(dag.StepDef) bool {
	return func(step dag.StepDef) bool { return step.ID == stepID }
}

// ownedStep loads runID once and returns the one step match selects,
// when its task is under prefix. Every failure is errNotYours, except
// a match that selects two steps (lossy journal tokens).
func (n *NATSAPI) ownedStep(
	ctx context.Context, prefix, runID string, match func(dag.StepDef) bool,
) (ownedStepState, error) {
	if prefix == "" || match == nil {
		panic("ownedStep: prefix and match must be set")
	}
	def, run, err := n.svc.runAndDef(ctx, runID)
	if err != nil {
		return ownedStepState{}, errNotYours
	}
	var found *dag.StepDef
	for i := range def.Steps {
		if !match(def.Steps[i]) {
			continue
		}
		if found != nil {
			return ownedStepState{}, refuse(natsutil.RefusalSubject, "step token is ambiguous")
		}
		found = &def.Steps[i]
	}
	if found == nil || !underPrefix(found.Task, prefix) {
		return ownedStepState{}, errNotYours
	}
	state, ok := run.Steps[found.ID]
	if !ok {
		return ownedStepState{}, errNotYours
	}
	return ownedStepState{runID: runID, step: *found, state: state}, nil
}

// ownedJournal resolves journal.<run>.<step token> to the program's own
// step and checks nonce is its current dispatch.
func (n *NATSAPI) ownedJournal(
	ctx context.Context, prefix, subject, nonce string,
) (ownedStepState, error) {
	parts := strings.SplitN(subject, ".", 3)
	if len(parts) != 3 || parts[0] != "journal" || parts[1] == "" || parts[2] == "" {
		return ownedStepState{}, refuse(natsutil.RefusalSubject, subject)
	}
	if nonce == "" {
		return ownedStepState{}, refuse(natsutil.RefusalIdentity, "no dispatch nonce")
	}
	token := parts[2]
	owned, err := n.ownedStep(ctx, prefix, parts[1], func(step dag.StepDef) bool {
		return natsutil.SubjectToken(step.ID) == token
	})
	if err != nil {
		return ownedStepState{}, err
	}
	if owned.state.DispatchNonce != nonce {
		return ownedStepState{}, errNotYours
	}
	return owned, nil
}

// checkJournalMsgID requires the record's dedup id to be
// "<run>.<step>.<attempt>.<iteration>.<stream seq>.<delivered>.<index>"
// (journal.MsgID) for the owned, live step, with the attempt and
// iteration its record headers state and the step's dispatch holds.
func checkJournalMsgID(header nats.Header, owned ownedStepState) error {
	msgID := header.Get(jetstream.MsgIDHeader)
	head := owned.runID + "." + owned.step.ID + "."
	rest, ok := strings.CutPrefix(msgID, head)
	if !ok {
		return refuse(natsutil.RefusalMsgID, "dedup id is not this step's")
	}
	fields := strings.Split(rest, ".")
	if len(fields) != 5 {
		return refuse(natsutil.RefusalMsgID, "dedup id has the wrong shape")
	}
	numbers := make([]uint64, len(fields))
	for i, field := range fields {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return refuse(natsutil.RefusalMsgID, "dedup id field is not a number")
		}
		numbers[i] = value
	}
	attempt, iteration := strconv.FormatUint(numbers[0], 10), strconv.FormatUint(numbers[1], 10)
	if header.Get(journal.HeaderAttempt) != attempt ||
		header.Get(journal.HeaderIteration) != iteration {
		return refuse(natsutil.RefusalMsgID, "dedup id disagrees with the record's headers")
	}
	// As the engine fences step events: the iteration must be the
	// step's; the attempt only once the step is Running, because a
	// Queued step's Attempts still lags the worker's number until its
	// step.started is applied (step_event_fence.go).
	status := owned.state.Status
	if status != dag.StepStatusRunning && status != dag.StepStatusQueued {
		return errNotYours
	}
	if numbers[1] != uint64(owned.state.Iterations) {
		return refuse(natsutil.RefusalMsgID, "dedup id is not the current iteration's")
	}
	if status == dag.StepStatusRunning && numbers[0] != uint64(owned.state.Attempts) {
		return refuse(natsutil.RefusalMsgID, "dedup id is not the current attempt's")
	}
	return nil
}

// journalStream binds AGENT_JOURNAL once.
func (n *NATSAPI) journalStream(ctx context.Context) (jetstream.Stream, error) {
	n.journalMu.Lock()
	defer n.journalMu.Unlock()
	if n.journal != nil {
		return n.journal, nil
	}
	stream, err := n.svc.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		return nil, err
	}
	n.journal = stream
	return stream, nil
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

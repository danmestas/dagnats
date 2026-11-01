package api

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

// The stores a program reaches only through the engine (#885), each
// checked before it touches the bucket:
//
//   - checkpoint: its own step's checkpoint (key "<run>.<step>"), with
//     the step's current dispatch nonce;
//   - signal: its own signals, named "<name>.*": sent to a run it takes
//     part in, read only with its own current dispatch in that run;
//   - inputs: its own step's agent inputs, with the current nonce.
//
// The operation is the Dagnats-Relay-Op header.
func (n *NATSAPI) storeEndpoints() []endpointSpec {
	return []endpointSpec{
		{"program-checkpoint", "api.p.*.checkpoint",
			n.relayHandler(relayClassOther, n.relayCheckpoint), programQueueGroup},
		{"program-signal", "api.p.*.signal",
			n.relayHandler(relayClassSignals, n.relaySignal), programQueueGroup},
		{"program-inputs", "api.p.*.inputs",
			n.relayHandler(relayClassOther, n.relayInputs), programQueueGroup},
	}
}

// ownedDispatch resolves the request's run and step to the program's
// own step and checks the nonce header is its current dispatch.
func (n *NATSAPI) ownedDispatch(
	ctx context.Context, prefix string, header nats.Header,
) (ownedStepState, error) {
	runID, stepID := header.Get(natsutil.RelayRunHeader), header.Get(natsutil.RelayStepHeader)
	nonce := header.Get(journal.HeaderNonce)
	if runID == "" || stepID == "" || nonce == "" {
		return ownedStepState{}, refuse(natsutil.RefusalIdentity,
			"run, step and nonce are required")
	}
	owned, err := n.ownedStep(ctx, prefix, runID, byID(stepID))
	if err != nil {
		return ownedStepState{}, err
	}
	if owned.state.DispatchNonce != nonce {
		return ownedStepState{}, errNotYours
	}
	return owned, nil
}

func (n *NATSAPI) relayCheckpoint(
	ctx context.Context, prefix string, req micro.Request,
) (relayResult, error) {
	header := nats.Header(req.Headers())
	owned, err := n.ownedDispatch(ctx, prefix, header)
	if err != nil {
		return relayResult{}, err
	}
	kv, err := n.bucket(ctx, "checkpoints")
	if err != nil {
		return relayResult{}, err
	}
	key := owned.runID + "." + owned.step.ID
	switch header.Get(natsutil.RelayOpHeader) {
	case "get":
		return kvGetResult(ctx, kv, key)
	case "put":
		_, err := kv.Put(ctx, key, req.Data())
		return relayResult{}, err
	case "delete":
		err := kv.Delete(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			err = nil
		}
		return relayResult{}, err
	}
	return relayResult{}, refuse(natsutil.RefusalBadRequest, "checkpoint op")
}

// relaySignal sends or reads a signal. Signal names are namespaced by
// program: a program sends and reads only signals named "<name>.*", so
// one program in a composed run cannot read or forge another's (an
// "approved" a human sends through the API to another program's step).
// A send also needs a run the program takes part in; a read needs the
// caller's own current dispatch in that run (run, step, nonce), so it
// only ever reads its own run's signals.
func (n *NATSAPI) relaySignal(
	ctx context.Context, prefix string, req micro.Request,
) (relayResult, error) {
	header := nats.Header(req.Headers())
	runID, name := header.Get(natsutil.RelayRunHeader), header.Get(natsutil.RelayKeyHeader)
	if runID == "" || name == "" {
		return relayResult{}, refuse(natsutil.RefusalBadRequest, "run and signal name are required")
	}
	if !underPrefix(name, prefix) {
		return relayResult{}, refuse(natsutil.RefusalNotYours,
			"a program's signals are named under its prefix")
	}
	op := header.Get(natsutil.RelayOpHeader)
	switch op {
	case "put":
		if err := n.takesPart(ctx, prefix, runID); err != nil {
			return relayResult{}, err
		}
	case "get":
		if _, err := n.ownedDispatch(ctx, prefix, header); err != nil {
			return relayResult{}, err
		}
	default:
		return relayResult{}, refuse(natsutil.RefusalBadRequest, "signal op")
	}
	kv, err := n.bucket(ctx, "signals")
	if err != nil {
		return relayResult{}, err
	}
	key := runID + "." + name
	if op == "get" {
		return kvGetResult(ctx, kv, key)
	}
	_, err = kv.Put(ctx, key, req.Data())
	return relayResult{}, err
}

// takesPart reports nil when a step of runID has a task under prefix.
func (n *NATSAPI) takesPart(ctx context.Context, prefix, runID string) error {
	if prefix == "" || runID == "" {
		panic("takesPart: prefix and runID must be set")
	}
	def, _, err := n.svc.runAndDef(ctx, runID)
	if err != nil {
		return errNotYours
	}
	for _, step := range def.Steps {
		if underPrefix(step.Task, prefix) {
			return nil
		}
	}
	return errNotYours
}

// inputsRequest is an inputs operation's body: the submission an entry
// holds, its revision (Entry.Revision does not encode), and the target
// status and iteration for a transition or claim.
type inputsRequest struct {
	Entry     inputs.Submission `json:"entry"`
	Revision  uint64            `json:"revision"`
	RequestID string            `json:"request_id,omitempty"`
	Status    inputs.Status     `json:"status,omitempty"`
	Iteration int               `json:"iteration"`
}

// inputsReply is one entry or a list, each with its revision.
type inputsReply struct {
	Entries []inputsRequest `json:"entries"`
}

func (n *NATSAPI) relayInputs(
	ctx context.Context, prefix string, req micro.Request,
) (relayResult, error) {
	header := nats.Header(req.Headers())
	owned, err := n.ownedDispatch(ctx, prefix, header)
	if err != nil {
		return relayResult{}, err
	}
	var body inputsRequest
	if len(req.Data()) > 0 {
		if err := json.Unmarshal(req.Data(), &body); err != nil {
			return relayResult{}, refuse(natsutil.RefusalBadRequest, "inputs: "+err.Error())
		}
	}
	store, err := n.svc.inputsStore(ctx)
	if err != nil {
		return relayResult{}, err
	}
	entries, err := n.inputsOp(ctx, store, owned, header.Get(natsutil.RelayOpHeader), body)
	if err != nil {
		return relayResult{}, err
	}
	data, err := json.Marshal(inputsReply{Entries: entries})
	if err != nil {
		return relayResult{}, err
	}
	return relayResult{data: data}, nil
}

// inputsOp runs one inputs operation on the owned step. Every value
// that reaches an inputs.Store assertion is checked here first: the
// request id's shape, a positive revision, a known status. A transition
// or claim applies to the stored submission, never a body the program
// sends; only the revision it read (the compare-and-set) comes from the
// request, so a stale one fails as a conflict.
func (n *NATSAPI) inputsOp(
	ctx context.Context, store *inputs.Store, owned ownedStepState, op string, body inputsRequest,
) ([]inputsRequest, error) {
	if store == nil {
		panic("inputsOp: store must be open")
	}
	runID, stepID := owned.runID, owned.step.ID
	if op == "list" {
		entries, err := store.List(ctx, runID, stepID)
		return entriesReply(entries), inputsError(err)
	}
	requestID := body.RequestID
	if op == "transition" || op == "claim" {
		requestID = body.Entry.RequestID
		if body.Entry.RunID != runID || body.Entry.StepID != stepID {
			return nil, errNotYours
		}
		if body.Revision == 0 || body.Iteration < 0 {
			return nil, refuse(natsutil.RefusalBadRequest, "revision and iteration are required")
		}
	}
	if err := inputs.ValidateRequestID(requestID); err != nil {
		return nil, refuse(natsutil.RefusalBadRequest, err.Error())
	}
	stored, err := store.Get(ctx, runID, stepID, requestID)
	if err != nil {
		return nil, inputsError(err)
	}
	switch op {
	case "get":
		return entriesReply([]inputs.Entry{stored}), nil
	case "transition":
		if !knownStatus(body.Status) {
			return nil, refuse(natsutil.RefusalBadRequest, "unknown status")
		}
		stored.Revision = body.Revision
		next, err := store.Transition(ctx, stored, body.Status, body.Iteration)
		return entriesReply([]inputs.Entry{next}), inputsError(err)
	case "claim":
		stored.Revision = body.Revision
		next, err := store.Claim(ctx, stored, body.Iteration)
		return entriesReply([]inputs.Entry{next}), inputsError(err)
	}
	return nil, refuse(natsutil.RefusalBadRequest, "inputs op")
}

func knownStatus(status inputs.Status) bool {
	switch status {
	case inputs.StatusQueued, inputs.StatusPlaced, inputs.StatusDone, inputs.StatusUnanswered:
		return true
	}
	return false
}

func entriesReply(entries []inputs.Entry) []inputsRequest {
	out := make([]inputsRequest, len(entries))
	for i, e := range entries {
		out[i] = inputsRequest{Entry: e.Submission, Revision: e.Revision}
	}
	return out
}

// inputsError carries the store's typed outcomes as refusal codes.
func inputsError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, inputs.ErrConflict):
		return refuse(natsutil.RefusalInputsConflict, err.Error())
	case errors.Is(err, inputs.ErrNotFound):
		return refuse(natsutil.RefusalInputsNotFound, err.Error())
	case errors.Is(err, inputs.ErrNotQueued):
		return refuse(natsutil.RefusalInputsNotQueued, err.Error())
	}
	return err
}

// kvGetResult replies with a key's value and revision, or empty.
func kvGetResult(ctx context.Context, kv jetstream.KeyValue, key string) (relayResult, error) {
	entry, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		empty := nats.Header{}
		empty.Set(natsutil.RelayEmptyHeader, "true")
		return relayResult{header: empty}, nil
	}
	if err != nil {
		return relayResult{}, err
	}
	out := nats.Header{}
	out.Set(natsutil.RelayRevisionHeader, strconv.FormatUint(entry.Revision(), 10))
	return relayResult{header: out, data: entry.Value()}, nil
}

// bucket binds an engine bucket by name, once.
func (n *NATSAPI) bucket(ctx context.Context, name string) (jetstream.KeyValue, error) {
	n.journalMu.Lock()
	defer n.journalMu.Unlock()
	if kv, ok := n.buckets[name]; ok {
		return kv, nil
	}
	kv, err := n.svc.js.KeyValue(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.buckets == nil {
		n.buckets = make(map[string]jetstream.KeyValue)
	}
	n.buckets[name] = kv
	return kv, nil
}

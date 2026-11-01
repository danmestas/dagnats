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
//   - signal: signals ("<run>.<name>") to and from runs it takes part
//     in, meaning a step of the run has a task under its prefix;
//   - inputs: its own step's agent inputs, with the current nonce.
//
// The operation is the Dagnats-Relay-Op header.
func (n *NATSAPI) storeEndpoints() []endpointSpec {
	return []endpointSpec{
		{"program-checkpoint", "api.p.*.checkpoint",
			n.relayHandler(relayClassOther, n.relayCheckpoint), programQueueGroup},
		{"program-signal", "api.p.*.signal",
			n.relayHandler(relayClassOther, n.relaySignal), programQueueGroup},
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

// relaySignal sends or reads a signal on a run the program takes part
// in. Signals cross runs by design (a step wakes another run), so there
// is no nonce, but the run must have a step that is the program's.
func (n *NATSAPI) relaySignal(
	ctx context.Context, prefix string, req micro.Request,
) (relayResult, error) {
	header := nats.Header(req.Headers())
	runID, name := header.Get(natsutil.RelayRunHeader), header.Get(natsutil.RelayKeyHeader)
	if runID == "" || name == "" {
		return relayResult{}, refuse(natsutil.RefusalBadRequest, "run and signal name are required")
	}
	if err := n.takesPart(ctx, prefix, runID); err != nil {
		return relayResult{}, err
	}
	kv, err := n.bucket(ctx, "signals")
	if err != nil {
		return relayResult{}, err
	}
	key := runID + "." + name
	switch header.Get(natsutil.RelayOpHeader) {
	case "get":
		return kvGetResult(ctx, kv, key)
	case "put":
		_, err := kv.Put(ctx, key, req.Data())
		return relayResult{}, err
	}
	return relayResult{}, refuse(natsutil.RefusalBadRequest, "signal op")
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

// inputsOp runs one inputs operation on the owned step. An entry passed
// in must be for that step, so a transition or claim cannot reach
// another step's submission.
func (n *NATSAPI) inputsOp(
	ctx context.Context, store *inputs.Store, owned ownedStepState, op string, body inputsRequest,
) ([]inputsRequest, error) {
	if store == nil {
		panic("inputsOp: store must be open")
	}
	runID, stepID := owned.runID, owned.step.ID
	entry := inputs.Entry{Submission: body.Entry, Revision: body.Revision}
	if (op == "transition" || op == "claim") &&
		(entry.RunID != runID || entry.StepID != stepID) {
		return nil, errNotYours
	}
	var result []inputs.Entry
	var err error
	switch op {
	case "get":
		var one inputs.Entry
		one, err = store.Get(ctx, runID, stepID, body.RequestID)
		result = []inputs.Entry{one}
	case "list":
		result, err = store.List(ctx, runID, stepID)
	case "transition":
		var one inputs.Entry
		one, err = store.Transition(ctx, entry, body.Status, body.Iteration)
		result = []inputs.Entry{one}
	case "claim":
		var one inputs.Entry
		one, err = store.Claim(ctx, entry, body.Iteration)
		result = []inputs.Entry{one}
	default:
		return nil, refuse(natsutil.RefusalBadRequest, "inputs op")
	}
	if err != nil {
		return nil, err
	}
	out := make([]inputsRequest, len(result))
	for i, e := range result {
		out[i] = inputsRequest{Entry: e.Submission, Revision: e.Revision}
	}
	return out, nil
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

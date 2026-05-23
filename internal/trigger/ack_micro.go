// trigger/ack_micro.go
// `_REGISTRY.trigger_types.ack` micro endpoint (#327). Workers that
// have already Put a TriggerTypeDef into the trigger_types KV bucket
// call this subject to ask the engine to (a) validate the registration
// and (b) allocate an externalRegistrar so subsequent External trigger
// defs in the `triggers` bucket get bridged to the owner worker.
//
// Audit-adjusted request shape (#327, see also issue comment
// 4525546797): `{Name, OwnerWorkerID}`. No ConfigSchemaSHA — the engine
// reads the schema bytes from KV directly. KV is the source of truth.
//
// Idempotency contract:
//   - Same Name + same OwnerWorkerID + same schema → success, no
//     re-allocation.
//   - Same Name + different OwnerWorkerID → error (a kind cannot be
//     owned by two workers simultaneously).
//   - Same Name + same OwnerWorkerID + different schema bytes → error
//     (callers must update the KV record consistently across restarts).
//
// Reply payload: empty bytes on success, JSON `{"error":"..."}` on
// failure. Mirrors externalRegistrar.requestOwner so workers can share
// one decode path. 5s natural timeout on the request side; the handler
// is expected to finish in well under a second.
package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ackSubject is the NATS subject the engine listens on for worker
// RegisterTriggerType acknowledgements.
const ackSubject = "_REGISTRY.trigger_types.ack"

// RegisterTriggerTypeRequest is the wire shape workers send. Exported
// so the worker SDK in Phase 2.4 can share the type.
type RegisterTriggerTypeRequest struct {
	Name          string `json:"name"`
	OwnerWorkerID string `json:"owner_worker_id"`
}

// ackResponse is the engine's reply on the ack subject. Empty Error
// signals success.
type ackResponse struct {
	Error string `json:"error,omitempty"`
}

// startAckMicro subscribes to `_REGISTRY.trigger_types.ack` and binds
// the handler to ts. Stores the subscription on ts.ackSub so Stop can
// drain it. Returns the subscribe error verbatim.
func (ts *TriggerService) startAckMicro() error {
	if ts.nc == nil {
		panic("startAckMicro: nc must not be nil")
	}
	sub, err := ts.nc.Subscribe(ackSubject, ts.handleAck)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", ackSubject, err)
	}
	ts.ackSub = sub
	return nil
}

// handleAck decodes the request, validates the trigger_types KV record,
// and allocates an externalRegistrar in ts.registrars under the
// "external::<name>" key. Reply is empty bytes on success or a JSON
// error envelope on failure. Never panics on bad wire input — workers
// must surface boot failures cleanly.
func (ts *TriggerService) handleAck(msg *nats.Msg) {
	if msg == nil {
		panic("handleAck: msg must not be nil")
	}
	if ts.triggerTypesKV == nil {
		ts.replyAck(msg, fmt.Errorf("trigger_types KV not initialised"))
		return
	}

	var req RegisterTriggerTypeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		ts.replyAck(msg, fmt.Errorf("decode request: %w", err))
		return
	}
	if req.Name == "" {
		ts.replyAck(msg, fmt.Errorf("name must not be empty"))
		return
	}
	if req.OwnerWorkerID == "" {
		ts.replyAck(msg, fmt.Errorf("owner_worker_id must not be empty"))
		return
	}

	tdef, err := ts.loadTriggerType(req.Name)
	if err != nil {
		ts.replyAck(msg, err)
		return
	}
	if tdef.OwnerWorkerID != req.OwnerWorkerID {
		ts.replyAck(msg, fmt.Errorf(
			"name %q owner mismatch: KV=%q request=%q",
			req.Name, tdef.OwnerWorkerID, req.OwnerWorkerID))
		return
	}
	if err := ts.installExternalRegistrar(req.Name, tdef); err != nil {
		ts.replyAck(msg, err)
		return
	}
	ts.replyAck(msg, nil)
}

// loadTriggerType reads the TriggerTypeDef from the trigger_types KV
// bucket and verifies its ConfigSchema parses as JSON. Returns a
// canonical "not registered" error when the key is missing.
func (ts *TriggerService) loadTriggerType(
	name string,
) (TriggerTypeDef, error) {
	if name == "" {
		panic("loadTriggerType: name must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	entry, err := ts.triggerTypesKV.Get(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return TriggerTypeDef{}, fmt.Errorf(
				"trigger type %q not registered in KV", name)
		}
		return TriggerTypeDef{}, fmt.Errorf(
			"trigger_types KV get %q: %w", name, err)
	}
	var tdef TriggerTypeDef
	if err := json.Unmarshal(entry.Value(), &tdef); err != nil {
		return TriggerTypeDef{}, fmt.Errorf(
			"decode trigger_types[%q]: %w", name, err)
	}
	if len(tdef.ConfigSchema) == 0 {
		return TriggerTypeDef{}, fmt.Errorf(
			"trigger type %q has empty config_schema", name)
	}
	// Parse the schema to surface malformed JSON early. The expensive
	// santhosh-tekuri compile happens lazily at first config validate
	// (validate_external.go) — the cheap json.Unmarshal here is enough
	// to reject obvious garbage at registration time.
	var probe any
	if err := json.Unmarshal(tdef.ConfigSchema, &probe); err != nil {
		return TriggerTypeDef{}, fmt.Errorf(
			"config_schema for %q is not valid JSON: %w", name, err)
	}
	return tdef, nil
}

// installExternalRegistrar idempotently registers an externalRegistrar
// under the "external::<name>" key. Conflicts on owner or schema bytes
// fail loudly so workers can detect mismatched state. A successful
// first registration also fires Activate for any pre-existing matching
// entries in the `triggers` KV bucket so worker restarts re-bind
// in-flight triggers.
func (ts *TriggerService) installExternalRegistrar(
	name string, tdef TriggerTypeDef,
) error {
	if name == "" {
		panic("installExternalRegistrar: name must not be empty")
	}
	if ts.triggerKV == nil {
		panic("installExternalRegistrar: triggerKV must not be nil")
	}
	key := externalKindPrefix + name

	ts.mu.Lock()
	existing, ok := ts.registrars[key]
	if ok {
		// Idempotency: same kind already wired. Verify the bound
		// owner matches; otherwise the worker fleet is in conflict
		// and the operator needs to intervene.
		existingExt, isExt := existing.(*externalRegistrar)
		if !isExt {
			ts.mu.Unlock()
			return fmt.Errorf(
				"registrar %q exists but is not external", key)
		}
		if existingExt.ownerWorkerID != tdef.OwnerWorkerID {
			ts.mu.Unlock()
			return fmt.Errorf(
				"name %q already owned by %q (request from %q)",
				name, existingExt.ownerWorkerID, tdef.OwnerWorkerID)
		}
		// Schema bytes must also match the original registration —
		// workers re-registering with a different schema must drain
		// in-flight triggers and unregister/re-register cleanly.
		if !bytes.Equal(existingExt.configSchema, tdef.ConfigSchema) {
			ts.mu.Unlock()
			return fmt.Errorf(
				"name %q schema mismatch on re-register", name)
		}
		ts.mu.Unlock()
		return nil
	}

	reg := newExternalRegistrar(
		ts.nc, ts.triggerKV, name, tdef.OwnerWorkerID, tdef.ConfigSchema,
	)
	ts.registrars[key] = reg
	ts.mu.Unlock()

	// fireExistingEntries lives outside the registrars-map mutex so
	// the recursive Activate path (which itself takes registrar.mu)
	// does not deadlock.
	reg.fireExistingEntries(ts.ctx)
	return nil
}

// replyAck encodes the ackResponse and publishes it on msg.Reply.
// Logs respond failures — there is nothing actionable for the engine
// to do when the reply path is gone.
func (ts *TriggerService) replyAck(msg *nats.Msg, err error) {
	if msg == nil {
		panic("replyAck: msg must not be nil")
	}
	if msg.Reply == "" {
		if err != nil {
			slog.Warn("ack with no reply subject",
				"error", err)
		}
		return
	}
	if err == nil {
		if rerr := msg.Respond(nil); rerr != nil {
			slog.Warn("ack respond", "error", rerr)
		}
		return
	}
	body, _ := json.Marshal(ackResponse{Error: err.Error()})
	if rerr := msg.Respond(body); rerr != nil {
		slog.Warn("ack respond error", "error", rerr)
	}
}

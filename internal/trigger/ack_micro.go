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

// liveTriggersScanMax bounds the KV scan that decides whether a
// version-bump RegisterTriggerType is safe (#351). The scan returns
// early on the first matching live trigger, so the bound only fires
// when the bucket holds many irrelevant entries of other kinds.
const liveTriggersScanMax = 10000

// liveTriggersScanCounter is a test-only seam: when non-nil it is
// incremented once per entry inspected by hasLiveTriggersOfKind. Tests
// install one to prove the early-return contract.
var liveTriggersScanCounter func()

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
//
// Version semantics (#351, Phase 2.7):
//   - same Owner + same Schema + same Version → idempotent no-op.
//   - same Owner + same Schema + different Version + zero live
//     triggers of that kind → registrar is replaced (overwrite ok).
//   - same Owner + same Schema + different Version + ≥1 live trigger
//     → hard error; operator must drain or migrate first.
//
// The KV scan is bounded (liveTriggersScanMax) and returns on the first
// match, and it runs OUTSIDE ts.mu — releasing the lock before KV I/O
// matches the #330 lock-ordering rule so KV latency can't pin the
// registrars map.
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
		existingExt, isExt := existing.(*externalRegistrar)
		ts.mu.Unlock()
		if !isExt {
			return fmt.Errorf(
				"registrar %q exists but is not external", key)
		}
		return ts.reconcileExternalRegistrar(key, existingExt, tdef)
	}

	reg := newExternalRegistrar(
		ts.nc, ts.triggerKV, name,
		tdef.OwnerWorkerID, tdef.ConfigSchema, tdef.Version,
	)
	ts.registrars[key] = reg
	ts.mu.Unlock()

	// fireExistingEntries lives outside the registrars-map mutex so
	// the recursive Activate path (which itself takes registrar.mu)
	// does not deadlock.
	reg.fireExistingEntries(ts.ctx)
	return nil
}

// reconcileExternalRegistrar handles re-registration of a kind whose
// registrar already exists. Splits owner/schema conflict detection,
// version short-circuit, and the bounded live-trigger scan into one
// helper so installExternalRegistrar stays under the 70-line cap.
//
// Caller MUST NOT hold ts.mu — this function manages its own locking
// and drops the lock around KV I/O (#330 lock-ordering rule).
func (ts *TriggerService) reconcileExternalRegistrar(
	key string, existing *externalRegistrar, tdef TriggerTypeDef,
) error {
	name := existing.kind
	if existing.ownerWorkerID != tdef.OwnerWorkerID {
		return fmt.Errorf(
			"name %q already owned by %q (request from %q)",
			name, existing.ownerWorkerID, tdef.OwnerWorkerID)
	}
	if !bytes.Equal(existing.configSchema, tdef.ConfigSchema) {
		return fmt.Errorf(
			"name %q schema mismatch on re-register", name)
	}
	if existing.version == tdef.Version {
		// Short-circuit: identical version → no scan needed.
		return nil
	}

	scanCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	hasLive, err := ts.hasLiveTriggersOfKind(
		scanCtx, name, liveTriggersScanMax,
	)
	cancel()
	if err != nil {
		return fmt.Errorf("scan live triggers for %q: %w", name, err)
	}
	if hasLive {
		return fmt.Errorf(
			"name %q version mismatch (existing=%q new=%q): "+
				"live triggers of this kind exist; drain or "+
				"migrate before re-registering",
			name, existing.version, tdef.Version)
	}
	// No live triggers — replace the registrar with the new version.
	ts.mu.Lock()
	reg := newExternalRegistrar(
		ts.nc, ts.triggerKV, name,
		tdef.OwnerWorkerID, tdef.ConfigSchema, tdef.Version,
	)
	ts.registrars[key] = reg
	ts.mu.Unlock()
	reg.fireExistingEntries(ts.ctx)
	return nil
}

// hasLiveTriggersOfKind streams the triggers KV bucket and returns true
// on the first entry whose External.Kind equals kind AND Disabled is
// false. Scan stops on first match (early return) or after scanMax
// entries have been inspected, whichever comes first. The bound caps
// worst-case latency when no entry matches.
//
// Disabled is read from the negation of TriggerDef.Enabled — the trigger
// KV records the *enabled* flag, and "live" means enabled here.
//
// Test seam: when liveTriggersScanCounter is non-nil it is called once
// per entry actually inspected (not per key listed) so tests can prove
// the early-return semantics without a timing race.
func (ts *TriggerService) hasLiveTriggersOfKind(
	ctx context.Context, kind string, scanMax int,
) (bool, error) {
	if ctx == nil {
		panic("hasLiveTriggersOfKind: ctx must not be nil")
	}
	if kind == "" {
		panic("hasLiveTriggersOfKind: kind must not be empty")
	}
	if scanMax <= 0 {
		panic("hasLiveTriggersOfKind: scanMax must be positive")
	}
	if ts.triggerKV == nil {
		panic("hasLiveTriggersOfKind: triggerKV must not be nil")
	}

	lister, err := ts.triggerKV.ListKeys(ctx)
	if err != nil {
		return false, fmt.Errorf("list trigger keys: %w", err)
	}
	defer func() { _ = lister.Stop() }()

	inspected := 0
	for key := range lister.Keys() {
		if inspected >= scanMax {
			return false, nil
		}
		entry, err := ts.triggerKV.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return false, fmt.Errorf("get %q: %w", key, err)
		}
		inspected++
		if liveTriggersScanCounter != nil {
			liveTriggersScanCounter()
		}
		var def TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			// Malformed entries do not block a version bump; skip.
			continue
		}
		if def.External == nil {
			continue
		}
		if def.External.Kind != kind {
			continue
		}
		if !def.Enabled {
			continue
		}
		return true, nil
	}
	return false, nil
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

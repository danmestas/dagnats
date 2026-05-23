// trigger/registrar_external.go
// ExternalRegistrar (parent #273 Phase 2.3, #327). Owns the lifecycle
// of one externally-defined trigger kind. Bridges activate/deactivate
// events to the owning worker via `_TRIGGER.<kind>.{activate,deactivate}`
// NATS request/reply. Created on demand by the _REGISTRY.trigger_types.ack
// micro endpoint when a worker calls RegisterTriggerType.
//
// One externalRegistrar instance per kind. Registered in the
// TriggerService.registrars map under the key "external::<kind>"; the
// "external::" prefix keeps the dispatch keyspace disjoint from the
// fixed-kind constants (kindCron / kindSubject / kindWebhook / kindHTTP).
//
// The owning worker is the authority for both the trigger's behavior
// and (per #325) its config schema. The engine's role is restricted to
// (a) holding the KV record, (b) signalling the worker on enable/disable,
// (c) firing the matching workflow when the worker reports a trigger
// event (Phase 2.4 — out of scope here).
//
// Activate request shape (JSON):
//
//	{
//	    "trigger_id":   "<def.ID>",
//	    "workflow_id":  "<def.WorkflowID>",
//	    "config":        <opaque worker config object>
//	}
//
// Owner worker is expected to reply with an empty body on success or a
// payload containing {"error": "..."} on failure. Any reply is currently
// accepted as success unless transport timed out — the worker SDK
// (Phase 2.4) will tighten this contract.
package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// externalKindPrefix namespaces registrar map keys for externally-
// defined trigger kinds so they cannot collide with the built-in
// kindCron / kindSubject / kindWebhook / kindHTTP constants.
const externalKindPrefix = "external::"

// externalRequestTimeout bounds the activate/deactivate request/reply
// to the owner worker. Owner workers are expected to respond quickly;
// a hung worker should surface as a registrar error, not a deadlock.
const externalRequestTimeout = 5 * time.Second

// externalActivatePayload is the JSON shape sent on
// `_TRIGGER.<kind>.activate`. Workers receive the trigger ID, target
// workflow, and the opaque config blob they validated at registration
// time. Kept minimal — anything richer is recoverable from the
// `triggers` KV bucket on the worker side.
type externalActivatePayload struct {
	TriggerID  string          `json:"trigger_id"`
	WorkflowID string          `json:"workflow_id"`
	Config     json.RawMessage `json:"config"`
}

// externalRegistrar implements TriggerRegistrar for one external
// trigger kind. Per-instance state is the active set keyed by trigger
// ID for idempotency (ADR-016). The owner-worker identity is stored
// for observability — replies are addressed by subject, not by worker
// identity.
type externalRegistrar struct {
	nc            *nats.Conn
	triggerKV     jetstream.KeyValue
	kind          string
	ownerWorkerID string
	// configSchema is the bound schema bytes at registration time.
	// Used to detect mismatched re-registrations (issue #327
	// ConflictingSchema contract). Stored verbatim — the engine never
	// recompiles it; santhosh-tekuri does that on demand inside
	// validate_external.go.
	configSchema json.RawMessage
	// version is the bound TriggerTypeDef.Version at registration time
	// (#351). A re-register with a different Version is allowed only
	// when no live triggers of this kind exist — otherwise existing
	// in-flight triggers could observe a schema/payload break.
	version string

	mu     sync.Mutex
	active map[string]TriggerDef
}

// newExternalRegistrar constructs an ExternalRegistrar for kind owned
// by ownerWorkerID. Panics on the usual programmer-error inputs.
// version is the bound TriggerTypeDef.Version (#351); empty is tolerated
// because not every test seed populates the field.
func newExternalRegistrar(
	nc *nats.Conn,
	triggerKV jetstream.KeyValue,
	kind string,
	ownerWorkerID string,
	configSchema json.RawMessage,
	version string,
) *externalRegistrar {
	if nc == nil {
		panic("newExternalRegistrar: nc must not be nil")
	}
	if triggerKV == nil {
		panic("newExternalRegistrar: triggerKV must not be nil")
	}
	if kind == "" {
		panic("newExternalRegistrar: kind must not be empty")
	}
	if ownerWorkerID == "" {
		panic("newExternalRegistrar: ownerWorkerID must not be empty")
	}
	if len(configSchema) == 0 {
		panic("newExternalRegistrar: configSchema must not be empty")
	}
	return &externalRegistrar{
		nc:            nc,
		triggerKV:     triggerKV,
		kind:          kind,
		ownerWorkerID: ownerWorkerID,
		configSchema:  configSchema,
		version:       version,
		active:        make(map[string]TriggerDef),
	}
}

// Activate sends an activate request to the owner worker via
// `_TRIGGER.<kind>.activate`. Idempotent per ADR-016: re-activating a
// def already known to this registrar is a no-op returning nil.
func (r *externalRegistrar) Activate(
	ctx context.Context, def TriggerDef,
) error {
	if def.ID == "" {
		panic("externalRegistrar.Activate: def.ID must not be empty")
	}
	if def.External == nil {
		return fmt.Errorf(
			"trigger %q: external config missing", def.ID)
	}
	if def.External.Kind != r.kind {
		return fmt.Errorf(
			"trigger %q: kind %q does not match registrar %q",
			def.ID, def.External.Kind, r.kind)
	}

	r.mu.Lock()
	if _, exists := r.active[def.ID]; exists {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	if err := r.requestOwner(ctx, "activate", def); err != nil {
		return err
	}
	r.mu.Lock()
	r.active[def.ID] = def
	r.mu.Unlock()
	return nil
}

// Deactivate sends a deactivate request to the owner worker. Idempotent:
// removing an unknown ID is a no-op returning nil so the watcher's delete
// fan-out does not error against unrelated registrars.
func (r *externalRegistrar) Deactivate(
	ctx context.Context, def TriggerDef,
) error {
	if def.ID == "" {
		panic("externalRegistrar.Deactivate: def.ID must not be empty")
	}
	r.mu.Lock()
	stored, exists := r.active[def.ID]
	if !exists {
		r.mu.Unlock()
		return nil
	}
	delete(r.active, def.ID)
	r.mu.Unlock()
	// Use the stored def so deactivate carries the kind/config the
	// owner originally accepted, even when the caller passed a stub.
	if err := r.requestOwner(ctx, "deactivate", stored); err != nil {
		// Re-arm the active map so a retry can repair the state.
		r.mu.Lock()
		r.active[def.ID] = stored
		r.mu.Unlock()
		return err
	}
	return nil
}

// ValidateConfig performs the in-package structural checks. The
// schema-driven config validation happens in trigger.ValidateWithKV
// (validate_external.go); ValidateConfig only proves the External
// branch is present and the kind matches this registrar.
func (r *externalRegistrar) ValidateConfig(def TriggerDef) error {
	if def.External == nil {
		return fmt.Errorf(
			"trigger %q: external config missing", def.ID)
	}
	if def.External.Kind == "" {
		return fmt.Errorf(
			"trigger %q: external.kind must not be empty", def.ID)
	}
	if def.External.Kind != r.kind {
		return fmt.Errorf(
			"trigger %q: kind %q does not match registrar %q",
			def.ID, def.External.Kind, r.kind)
	}
	return nil
}

// requestOwner addresses `_TRIGGER.<kind>.<action>` and waits up to
// externalRequestTimeout. Reply payload is currently advisory — any
// reply is accepted as success. Phase 2.4 may upgrade this to parse a
// structured error response.
func (r *externalRegistrar) requestOwner(
	ctx context.Context, action string, def TriggerDef,
) error {
	if action == "" {
		panic("requestOwner: action must not be empty")
	}
	if def.ID == "" {
		panic("requestOwner: def.ID must not be empty")
	}
	payload := externalActivatePayload{
		TriggerID:  def.ID,
		WorkflowID: def.WorkflowID,
	}
	if def.External != nil {
		payload.Config = def.External.Config
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal activate payload: %w", err)
	}
	subj := "_TRIGGER." + r.kind + "." + action

	reqCtx, cancel := context.WithTimeout(ctx, externalRequestTimeout)
	defer cancel()
	msg, err := r.nc.RequestWithContext(reqCtx, subj, data)
	if err != nil {
		return fmt.Errorf("request %s: %w", subj, err)
	}
	// Owner may signal error via a JSON {"error":"..."} body. Empty
	// body = success.
	if len(msg.Data) == 0 {
		return nil
	}
	var resp struct {
		Error string `json:"error,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		// Non-JSON replies are tolerated as success — the contract is
		// fire-and-acknowledge, not RPC. Phase 2.4 may tighten this.
		return nil
	}
	if resp.Error != "" {
		return fmt.Errorf("owner worker error: %s", resp.Error)
	}
	return nil
}

// fireExistingEntries scans the triggers KV bucket and Activates every
// enabled External entry whose Kind matches this registrar. Called once
// at registrar construction so a worker restart re-binds in-flight
// triggers without operator intervention. Errors are logged and skipped
// so one bad entry does not poison the whole scan.
func (r *externalRegistrar) fireExistingEntries(ctx context.Context) {
	if ctx == nil {
		panic("fireExistingEntries: ctx must not be nil")
	}
	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	keys, err := r.triggerKV.Keys(listCtx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return
		}
		slog.Error("externalRegistrar.fireExistingEntries: list keys",
			"kind", r.kind, "error", err)
		return
	}
	const maxScan = maxActiveTriggers
	if len(keys) > maxScan {
		keys = keys[:maxScan]
	}
	for _, key := range keys {
		entry, err := r.triggerKV.Get(listCtx, key)
		if err != nil {
			continue
		}
		var def TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}
		if def.External == nil || def.External.Kind != r.kind {
			continue
		}
		if !def.Enabled {
			continue
		}
		if err := r.Activate(ctx, def); err != nil {
			slog.Warn("externalRegistrar.fireExistingEntries: activate",
				"kind", r.kind, "trigger_id", def.ID, "error", err)
		}
	}
}

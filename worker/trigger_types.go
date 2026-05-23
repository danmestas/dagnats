// worker/trigger_types.go
// Worker-side complement to the ExternalRegistrar ack micro endpoint
// (#327). RegisterTriggerType is the SDK call workers make once on
// boot to (a) publish their TriggerTypeDef into the `trigger_types`
// KV bucket and (b) ask the engine to allocate an externalRegistrar
// so subsequent `_TRIGGER.<kind>.{activate,deactivate}` requests get
// bridged to this worker.
//
// KV-then-ack ordering is load-bearing — the engine's handleAck
// reads the schema bytes straight from KV (audit-adjusted contract:
// "KV is the source of truth"). A worker that calls ack before its
// Put has landed will see a "trigger type %q not registered in KV"
// error and must retry.
//
// Idempotency: re-registering with the same Name + OwnerWorkerID +
// ConfigSchema bytes returns nil — workers may call this on every
// boot without coordinating. Schema or owner drift surfaces as an
// error so silent fleet skew is impossible.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/internal/trigger"
)

// registerAckTimeout bounds the ack request. The engine handler is
// expected to finish in well under a second; 5s gives slack for
// JetStream KV reads under load without letting a broken engine
// indefinitely hang a worker boot.
const registerAckTimeout = 5 * time.Second

// registerAckSubject mirrors the engine's `_REGISTRY.trigger_types.ack`
// micro endpoint. Duplicated as a literal (not imported) to keep the
// worker SDK independent of the internal/trigger package's private
// constants — the wire subject is the contract.
const registerAckSubject = "_REGISTRY.trigger_types.ack"

// RegisterTriggerType publishes def into the trigger_types KV bucket,
// then requests engine acknowledgement on `_REGISTRY.trigger_types.ack`
// (#330). Returns the engine's error verbatim on failure.
//
// Side effects:
//   - Sets def.OwnerWorkerID to w.WorkerID() when empty so callers
//     don't have to look it up.
//   - Sets def.RegisteredAt to time.Now().UTC() when zero.
//
// Idempotent at the engine side per the ack contract (#327): same
// Name + same OwnerWorkerID + same ConfigSchema → nil. Schema drift
// → error. Owner drift → error.
func (w *Worker) RegisterTriggerType(
	ctx context.Context, def trigger.TriggerTypeDef,
) error {
	if w.nc == nil {
		panic("RegisterTriggerType: worker.nc must not be nil")
	}
	if w.js == nil {
		panic("RegisterTriggerType: worker.js must not be nil")
	}
	if ctx == nil {
		return fmt.Errorf("RegisterTriggerType: ctx must not be nil")
	}
	if def.Name == "" {
		return fmt.Errorf("RegisterTriggerType: def.Name must not be empty")
	}
	if len(def.ConfigSchema) == 0 {
		return fmt.Errorf(
			"RegisterTriggerType: def.ConfigSchema must not be empty")
	}
	if def.OwnerWorkerID == "" {
		def.OwnerWorkerID = w.workerID
	}
	if def.RegisteredAt.IsZero() {
		def.RegisteredAt = time.Now().UTC()
	}

	kv, err := w.js.KeyValue(ctx, "trigger_types")
	if err != nil {
		return fmt.Errorf("trigger_types KV bind: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal TriggerTypeDef: %w", err)
	}
	if _, err := kv.Put(ctx, def.Name, data); err != nil {
		return fmt.Errorf("trigger_types KV put %q: %w", def.Name, err)
	}

	req := trigger.RegisterTriggerTypeRequest{
		Name:          def.Name,
		OwnerWorkerID: def.OwnerWorkerID,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal ack request: %w", err)
	}

	ackCtx, cancel := context.WithTimeout(ctx, registerAckTimeout)
	defer cancel()
	msg, err := w.nc.RequestWithContext(ackCtx, registerAckSubject, body)
	if err != nil {
		return fmt.Errorf("ack request %s: %w", registerAckSubject, err)
	}
	if len(msg.Data) == 0 {
		return nil
	}
	var resp struct {
		Error string `json:"error,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		// Non-JSON reply with data — surface as opaque error rather
		// than silently swallow. Mirrors externalRegistrar tolerance
		// for empty body = success but rejects unparseable failure.
		return fmt.Errorf("decode ack reply: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("engine ack error: %s", resp.Error)
	}
	return nil
}

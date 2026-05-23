// worker/trigger_types_test.go
// Methodology: real embedded NATS server, real TriggerService (engine
// side) so the ack roundtrip exercises the actual handleAck path.
// Tests cover the SDK contract documented in trigger_types.go:
// KV-then-ack ordering, owner default-fill, idempotent re-register,
// schema-drift rejection.
//
// Bounded waits via context.WithTimeout so a hung NATS server surfaces
// as a test failure within seconds, not a CI hang.
package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
)

// startWorkerForTriggers is the common harness for trigger SDK tests:
// engine TriggerService + a Worker on the same NATS, with `triggers`
// and `trigger_state` buckets provisioned (the default SetupAll only
// provisions trigger_types).
func startWorkerForTriggers(t *testing.T) *Worker {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc, err := trigger.NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("svc.Start: %v", err)
	}
	t.Cleanup(svc.Stop)

	w := NewWorker(nc)
	t.Cleanup(w.Stop)
	return w
}

func TestRegisterTriggerType_RoundTrip(t *testing.T) {
	w := startWorkerForTriggers(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 1*time.Second,
	)
	defer cancel()

	def := trigger.TriggerTypeDef{
		Name:         "fs.watch",
		Description:  "Filesystem watcher",
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
		Version:      "1.0.0",
	}
	// Positive: ack succeeds inside the 1s bound.
	if err := w.RegisterTriggerType(ctx, def); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}
	// Negative: OwnerWorkerID was default-filled from w.workerID.
	// Read the KV directly to confirm what landed.
	js := w.js
	kv, err := js.KeyValue(ctx, "trigger_types")
	if err != nil {
		t.Fatalf("KeyValue(trigger_types): %v", err)
	}
	entry, err := kv.Get(ctx, def.Name)
	if err != nil {
		t.Fatalf("kv.Get: %v", err)
	}
	var got trigger.TriggerTypeDef
	if err := json.Unmarshal(entry.Value(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.OwnerWorkerID != w.workerID {
		t.Fatalf("OwnerWorkerID = %q, want worker default %q",
			got.OwnerWorkerID, w.workerID)
	}
	if got.RegisteredAt.IsZero() {
		t.Fatalf("RegisteredAt was not default-filled")
	}
}

func TestRegisterTriggerType_Idempotent(t *testing.T) {
	w := startWorkerForTriggers(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()

	def := trigger.TriggerTypeDef{
		Name:         "fs.watch",
		Description:  "Filesystem watcher",
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
		Version:      "1.0.0",
	}
	if err := w.RegisterTriggerType(ctx, def); err != nil {
		t.Fatalf("first RegisterTriggerType: %v", err)
	}
	// Positive: same kind + same schema → nil error (engine ack
	// returns success on idempotent re-register).
	if err := w.RegisterTriggerType(ctx, def); err != nil {
		t.Fatalf("idempotent re-register: %v", err)
	}
	// Negative: schema drift surfaces as an error rather than silent
	// acceptance. We change the schema bytes; the engine's
	// installExternalRegistrar fast-fails on schema mismatch.
	defDrift := def
	defDrift.ConfigSchema = json.RawMessage(
		`{"type":"object","required":["path"]}`,
	)
	if err := w.RegisterTriggerType(ctx, defDrift); err == nil {
		t.Fatalf("schema drift accepted; want schema mismatch error")
	}
}

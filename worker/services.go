// worker/services.go
// ServiceDef + Worker.RegisterService SDK method (ADR-017 / #321).
//
// Services are a metadata namespace for grouping task types under a
// logical name. They are deliberately separated from the worker
// directory (worker/directory.go, #289): workers have a 60s TTL and
// a heartbeat loop; services have neither. A service entry persists
// across worker restarts and never expires automatically — it is a
// stable description, not a liveness signal.
//
// This file does not import or extend Directory. Sharing machinery
// would conflate two different lifecycles. See ADR-017 §Alternatives
// for the rejected re-use option.
package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// servicesBucket is the KV bucket name for service metadata.
// Provisioned by natsutil.SetupKVBuckets with TTL=0 and History=1.
const servicesBucket = "services"

// registerServiceTimeout bounds the KV Put call. Service registration
// is a startup-time operation; a multi-second hang would surface as a
// worker boot failure rather than a silent stall.
const registerServiceTimeout = 5 * time.Second

// ServiceDef is the metadata entry for a logical service in the
// `services` KV bucket. Pure descriptive surface — does NOT gate
// task invocation. The `service::task` convention in task-type
// names is just a naming hint; the engine never reads this bucket
// during dispatch.
//
// Fields are intentionally minimal. A `ParentService` grouping field
// was considered and deferred: no consumer exists yet (#274 R11 may
// add one). Adding it later is additive and last-write-wins handles
// the migration.
type ServiceDef struct {
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	RegisteredAt time.Time `json:"registered_at"`
}

// RegisterService publishes service metadata to the `services` KV
// bucket. Last-write-wins: re-calling with different Description (or
// any other field) silently replaces the prior entry without error.
// This is intentional — the bucket is a descriptive surface, not an
// authoritative registry, and worker restarts must be safe to repeat
// without conflict-handling boilerplate at every call site.
//
// Idempotency contract:
//   - Two calls with identical def → identical KV state.
//   - Two calls with the same Name but different Description → second
//     call's Description wins, no error returned.
//   - Concurrent calls race to the latest Put; no locking, no compare-
//     and-swap. Callers needing strict serialization should layer it
//     above.
//
// Panics on empty Name (programmer error). Stamps RegisteredAt on
// every call so callers don't have to.
func (w *Worker) RegisterService(def ServiceDef) error {
	if w == nil {
		panic("Worker.RegisterService: w must not be nil")
	}
	if def.Name == "" {
		panic("Worker.RegisterService: def.Name must not be empty")
	}

	kv, err := w.js.KeyValue(
		context.Background(), servicesBucket,
	)
	if err != nil {
		return err
	}

	def.RegisteredAt = time.Now()
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), registerServiceTimeout,
	)
	defer cancel()

	// Put (not Create): last-write-wins is the documented contract.
	_, err = kv.Put(ctx, def.Name, data)
	return err
}

// ListServices reads every entry from the `services` KV bucket.
// Returns an empty slice when no services are registered.
// Skips entries that fail to unmarshal so a single bad payload does
// not block the whole listing (defensive — the bucket is metadata
// only, never authoritative).
//
// This is package-level rather than a method on Worker because the
// CLI reads the bucket without owning a Worker. It takes a
// jetstream.JetStream handle so callers can share their existing
// connection.
func ListServices(
	js jetstream.JetStream,
) ([]ServiceDef, error) {
	if js == nil {
		panic("ListServices: js must not be nil")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, servicesBucket)
	if err != nil {
		return nil, err
	}

	keys, err := kv.ListKeys(ctx)
	if err != nil {
		// Empty bucket returns a "no keys found" error in some
		// nats.go versions — surface as an empty list instead so
		// callers don't have to special-case the boot state.
		if err == jetstream.ErrNoKeysFound {
			return []ServiceDef{}, nil
		}
		return nil, err
	}

	services := make([]ServiceDef, 0, 16)
	const maxServices = 10000
	count := 0
	for key := range keys.Keys() {
		count++
		if count > maxServices {
			panic("ListServices: keys exceeds max bound")
		}
		entry, err := kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var def ServiceDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}
		services = append(services, def)
	}
	return services, nil
}

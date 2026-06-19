// Package dagnatsext holds the public types that cross the DagNats worker SDK
// boundary so out-of-tree modules can register external trigger types and watch
// trigger lifecycle events without importing internal packages.
//
// All types in this package are pure data shapes — no behaviour wiring lives
// here. They are the contract between the dagnats core SDK (worker package) and
// add-on modules such as dagnats-ci.
package dagnatsext

import (
	"encoding/json"
	"time"
)

// TriggerTypeDef defines an External trigger type contributed by a worker.
// It crosses the worker SDK boundary so out-of-tree modules can call
// worker.RegisterTriggerType without naming internal/trigger types.
// Stored in the "trigger_types" KV bucket keyed by Name.
type TriggerTypeDef struct {
	Name          string          `json:"name"`
	OwnerWorkerID string          `json:"owner_worker_id"`
	Description   string          `json:"description"`
	ConfigSchema  json.RawMessage `json:"config_schema"`
	PayloadSchema json.RawMessage `json:"payload_schema"`
	Version       string          `json:"version"`
	RegisteredAt  time.Time       `json:"registered_at"`
}

// ExternalTriggerConfig selects an External trigger by Kind (matches the Name
// field of a registered TriggerTypeDef) and carries a Config payload that must
// validate against that type's ConfigSchema. Lives here so TriggerDef (below)
// and internal/trigger.TriggerDef can share the config shape without an import
// cycle.
type ExternalTriggerConfig struct {
	Kind   string          `json:"kind"`
	Config json.RawMessage `json:"config"`
}

// TriggerEnvelope is the standard workflow input produced by all trigger types.
// Workflows always know how they were triggered via this envelope. Published at
// the public boundary so add-on webhook receivers can construct it directly.
type TriggerEnvelope struct {
	Trigger    string          `json:"trigger"`
	Source     string          `json:"source"`
	WorkflowID string          `json:"workflow_id"`
	Timestamp  time.Time       `json:"timestamp"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// TriggerDef is the slim public view of a trigger delivered to WatchTriggers
// handlers. It carries only the fields an external worker needs to act on an
// activate/deactivate event — ID, WorkflowID, Enabled flag, and the External
// config identifying which kind and carrying the per-trigger config payload.
// The rich internal trigger.TriggerDef (with Cron/Subject/Webhook/HTTP fields)
// stays internal and is not exposed here.
type TriggerDef struct {
	ID         string                `json:"id"`
	WorkflowID string                `json:"workflow_id"`
	Enabled    bool                  `json:"enabled"`
	External   ExternalTriggerConfig `json:"external"`
}

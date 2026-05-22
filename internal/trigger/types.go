// trigger/types.go
// The trigger package provides automatic workflow triggering via cron
// schedules, NATS subject subscriptions, and HTTP webhooks. All trigger
// types produce standard workflow.started events on the history stream.
package trigger

import (
	"encoding/json"
	"time"
)

// TriggerDef defines a single trigger. Exactly one of Cron, Subject,
// Webhook, or HTTP must be non-nil. HTTP triggers (ADR-013) differ
// from Webhook triggers in that the caller waits for a workflow
// response; webhook callers are fire-and-forget. The shapes stay
// distinct so the semantic contract cannot be mistaken at a glance.
type TriggerDef struct {
	ID         string          `json:"id"`
	WorkflowID string          `json:"workflow_id"`
	Enabled    bool            `json:"enabled"`
	Cron       *CronConfig     `json:"cron,omitempty"`
	Subject    *SubjectConfig  `json:"subject,omitempty"`
	Webhook    *WebhookConfig  `json:"webhook,omitempty"`
	HTTP       *HTTPConfig     `json:"http,omitempty"`
	Debounce   *DebounceConfig `json:"debounce,omitempty"`
}

// DebounceConfig delays execution until events stop arriving.
// Period resets on each new event. Timeout is the hard upper bound
// — if events keep arriving and Period never elapses, the workflow
// fires after Timeout with the most recent event.
type DebounceConfig struct {
	Period  time.Duration `json:"period"`
	Timeout time.Duration `json:"timeout,omitempty"`
	Key     string        `json:"key,omitempty"`
}

// CronConfig defines a cron-scheduled trigger.
type CronConfig struct {
	Expression string `json:"expression"`
	Timezone   string `json:"timezone"`
	Backfill   bool   `json:"backfill"`
}

// SubjectConfig defines a NATS subject trigger.
type SubjectConfig struct {
	Subject string `json:"subject"`
}

// WebhookConfig defines an HTTP webhook trigger.
type WebhookConfig struct {
	Path   string `json:"path"`
	Secret string `json:"secret,omitempty"`
}

// TriggerEnvelope is the standard workflow input produced by all
// trigger types. Workflows always know how they were triggered.
// WorkflowID identifies which registered workflow to run; the
// orchestrator resolves the WorkflowDef from workflow_defs KV at
// handle time. Including it here keeps the trigger publish paths
// free of KV lookups (#167).
type TriggerEnvelope struct {
	Trigger    string          `json:"trigger"`
	Source     string          `json:"source"`
	WorkflowID string          `json:"workflow_id"`
	Timestamp  time.Time       `json:"timestamp"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// TriggerTypeDef defines an External trigger type contributed by a
// worker (parent #273 Phase 2.1, audit-adjusted in #313). Stored in
// the "trigger_types" KV bucket keyed by Name. The owning worker
// (OwnerWorkerID matches worker.WorkerRegistration.WorkerID) is the
// authority for both the trigger's config schema and its payload
// schema.
//
// ConfigSchema and PayloadSchema are json.RawMessage rather than
// string so they round-trip through json.Marshal/Unmarshal without
// double-encoding (audit fix): Phase 2.2's santhosh-tekuri/jsonschema
// validator and Phase 2.4's worker SDK can pass the bytes straight
// through. Storing a string here would re-quote the schema each hop.
//
// This is a pure data shape — no behavior wiring lives here. The
// registrar consumes TriggerTypeDef in Phase 2.3.
type TriggerTypeDef struct {
	Name          string          `json:"name"`
	OwnerWorkerID string          `json:"owner_worker_id"`
	Description   string          `json:"description"`
	ConfigSchema  json.RawMessage `json:"config_schema"`
	PayloadSchema json.RawMessage `json:"payload_schema"`
	Version       string          `json:"version"`
	RegisteredAt  time.Time       `json:"registered_at"`
}

// TriggerFire records a single trigger fire event for history
// tracking. Published to TRIGGER_HISTORY stream.
type TriggerFire struct {
	TriggerID  string    `json:"trigger_id"`
	WorkflowID string    `json:"workflow_id"`
	RunID      string    `json:"run_id,omitempty"`
	Source     string    `json:"source"`
	FiredAt    time.Time `json:"fired_at"`
	Input      []byte    `json:"input,omitempty"`
	Skipped    bool      `json:"skipped,omitempty"`
	SkipReason string    `json:"skip_reason,omitempty"`
}

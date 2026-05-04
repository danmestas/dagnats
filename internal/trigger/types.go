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
// or Webhook must be non-nil.
type TriggerDef struct {
	ID         string          `json:"id"`
	WorkflowID string          `json:"workflow_id"`
	Enabled    bool            `json:"enabled"`
	Cron       *CronConfig     `json:"cron,omitempty"`
	Subject    *SubjectConfig  `json:"subject,omitempty"`
	Webhook    *WebhookConfig  `json:"webhook,omitempty"`
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

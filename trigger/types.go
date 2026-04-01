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
	ID         string         `json:"id"`
	WorkflowID string         `json:"workflow_id"`
	Enabled    bool           `json:"enabled"`
	Cron       *CronConfig    `json:"cron,omitempty"`
	Subject    *SubjectConfig `json:"subject,omitempty"`
	Webhook    *WebhookConfig `json:"webhook,omitempty"`
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
type TriggerEnvelope struct {
	Trigger   string          `json:"trigger"`
	Source    string          `json:"source"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data,omitempty"`
}

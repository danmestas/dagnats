// Package configfile loads, validates, and diffs the declarative
// dagnats.yaml file that lists workflows and triggers. The package
// is pure: it produces a Plan that the apply layer turns into KV
// writes against the engine's workflow_defs and triggers buckets.
//
// ADR-018. The file shape uses YAML-aware structs (TriggerYAML,
// WorkflowYAML) rather than reusing dag.WorkflowDef and
// trigger.TriggerDef directly, because those carry only `json:` tags
// and gopkg.in/yaml.v3 wants `yaml:` tags. Converting at the package
// boundary keeps the runtime types unchanged while letting the file
// surface evolve independently.
package configfile

import (
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

// SourceFilePrefix marks KV records owned by the file loader.
// Used on the optional TriggerDef.Source field so the CLI / API
// can refuse to delete a file-managed trigger without operator
// intent (Phase 4, #358).
const SourceFilePrefix = "file:"

// ConfigFile is the root shape of dagnats.yaml. Both sections are
// optional — a file with only workflows or only triggers is valid.
// The legacy server.Config keys (data_dir, http_addr, workers, ...)
// are tolerated as unknown top-level fields by yaml.v3 with
// KnownFields(false), so the same file can carry both layers.
type ConfigFile struct {
	Workflows []WorkflowYAML `yaml:"workflows,omitempty"`
	Triggers  []TriggerYAML  `yaml:"triggers,omitempty"`
}

// WorkflowYAML mirrors the public surface of dag.WorkflowDef in
// YAML-friendly shape. Lowercase snake_case keys match the
// established `json:` tag convention on dag.WorkflowDef so an
// operator who knows the JSON surface can read the YAML at a glance.
type WorkflowYAML struct {
	Name           string                 `yaml:"name"`
	Version        string                 `yaml:"version,omitempty"`
	Steps          []StepYAML             `yaml:"steps"`
	Timeout        time.Duration          `yaml:"timeout,omitempty"`
	IdempotencyKey string                 `yaml:"idempotency_key,omitempty"`
	Metadata       map[string]interface{} `yaml:"metadata,omitempty"`
}

// StepYAML mirrors dag.StepDef. Type is the string form
// ("normal" / "agent_loop" / ...), matching dag.StepType.String().
type StepYAML struct {
	ID        string            `yaml:"id"`
	Task      string            `yaml:"task"`
	DependsOn []string          `yaml:"depends_on,omitempty"`
	Retries   int               `yaml:"retries,omitempty"`
	Timeout   time.Duration     `yaml:"timeout,omitempty"`
	Type      string            `yaml:"type,omitempty"`
	Metadata  map[string]string `yaml:"metadata,omitempty"`
}

// TriggerYAML mirrors trigger.TriggerDef. Exactly one of cron /
// subject / webhook / http must be set, matching the runtime rule.
type TriggerYAML struct {
	ID         string        `yaml:"id"`
	WorkflowID string        `yaml:"workflow_id"`
	Enabled    bool          `yaml:"enabled"`
	Cron       *CronYAML     `yaml:"cron,omitempty"`
	Subject    *SubjectYAML  `yaml:"subject,omitempty"`
	Webhook    *WebhookYAML  `yaml:"webhook,omitempty"`
	HTTP       *HTTPYAML     `yaml:"http,omitempty"`
	Debounce   *DebounceYAML `yaml:"debounce,omitempty"`
}

// CronYAML mirrors trigger.CronConfig.
type CronYAML struct {
	Expression string `yaml:"expression"`
	Timezone   string `yaml:"timezone,omitempty"`
	Backfill   bool   `yaml:"backfill,omitempty"`
}

// SubjectYAML mirrors trigger.SubjectConfig.
type SubjectYAML struct {
	Subject string `yaml:"subject"`
}

// WebhookYAML mirrors trigger.WebhookConfig.
type WebhookYAML struct {
	Path   string `yaml:"path"`
	Secret string `yaml:"secret,omitempty"`
}

// HTTPYAML mirrors trigger.HTTPConfig (the subset needed for declarative
// trigger registration). Method + Path are required; the full
// trigger.HTTPConfig carries additional respond-step fields populated
// at workflow runtime, not at trigger-registration time.
type HTTPYAML struct {
	Method string `yaml:"method"`
	Path   string `yaml:"path"`
}

// DebounceYAML mirrors trigger.DebounceConfig.
type DebounceYAML struct {
	Period  time.Duration `yaml:"period"`
	Timeout time.Duration `yaml:"timeout,omitempty"`
	Key     string        `yaml:"key,omitempty"`
}

// Plan is the result of Diff(current, desired). Each list holds
// fully-formed runtime values ready to hand to the apply layer.
// Add and Update both target Puts; the difference is recorded so the
// apply layer can log the right verb. Remove holds IDs only.
type Plan struct {
	WorkflowsAdd    []dag.WorkflowDef
	WorkflowsUpdate []dag.WorkflowDef
	WorkflowsRemove []string

	TriggersAdd    []trigger.TriggerDef
	TriggersUpdate []trigger.TriggerDef
	TriggersRemove []string
}

// Empty returns true when the plan has zero work.
func (p Plan) Empty() bool {
	return len(p.WorkflowsAdd) == 0 &&
		len(p.WorkflowsUpdate) == 0 &&
		len(p.WorkflowsRemove) == 0 &&
		len(p.TriggersAdd) == 0 &&
		len(p.TriggersUpdate) == 0 &&
		len(p.TriggersRemove) == 0
}

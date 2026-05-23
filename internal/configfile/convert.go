// internal/configfile/convert.go
// YAML-shaped structs → runtime types (dag.WorkflowDef,
// trigger.TriggerDef). Conversion is total: the loader has already
// validated invariants, so a conversion failure is an internal bug
// and panics.
package configfile

import (
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

// httpDefaultTimeoutMs and httpDefaultMaxBodyBytes are the defaults
// applied to HTTP triggers declared in dagnats.yaml. The runtime
// trigger.HTTPConfig.Validate() rejects zero values; YAML callers
// who don't override get reasonable production defaults rather than
// a validation error.
const (
	httpDefaultTimeoutMs    = 30_000
	httpDefaultMaxBodyBytes = 1 << 20
)

// ToWorkflowDef converts a validated WorkflowYAML into a runtime
// dag.WorkflowDef. Panics on programmer error (empty name reaches
// this point only via a Validate bypass).
func ToWorkflowDef(wf WorkflowYAML) dag.WorkflowDef {
	if wf.Name == "" {
		panic("ToWorkflowDef: name must not be empty")
	}
	if len(wf.Steps) == 0 {
		panic("ToWorkflowDef: steps must not be empty")
	}
	steps := make([]dag.StepDef, 0, len(wf.Steps))
	for _, s := range wf.Steps {
		steps = append(steps, dag.StepDef{
			ID:        s.ID,
			Task:      s.Task,
			DependsOn: s.DependsOn,
			Retries:   s.Retries,
			Timeout:   s.Timeout,
			Type:      parseStepType(s.Type),
			Metadata:  s.Metadata,
		})
	}
	return dag.WorkflowDef{
		Name:           wf.Name,
		Version:        wf.Version,
		Steps:          steps,
		Timeout:        wf.Timeout,
		IdempotencyKey: wf.IdempotencyKey,
	}
}

// parseStepType maps the YAML string form to the dag.StepType enum.
// Unknown / empty defaults to StepTypeNormal — the same default a
// missing JSON `type` field would produce in dag.WorkflowDef.
func parseStepType(s string) dag.StepType {
	switch s {
	case "", "normal":
		return dag.StepTypeNormal
	case "agent_loop":
		return dag.StepTypeAgentLoop
	case "sub_workflow":
		return dag.StepTypeSubWorkflow
	case "agent":
		return dag.StepTypeAgent
	case "map":
		return dag.StepTypeMap
	case "sleep":
		return dag.StepTypeSleep
	case "wait_for_event":
		return dag.StepTypeWaitForEvent
	case "approval":
		return dag.StepTypeApproval
	case "planner":
		return dag.StepTypePlanner
	case "respond":
		return dag.StepTypeRespond
	default:
		// Unknown step types reach this point only after Validate
		// accepted the file — which today does not gate type strings.
		// Tolerate the unknown value as Normal to keep the apply
		// path moving; the engine will fail loudly at run time if the
		// task is missing.
		return dag.StepTypeNormal
	}
}

// ToTriggerDef converts a validated TriggerYAML into a runtime
// trigger.TriggerDef with the Source annotation pre-populated.
// sourceLabel should be "file:<basename>" so KV inspection makes
// the origin obvious.
func ToTriggerDef(tr TriggerYAML, sourceLabel string) trigger.TriggerDef {
	if tr.ID == "" {
		panic("ToTriggerDef: id must not be empty")
	}
	if tr.WorkflowID == "" {
		panic("ToTriggerDef: workflow_id must not be empty")
	}
	if sourceLabel == "" {
		panic("ToTriggerDef: sourceLabel must not be empty")
	}

	def := trigger.TriggerDef{
		ID:         tr.ID,
		WorkflowID: tr.WorkflowID,
		Enabled:    tr.Enabled,
		Source:     sourceLabel,
	}
	if tr.Cron != nil {
		def.Cron = &trigger.CronConfig{
			Expression: tr.Cron.Expression,
			Timezone:   tr.Cron.Timezone,
			Backfill:   tr.Cron.Backfill,
		}
	}
	if tr.Subject != nil {
		def.Subject = &trigger.SubjectConfig{
			Subject: tr.Subject.Subject,
		}
	}
	if tr.Webhook != nil {
		def.Webhook = &trigger.WebhookConfig{
			Path:   tr.Webhook.Path,
			Secret: tr.Webhook.Secret,
		}
	}
	if tr.HTTP != nil {
		def.HTTP = &trigger.HTTPConfig{
			Method:       tr.HTTP.Method,
			Path:         tr.HTTP.Path,
			TimeoutMs:    httpDefaultTimeoutMs,
			MaxBodyBytes: httpDefaultMaxBodyBytes,
		}
	}
	if tr.Debounce != nil {
		def.Debounce = &trigger.DebounceConfig{
			Period:  tr.Debounce.Period,
			Timeout: tr.Debounce.Timeout,
			Key:     tr.Debounce.Key,
		}
	}
	return def
}

// SourceLabel constructs the value placed on the Source field of
// file-managed KV records: "file:<filename>". The filename is the
// last path component so KV inspection shows the file identity
// without leaking absolute paths into the bucket.
func SourceLabel(filename string) string {
	if filename == "" {
		panic("SourceLabel: filename must not be empty")
	}
	return SourceFilePrefix + filename
}

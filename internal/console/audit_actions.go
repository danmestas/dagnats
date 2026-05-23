package console

// audit_actions.go is the single source of truth for the audit-action
// vocabulary the console emits. Every call site that writes an action
// string passes one of these constants, not a string literal — the
// audit log filter, the audit-log table renderer, and the per-target
// link-back rules all key on the same finite enum.
//
// Outcomes are also constants: "success" / "denied" / "failed". An
// action call site picks one based on the result of the operation.
//
// Adding a new action: declare a constant here, update the audit log
// template's action filter dropdown (templates/audit_log.html), and
// pass the constant to actionAttempt at the call site.

// AuditAction is the short verb tag identifying one operator action.
// String form lives in the AuditEvent.Action field, in the URL query
// parameter that filters the audit log, and in the audit-log table.
type AuditAction string

const (
	// ActionDLQRetry — operator re-injected a dead-letter entry as a
	// fresh run. The replay step is followed by a best-effort discard
	// of the original entry; the discard outcome lands in Data.
	ActionDLQRetry AuditAction = "dlq.retry"

	// ActionDLQDiscard — operator removed a dead-letter entry
	// permanently. No undo by default; see ActionDLQUndoDiscard
	// for the soft-discard variant.
	ActionDLQDiscard AuditAction = "dlq.discard"

	// ActionDLQUndoDiscard — reserved for the soft-discard-with-undo
	// variant the brief mentions. Constant declared so the audit-log
	// filter dropdown can show the action when the soft-discard
	// behaviour lands; emit sites are gated until then.
	ActionDLQUndoDiscard AuditAction = "dlq.undo-discard"

	// ActionTriggerEnable / ActionTriggerDisable — operator flipped a
	// trigger's enabled bit. Target is the trigger id; Data records
	// the kind (cron / webhook / subject / http) so the audit log
	// stays grokkable without joining back to ListTriggers.
	ActionTriggerEnable  AuditAction = "trigger.enable"
	ActionTriggerDisable AuditAction = "trigger.disable"

	// ActionTriggerFireManual — operator clicked Fire-now on a cron
	// or webhook trigger to force one immediate run (#352). Target is
	// the trigger id; Data records the workflow id + the new run id
	// on success, or the rejection reason ("read_only",
	// "rate_limited", "not_fireable", "disabled", "engine_error") on
	// the denied / failed paths.
	ActionTriggerFireManual AuditAction = "trigger.fire.manual"

	// ActionWorkflowRun — operator started a fresh run from the
	// inline Run button on the workflows list. Target is the
	// workflow name; Data carries the new run id on success and
	// the rejection reason ("read_only", "input_required",
	// "engine_error") on the denied / failed paths. Scope is
	// limited to no-input workflows for R8; the typed-input form
	// path remains out of scope.
	ActionWorkflowRun AuditAction = "workflow.run"
)

// String returns the action's wire form. Centralising the cast keeps
// call sites readable: `string(ActionDLQRetry)` is noisy at every
// usage; `ActionDLQRetry.String()` reads as English.
func (a AuditAction) String() string {
	if a == "" {
		panic("AuditAction.String: empty action")
	}
	return string(a)
}

// AuditOutcome is the per-event outcome string. Same single-source-of-
// truth shape as AuditAction; emit sites pass an OutcomeXxx constant
// rather than a literal.
type AuditOutcome string

const (
	OutcomeSuccess AuditOutcome = "success"
	OutcomeDenied  AuditOutcome = "denied"
	OutcomeFailed  AuditOutcome = "failed"
)

// String returns the outcome's wire form. Matches AuditAction.String.
func (o AuditOutcome) String() string {
	if o == "" {
		panic("AuditOutcome.String: empty outcome")
	}
	return string(o)
}

// auditActionsAll is the list the audit-log filter dropdown renders.
// Order matches the dropdown's option order so the rendered HTML
// stays stable between server restarts. Adding a new constant above
// also requires extending this list.
var auditActionsAll = []AuditAction{
	ActionDLQRetry,
	ActionDLQDiscard,
	ActionDLQUndoDiscard,
	ActionTriggerEnable,
	ActionTriggerDisable,
	ActionTriggerFireManual,
	ActionWorkflowRun,
}

// AuditActionList returns a defensive copy of the registered action
// constants. Used by the audit-log filter renderer; keeping the
// returned slice independent of the package-level slice prevents
// callers from mutating the source of truth.
func AuditActionList() []AuditAction {
	out := make([]AuditAction, len(auditActionsAll))
	copy(out, auditActionsAll)
	return out
}

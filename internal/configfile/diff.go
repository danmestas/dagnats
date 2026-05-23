// internal/configfile/diff.go
// Pure plan computation. Given the file-managed records currently in
// KV and the records the file wants, produce a Plan whose three
// lists drive add / update / remove KV ops.
//
// Equality is structural via reflect.DeepEqual after blanking
// implementation-internal fields (e.g. the Source string is set by
// the converter for desired records but not necessarily on current
// records read back). Keeping the diff pure means the only KV writes
// are exactly the ones the operator's edit warranted.
package configfile

import (
	"reflect"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

// CurrentState is the file-managed KV slice the apply layer
// reads in. Both maps are keyed by the same key the engine uses
// (workflow name; trigger ID).
type CurrentState struct {
	Workflows map[string]dag.WorkflowDef
	Triggers  map[string]trigger.TriggerDef
}

// DesiredState is the runtime-typed shape of the file. Built by
// ToWorkflowDef / ToTriggerDef once per Load+Validate pass.
type DesiredState struct {
	Workflows map[string]dag.WorkflowDef
	Triggers  map[string]trigger.TriggerDef
}

// Diff returns the Plan that moves current → desired. Pure.
// Order within each list mirrors map iteration order in Go (random)
// but is acceptable here because each item is independent and the
// apply layer batches them.
func Diff(current CurrentState, desired DesiredState) Plan {
	var plan Plan

	plan.WorkflowsAdd,
		plan.WorkflowsUpdate,
		plan.WorkflowsRemove =
		diffWorkflows(current.Workflows, desired.Workflows)

	plan.TriggersAdd,
		plan.TriggersUpdate,
		plan.TriggersRemove =
		diffTriggers(current.Triggers, desired.Triggers)

	return plan
}

// diffWorkflows segments the workflow space into add/update/remove.
func diffWorkflows(
	current, desired map[string]dag.WorkflowDef,
) (add, update []dag.WorkflowDef, remove []string) {
	for name, desiredDef := range desired {
		currentDef, ok := current[name]
		switch {
		case !ok:
			add = append(add, desiredDef)
		case !workflowEqual(currentDef, desiredDef):
			update = append(update, desiredDef)
		}
	}
	for name := range current {
		if _, ok := desired[name]; !ok {
			remove = append(remove, name)
		}
	}
	return add, update, remove
}

// diffTriggers segments the trigger space. Source is intentionally
// ignored during equality so a record that has been touched by the
// apply layer (Source=file:dagnats.yaml) compares equal to the
// freshly-converted desired record with the same fields.
func diffTriggers(
	current, desired map[string]trigger.TriggerDef,
) (add, update []trigger.TriggerDef, remove []string) {
	for id, desiredDef := range desired {
		currentDef, ok := current[id]
		switch {
		case !ok:
			add = append(add, desiredDef)
		case !triggerEqual(currentDef, desiredDef):
			update = append(update, desiredDef)
		}
	}
	for id := range current {
		if _, ok := desired[id]; !ok {
			remove = append(remove, id)
		}
	}
	return add, update, remove
}

// workflowEqual compares two WorkflowDef values for structural
// equality. Uses reflect.DeepEqual on the whole struct — every field
// is JSON-marshalable so deep equality matches "round-trips to the
// same bytes" for the purpose of avoiding spurious KV writes.
func workflowEqual(a, b dag.WorkflowDef) bool {
	return reflect.DeepEqual(a, b)
}

// triggerEqual compares two TriggerDef values, ignoring Source so a
// re-read after apply doesn't bounce the diff.
func triggerEqual(a, b trigger.TriggerDef) bool {
	a.Source = ""
	b.Source = ""
	return reflect.DeepEqual(a, b)
}

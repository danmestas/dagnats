// internal/configfile/apply.go
// Apply layer: given a Plan and access to the workflow_defs +
// triggers KV buckets, write the changes through. Keeps the diff
// pure (no NATS) and the watcher reload simple (give me a Plan).
//
// Direct KV writes mirror what internal/api.Service.createTriggerInner
// and registerWorkflowInner already do — same validation rules, same
// keys, same Put cadence. Going through the API layer would bring an
// import cycle into play (server → api needs dag, configfile would
// need server). Direct writes are the least entangled option.
package configfile

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go/jetstream"
)

// KVHandles bundles the two buckets the apply layer needs. Passed
// in rather than constructed so test code can substitute mocks /
// pre-seeded buckets without dragging the embedded NATS server
// dependency into the configfile package.
type KVHandles struct {
	WorkflowDefs jetstream.KeyValue
	Triggers     jetstream.KeyValue
}

// Apply executes plan against kv. Errors are accumulated rather than
// fail-fast so a single bad trigger does not block the apply of the
// other 49 unaffected entries; the returned error is the joined
// list. Successful writes log via the caller's logger (passed via
// the closure — apply itself is library code, not a slog client).
func Apply(
	ctx context.Context, kv KVHandles, plan Plan,
) error {
	if ctx == nil {
		panic("Apply: ctx must not be nil")
	}
	if kv.WorkflowDefs == nil {
		panic("Apply: WorkflowDefs KV must not be nil")
	}
	if kv.Triggers == nil {
		panic("Apply: Triggers KV must not be nil")
	}

	var errs []error
	errs = appendErr(errs,
		applyWorkflows(ctx, kv.WorkflowDefs, plan))
	errs = appendErr(errs,
		applyTriggers(ctx, kv.Triggers, plan))

	if len(errs) == 0 {
		return nil
	}
	return joinErrs(errs)
}

// applyWorkflows writes workflow add/update KV puts and remove
// deletes. Each entry runs dag.Validate first so a malformed
// workflow never lands in KV.
func applyWorkflows(
	ctx context.Context, kv jetstream.KeyValue, plan Plan,
) []error {
	if len(plan.WorkflowsAdd) > maxEntries ||
		len(plan.WorkflowsUpdate) > maxEntries ||
		len(plan.WorkflowsRemove) > maxEntries {
		panic("applyWorkflows: exceeds max bound")
	}
	var errs []error
	for _, def := range plan.WorkflowsAdd {
		if err := putWorkflow(ctx, kv, def); err != nil {
			errs = append(errs,
				fmt.Errorf("workflow add %q: %w", def.Name, err))
		}
	}
	for _, def := range plan.WorkflowsUpdate {
		if err := putWorkflow(ctx, kv, def); err != nil {
			errs = append(errs,
				fmt.Errorf("workflow update %q: %w", def.Name, err))
		}
	}
	for _, name := range plan.WorkflowsRemove {
		if err := kv.Delete(ctx, name); err != nil {
			errs = append(errs,
				fmt.Errorf("workflow remove %q: %w", name, err))
		}
	}
	return errs
}

// putWorkflow validates and Puts one WorkflowDef. Extracted so the
// add and update branches stay below the 70-line limit.
func putWorkflow(
	ctx context.Context, kv jetstream.KeyValue, def dag.WorkflowDef,
) error {
	if def.Name == "" {
		panic("putWorkflow: name must not be empty")
	}
	if err := dag.Validate(def); err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := kv.Put(ctx, def.Name, data); err != nil {
		return fmt.Errorf("kv put: %w", err)
	}
	return nil
}

// applyTriggers writes trigger add/update KV puts and remove
// deletes. Validation goes through trigger.Validate so HTTP routes
// and required-field rules match the REST surface.
func applyTriggers(
	ctx context.Context, kv jetstream.KeyValue, plan Plan,
) []error {
	if len(plan.TriggersAdd) > maxEntries ||
		len(plan.TriggersUpdate) > maxEntries ||
		len(plan.TriggersRemove) > maxEntries {
		panic("applyTriggers: exceeds max bound")
	}
	var errs []error
	for _, def := range plan.TriggersAdd {
		if err := putTrigger(ctx, kv, def); err != nil {
			errs = append(errs,
				fmt.Errorf("trigger add %q: %w", def.ID, err))
		}
	}
	for _, def := range plan.TriggersUpdate {
		if err := putTrigger(ctx, kv, def); err != nil {
			errs = append(errs,
				fmt.Errorf("trigger update %q: %w", def.ID, err))
		}
	}
	for _, id := range plan.TriggersRemove {
		if err := kv.Delete(ctx, id); err != nil {
			errs = append(errs,
				fmt.Errorf("trigger remove %q: %w", id, err))
		}
	}
	return errs
}

// putTrigger validates and Puts one TriggerDef.
func putTrigger(
	ctx context.Context, kv jetstream.KeyValue,
	def trigger.TriggerDef,
) error {
	if def.ID == "" {
		panic("putTrigger: id must not be empty")
	}
	if err := trigger.Validate(def); err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := kv.Put(ctx, def.ID, data); err != nil {
		return fmt.Errorf("kv put: %w", err)
	}
	return nil
}

// ReadCurrent loads the file-managed slice of the workflow_defs and
// triggers buckets — every record whose Source matches the file
// label. KV-managed entries (no Source, or a different prefix) are
// excluded so the diff never proposes deleting them.
//
// sourceLabel: the SourceLabel(filename) value to match on.
func ReadCurrent(
	ctx context.Context, kv KVHandles, sourceLabel string,
) (CurrentState, error) {
	if ctx == nil {
		panic("ReadCurrent: ctx must not be nil")
	}
	if kv.WorkflowDefs == nil {
		panic("ReadCurrent: WorkflowDefs KV must not be nil")
	}
	if kv.Triggers == nil {
		panic("ReadCurrent: Triggers KV must not be nil")
	}
	if sourceLabel == "" {
		panic("ReadCurrent: sourceLabel must not be empty")
	}

	triggers, err := readFileTriggers(ctx, kv.Triggers, sourceLabel)
	if err != nil {
		return CurrentState{}, fmt.Errorf(
			"read file-managed triggers: %w", err)
	}
	// Workflows do not (yet) carry a Source field — workflow_defs
	// belong to one administrative source per name. The diff layer
	// treats every desired workflow as authoritative for that name
	// and removes only workflows we explicitly added (tracked via
	// the closing over).
	return CurrentState{
		Workflows: map[string]dag.WorkflowDef{},
		Triggers:  triggers,
	}, nil
}

// readFileTriggers walks the triggers bucket and returns only those
// records whose Source equals sourceLabel. Bounded by maxEntries.
func readFileTriggers(
	ctx context.Context, kv jetstream.KeyValue, sourceLabel string,
) (map[string]trigger.TriggerDef, error) {
	keysCtx, cancel := contextWithTimeout(ctx)
	defer cancel()
	keys, err := kv.Keys(keysCtx)
	if err != nil {
		if isNoKeysFound(err) {
			return map[string]trigger.TriggerDef{}, nil
		}
		return nil, fmt.Errorf("kv keys: %w", err)
	}
	if len(keys) > maxEntries {
		keys = keys[:maxEntries]
	}
	out := make(map[string]trigger.TriggerDef, len(keys))
	for _, k := range keys {
		entry, err := kv.Get(ctx, k)
		if err != nil {
			continue // best-effort; transient KV miss is benign
		}
		var def trigger.TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}
		if def.Source != sourceLabel {
			continue
		}
		out[def.ID] = def
	}
	return out, nil
}

// api/service_defs.go
// Split out of service.go (#566): workflow-definition domain of the control
// plane Service. Shares the private Service NATS/KV bundle; no new
// connection layer. Behavior identical to the pre-split file.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

// RegisterWorkflow validates and persists a workflow definition under
// its name. Subsequent calls with the same name overwrite the previous
// version -- the engine reads the definition at run-start time.
func (s *Service) RegisterWorkflow(
	ctx context.Context, def dag.WorkflowDef,
) error {
	if ctx == nil {
		panic("RegisterWorkflow: ctx must not be nil")
	}
	if def.Name == "" {
		panic("RegisterWorkflow: def.Name must not be empty")
	}
	return s.observed(ctx, "registerWorkflow",
		[]attribute.KeyValue{
			attribute.String("workflow_name", def.Name),
		},
		func(ctx context.Context) error {
			return s.registerWorkflowInner(ctx, def)
		},
	)
}

// registerWorkflowInner holds the core logic, keeping the
// instrumented wrapper under the 70-line limit.
//
// Beyond the historical name -> latest-def pointer write, this also
// persists an immutable name.v.hash version snapshot (#637) so a
// run that started under this content stays pinned to it even after
// a later re-register moves the pointer -- see loadRunAndDef in
// internal/engine/orchestrator.go. The version write is idempotent:
// re-registering byte-identical content is a no-op past the initial
// write (content-addressed, so the existing key already holds it).
func (s *Service) registerWorkflowInner(
	ctx context.Context, def dag.WorkflowDef,
) error {
	if s.defKV == nil {
		panic("registerWorkflowInner: defKV must not be nil")
	}
	if def.Name == "" {
		panic("registerWorkflowInner: def.Name must not be empty")
	}
	if dag.IsDefVersionKey(def.Name) {
		return fmt.Errorf(
			"workflow name %q is reserved for internal def "+
				"versioning and cannot be registered", def.Name)
	}
	if err := dag.Validate(def); err != nil {
		return fmt.Errorf("invalid workflow: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	if err := s.persistDefVersion(ctx, def, data); err != nil {
		return err
	}
	_, err = s.defKV.Put(ctx, def.Name, data)
	return err
}

// RegisterWorkflowWithWarnings is the variant that returns the
// graph-level warnings produced by dag.ValidateRespondReachability
// alongside the persistence outcome. Per ADR-013 PR 3, the REST
// handler surfaces these warnings in the response body so the
// workflow author sees them at registration time, not first
// production hang. Fatal field-level errors (dag.Validate) still
// short-circuit the persist; warnings do NOT.
//
// hasHTTPTrigger is computed by walking the triggers KV for any
// trigger whose WorkflowID matches def.Name and whose HTTP variant
// is non-nil. A registration error during the trigger lookup is
// logged and treated as "no HTTP trigger" — failing the registration
// over a transient list error would be worse than skipping the
// reachability warning.
func (s *Service) RegisterWorkflowWithWarnings(
	ctx context.Context, def dag.WorkflowDef,
) ([]dag.Warning, error) {
	if ctx == nil {
		panic("RegisterWorkflowWithWarnings: ctx must not be nil")
	}
	if def.Name == "" {
		panic("RegisterWorkflowWithWarnings: def.Name must not be empty")
	}
	if err := s.RegisterWorkflow(ctx, def); err != nil {
		return nil, err
	}
	hasHTTP := s.hasHTTPTriggerFor(ctx, def.Name)
	return dag.ValidateRespondReachability(def, hasHTTP), nil
}

// hasHTTPTriggerFor returns true when at least one trigger in the
// triggers KV binds an HTTP variant to workflowName. Errors are
// logged and the function falls through to false so a transient KV
// hiccup never escalates into a failed registration.
func (s *Service) hasHTTPTriggerFor(
	ctx context.Context, workflowName string,
) bool {
	if workflowName == "" {
		panic("hasHTTPTriggerFor: workflowName must not be empty")
	}
	if s.triggerKV == nil {
		return false
	}
	defs, err := s.listTriggersInner(ctx)
	if err != nil {
		if !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Warn("list triggers for HTTP-trigger check",
				"error", err, "workflow", workflowName)
		}
		return false
	}
	for _, d := range defs {
		if d.WorkflowID != workflowName {
			continue
		}
		if d.HTTP != nil {
			return true
		}
	}
	return false
}

// GetWorkflow retrieves the registered definition for the named
// workflow. Returns a key-not-found error when not registered.
func (s *Service) GetWorkflow(name string) (dag.WorkflowDef, error) {
	if name == "" {
		panic("GetWorkflow: name must not be empty")
	}
	if s.defKV == nil {
		panic("GetWorkflow: defKV must not be nil")
	}
	entry, err := s.defKV.Get(context.Background(), name)
	if err != nil {
		return dag.WorkflowDef{}, err
	}
	var def dag.WorkflowDef
	err = json.Unmarshal(entry.Value(), &def)
	return def, err
}

// DeleteWorkflow removes a registered workflow definition by name. It
// fails when the name is not registered (KV Delete alone is idempotent
// and would report success for a typo'd name — #607 requires a loud
// error instead). Only the definition record is touched; historical run
// snapshots live in the separate workflow_runs bucket and are untouched.
func (s *Service) DeleteWorkflow(
	ctx context.Context, name string,
) error {
	if ctx == nil {
		panic("DeleteWorkflow: ctx must not be nil")
	}
	if name == "" {
		panic("DeleteWorkflow: name must not be empty")
	}
	return s.observed(ctx, "deleteWorkflow",
		[]attribute.KeyValue{
			attribute.String("workflow_name", name),
		},
		func(ctx context.Context) error {
			return s.deleteWorkflowInner(ctx, name)
		},
	)
}

// deleteWorkflowInner confirms the definition exists, then removes it.
func (s *Service) deleteWorkflowInner(
	ctx context.Context, name string,
) error {
	if name == "" {
		panic("deleteWorkflowInner: name must not be empty")
	}
	if s.defKV == nil {
		panic("deleteWorkflowInner: defKV must not be nil")
	}
	if _, err := s.defKV.Get(ctx, name); err != nil {
		return fmt.Errorf("workflow %q not found: %w", name, err)
	}
	return s.defKV.Delete(ctx, name)
}

// ListWorkflows retrieves all registered workflow definitions from KV.
func (s *Service) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if ctx == nil {
		panic("ListWorkflows: ctx must not be nil")
	}
	if s.defKV == nil {
		panic("ListWorkflows: defKV must not be nil")
	}
	var defs []dag.WorkflowDef
	err := s.observed(ctx, "listWorkflows", nil,
		func(ctx context.Context) error {
			var innerErr error
			defs, innerErr = s.listWorkflowsInner(ctx)
			return innerErr
		},
	)
	return defs, err
}

// listWorkflowsInner holds the KV iteration logic.
func (s *Service) listWorkflowsInner(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if s.defKV == nil {
		panic("listWorkflowsInner: defKV must not be nil")
	}
	if s.js == nil {
		panic("listWorkflowsInner: js must not be nil")
	}
	keys, err := s.defKV.Keys(ctx)
	if err != nil {
		// Empty bucket -- treat as the documented "no workflows
		// registered" case so consumers (console, REST, NATS) get
		// nil slice + nil error and can render empty-state. Mirrors
		// the pattern used by ListTriggers, scheduled.go, and the
		// engine snapshot store.
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []dag.WorkflowDef{}, nil
		}
		return nil, err
	}

	// Skip immutable name.v.hash version snapshots (#637) -- they
	// share the bucket with the mutable name -> latest pointers this
	// endpoint lists, but are not themselves a "registered workflow".
	nameKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if dag.IsDefVersionKey(key) {
			continue
		}
		nameKeys = append(nameKeys, key)
	}

	entries, err := natsutil.ParallelGetJS(
		s.defKV, nameKeys, natsutil.DefaultParallelism,
	)
	if err != nil {
		return nil, err
	}

	defs := make([]dag.WorkflowDef, 0, len(entries))
	for _, entry := range entries {
		var def dag.WorkflowDef
		if err := json.Unmarshal(
			entry.Value(), &def,
		); err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, nil
}

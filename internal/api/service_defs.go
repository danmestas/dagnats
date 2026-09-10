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
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

// nonTerminalRunListCap bounds how many offending run IDs
// ErrWorkflowHasNonTerminalRuns lists explicitly (#682). Total still
// carries the true count, so a workflow with hundreds of active runs
// gets an honest number instead of a silently truncated list.
const nonTerminalRunListCap = 20

// ErrWorkflowHasNonTerminalRuns is returned by DeleteWorkflow when
// runs for the workflow are still non-terminal and force was not
// passed (#682). The REST handler maps this to 409; the CLI maps it
// to exit code 2, same as the trigger-reference refusal.
type ErrWorkflowHasNonTerminalRuns struct {
	Name   string
	RunIDs []string
	Total  int
}

func (e *ErrWorkflowHasNonTerminalRuns) Error() string {
	if e == nil {
		panic("ErrWorkflowHasNonTerminalRuns.Error: receiver is nil")
	}
	suffix := ""
	if e.Total > len(e.RunIDs) {
		suffix = fmt.Sprintf(" (and %d more)", e.Total-len(e.RunIDs))
	}
	return fmt.Sprintf(
		"refused: workflow %q has %d non-terminal run(s): %s%s."+
			" Cancel them or rerun with force.",
		e.Name, e.Total, strings.Join(e.RunIDs, ", "), suffix,
	)
}

// newErrWorkflowHasNonTerminalRuns constructs the refusal error. The
// Total==0 invariant belongs here, at construction, rather than in
// Error() -- a Stringer/error's Error() method must never panic, since
// fmt (and slog, when logging an error value) may call it at format
// time on a code path the caller does not control.
func newErrWorkflowHasNonTerminalRuns(
	name string, runIDs []string, total int,
) *ErrWorkflowHasNonTerminalRuns {
	if name == "" {
		panic("newErrWorkflowHasNonTerminalRuns: name must not be empty")
	}
	if total == 0 {
		panic("newErrWorkflowHasNonTerminalRuns: total must not be zero")
	}
	return &ErrWorkflowHasNonTerminalRuns{
		Name: name, RunIDs: runIDs, Total: total,
	}
}

// ErrWorkflowHasTriggers is returned by DeleteWorkflow when name is
// still referenced by trigger(s) and force was not passed (#607).
// Pulled down from a CLI-only check into the shared service guard
// (#682 review) so REST inherits the same refusal instead of the
// weaker "definition gone, trigger still firing at it" contract the
// HTTP-only guard would otherwise have. The REST handler maps this to
// 409; the CLI maps it to exit code 2.
type ErrWorkflowHasTriggers struct {
	Name       string
	TriggerIDs []string
}

func (e *ErrWorkflowHasTriggers) Error() string {
	if e == nil {
		panic("ErrWorkflowHasTriggers.Error: receiver is nil")
	}
	return fmt.Sprintf(
		"refused: workflow %q still has trigger(s) referencing it: %s."+
			" Delete the trigger(s) or rerun with force.",
		e.Name, strings.Join(e.TriggerIDs, ", "),
	)
}

// newErrWorkflowHasTriggers constructs the refusal error. Same
// construction-site-assertion rationale as
// newErrWorkflowHasNonTerminalRuns.
func newErrWorkflowHasTriggers(
	name string, triggerIDs []string,
) *ErrWorkflowHasTriggers {
	if name == "" {
		panic("newErrWorkflowHasTriggers: name must not be empty")
	}
	if len(triggerIDs) == 0 {
		panic("newErrWorkflowHasTriggers: triggerIDs must not be empty")
	}
	return &ErrWorkflowHasTriggers{Name: name, TriggerIDs: triggerIDs}
}

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
// persists an immutable name.v.hash version snapshot (#637, via
// persistDef -- see its doc for why runtimes.go's registerRuntimeWorkflow
// shares this exact path) so a run that started under this content
// stays pinned to it even after a later re-register moves the
// pointer -- see loadRunAndDef in internal/engine/orchestrator.go.
// The version write is idempotent: re-registering byte-identical
// content is a no-op past the initial write (content-addressed, so
// the existing key already holds it). Concurrent RegisterWorkflow
// calls for the same name are last-writer-wins on the pointer, same
// as before #637.
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
	return s.persistDef(ctx, def, data)
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
// error instead). Unless force is set, it also refuses with
// *ErrWorkflowHasTriggers while a trigger still references name
// (#607), and *ErrWorkflowHasNonTerminalRuns while any run for name is
// non-terminal (#682) -- deleting the definition out from under a
// still-running run would strand its next advance. Only the
// definition record is touched; historical run snapshots live in the
// separate workflow_runs bucket and are untouched either way.
//
// The non-terminal-run guard has a TOCTOU window: a run admitted
// between the scan and the delete is not seen by either. That run's
// next advance fails loudly via engine's failMissingPinnedVersion
// (missing pinned def version) rather than silently reading a
// different definition -- a stuck run beats a silently re-defined one,
// and it is exactly the same outcome force deliberately produces on
// purpose for every run left non-terminal at delete time.
func (s *Service) DeleteWorkflow(
	ctx context.Context, name string, force bool,
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
			attribute.Bool("force", force),
		},
		func(ctx context.Context) error {
			return s.deleteWorkflowInner(ctx, name, force)
		},
	)
}

// deleteWorkflowInner confirms the definition exists, applies the
// trigger-reference guard (#607) and the non-terminal-run guard (#682)
// unless force is set, then removes it. Both guards live here (not in
// a caller-side wrapper) so REST and CLI -- and any future caller --
// get the same refusal contract for free.
func (s *Service) deleteWorkflowInner(
	ctx context.Context, name string, force bool,
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
	if !force {
		triggerIDs, err := s.referencingTriggerIDs(ctx, name)
		if err != nil {
			return err
		}
		if len(triggerIDs) > 0 {
			return newErrWorkflowHasTriggers(name, triggerIDs)
		}
		runIDs, total, err := s.nonTerminalRunIDsForWorkflow(ctx, name)
		if err != nil {
			return err
		}
		if total > 0 {
			return newErrWorkflowHasNonTerminalRuns(name, runIDs, total)
		}
	}
	if err := s.deleteDefVersions(ctx, name); err != nil {
		return err
	}
	return s.defKV.Delete(ctx, name)
}

// referencingTriggerIDs returns the IDs of triggers whose WorkflowID
// matches name (#607, pulled down from a CLI-only helper into the
// shared service guard -- #682 review). An empty triggers bucket (no
// keys) is the benign "nothing references it" case, reported as no
// references.
func (s *Service) referencingTriggerIDs(
	ctx context.Context, name string,
) ([]string, error) {
	if ctx == nil {
		panic("referencingTriggerIDs: ctx must not be nil")
	}
	if name == "" {
		panic("referencingTriggerIDs: name must not be empty")
	}
	defs, err := s.ListTriggers(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("list triggers: %w", err)
	}
	const maxRefs = 10_000
	refs := make([]string, 0)
	for i, def := range defs {
		if i >= maxRefs {
			break
		}
		if def.WorkflowID == name {
			refs = append(refs, def.ID)
		}
	}
	return refs, nil
}

// nonTerminalRunIDsForWorkflow returns up to nonTerminalRunListCap run
// IDs for name that are not yet terminal, plus the true total (#682).
// Backed by the reconciler's active-run index (ListActive) rather than
// a scan over all history, so this stays cheap even on a large
// workflow_runs population -- mirrors countActiveRunsForRoot's pattern
// in runtimes.go.
//
// This is best-effort, not an absolute guarantee: ListActive's
// underlying fetch can silently drop a run on a per-key timeout
// (ScanStats.Skipped, via ParallelGetJSBestEffort) without that
// setting Truncated. What this function DOES fail closed on is the
// cheaper, larger-blast-radius failure mode -- the active-ID list
// itself coming back truncated -- which is treated as an error rather
// than a silent under-count, same as countActiveRunsForRoot's own
// truncation handling.
func (s *Service) nonTerminalRunIDsForWorkflow(
	ctx context.Context, name string,
) ([]string, int, error) {
	if ctx == nil {
		panic("nonTerminalRunIDsForWorkflow: ctx must not be nil")
	}
	if name == "" {
		panic("nonTerminalRunIDsForWorkflow: name must not be empty")
	}
	runs, stats, err := s.store.ListActive(ctx, runtimeRunScanMax)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	if stats.Truncated {
		activeRunCountTruncated.Add(ctx, 1)
		slog.ErrorContext(ctx,
			"nonTerminalRunIDsForWorkflow: ListActive truncated; "+
				"refusing to trust an under-count for delete safety",
			"workflow", name,
			"fetch_max", runtimeRunScanMax,
		)
		return nil, 0, fmt.Errorf(
			"non-terminal run scan for workflow %q truncated at %d -- "+
				"refusing to under-count for delete safety",
			name, runtimeRunScanMax,
		)
	}
	ids := make([]string, 0, nonTerminalRunListCap)
	total := 0
	for i := range runs {
		if runs[i].WorkflowID != name {
			continue
		}
		total++
		if len(ids) < nonTerminalRunListCap {
			ids = append(ids, runs[i].RunID)
		}
	}
	return ids, total, nil
}

// deleteDefVersions removes every immutable name.v.hash version key
// for name (#637 review fix): DeleteWorkflow used to delete only the
// mutable pointer, leaking up to DefVersionsMax version snapshots
// forever. Bounded by defVersionKeysForName's own scan bound
// (defVersionScanMax).
//
// Any run still pinned to one of these versions will fail its next
// advance (missing pinned version, engine.def_pin.missing_version)
// rather than silently reading something else -- consistent with
// #637's fail-loud rule. An explicit administrative delete of the
// whole workflow is exactly the case where that tradeoff is correct:
// the operator asked for the definition gone.
func (s *Service) deleteDefVersions(ctx context.Context, name string) error {
	if ctx == nil {
		panic("deleteDefVersions: ctx must not be nil")
	}
	if name == "" {
		panic("deleteDefVersions: name must not be empty")
	}
	versionKeys, err := s.defVersionKeysForName(ctx, name)
	if err != nil {
		return err
	}
	for _, key := range versionKeys {
		if err := s.defKV.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
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

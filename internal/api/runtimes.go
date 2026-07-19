// api/runtimes.go
// Server-side control-plane endpoints for agent runtimes (ADR-021 Phase
// A, #376). Two additive operations the worker-side ControlPlane handle
// calls over NATS request/reply:
//
//   - RegisterRuntimeWorkflow: author an ephemeral workflow def at
//     runtime. The SERVER owns naming — it scopes def.Name under the
//     owning run so a worker cannot pick a colliding or pre-scoped key.
//   - SpawnChildRun: launch a child run by publishing an
//     EventWorkflowSpawn. Routing through the existing spawn event (not a
//     fresh runs.start) is the load-bearing choice: lineage
//     (ParentRunID/ParentStepID), the MaxNestingDepth cap, and parent-step
//     linkage all come for free from the orchestrator's handleWorkflowSpawn
//     -> createChildRun path. We do NOT create a second, depth-unchecked
//     spawn path; instead we re-run the same depth check synchronously here
//     so the worker's reply carries a typed depth error.
//
// All business logic is transport-agnostic — the NATS framing lives in
// natsapi.go. Failures return a (kind, error) pair so the transport layer
// can echo a structured envelope the worker maps back to a typed error.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

// Wire kinds the control-plane endpoints emit. They mirror the worker's
// ControlPlaneErrorKind strings so the handle maps replies back without a
// translation table.
const (
	cpKindInvalidDef       = "invalid_def"
	cpKindNamespace        = "namespace"
	cpKindUnresolvableName = "unresolvable_name"
	cpKindTransport        = "transport"
	cpKindDepthExceeded    = "depth_exceeded"
	// Additive safety-limit kinds (#378). cpKindQuotaExceeded covers both
	// the active-run and the def quota; cpKindRateLimited covers the
	// register rate limit. Both mirror the worker ControlPlaneErrorKind
	// strings so the reply maps back without a translation table.
	cpKindQuotaExceeded = "quota_exceeded"
	cpKindRateLimited   = "rate_limited"
	// cpKindDenied is the authorization-denied kind (#380). Mirrors the
	// worker's KindDenied. Returned when a promotion is attempted by a
	// workflow not on the grant policy's promote list.
	cpKindDenied = "denied"
)

const (
	// Default per-runtime bounds (#378). NewServiceWithLimits resolves a
	// zero RuntimeLimits field to its default here, so a zero-value struct
	// inherits the safe caps rather than disabling them. The depth default
	// is engine.MaxNestingDepth — the existing cap is reused, not forked.
	// Exported so the server config layer asserts its own defaults match
	// (drift guard, #378) — the two must stay in lock-step.
	DefaultMaxActiveRunsPerRoot         = 100
	DefaultMaxDefsPerRoot               = 500
	DefaultMaxRegistersPerMinutePerRoot = 60

	// runtimeRunScanMax bounds the active-run quota scan (TigerStyle:
	// every loop has a fixed upper bound). A tree with more runs than this
	// is already far past any sane quota; the scan returns an error rather
	// than silently undercounting.
	runtimeRunScanMax = 10_000

	// runtimeDefScanMax symmetrically bounds the def quota scan. defKV.Keys
	// returns ALL defs (every tree's), so the bound is on the returned key
	// slice, not on tree-local defs. Exceeding it returns an error on the
	// request path (never a panic).
	runtimeDefScanMax = 100_000
)

// registerRatePeriod is the window the register rate limit refills over.
// One minute matches the per-minute config knob (MaxRegistersPerMinutePerRoot).
const registerRatePeriod = time.Minute

// scopeName is the single server-side source of truth for runtime
// workflow naming. root is the namespace root — the true tree-root run of
// the owning run's spawn lineage (resolveRootRunID derives it server-side
// from the worker-supplied owner run; #377). Keeping the formula in one
// function means the register endpoint and any future resolver agree by
// construction.
//
// The scoped name doubles as the workflow_defs KV key AND the
// child_workflow the spawn lookup resolves, so it MUST satisfy the NATS
// KV key regex `^[-/_=.a-zA-Z0-9]+$` (nats.go@v1.50.0 kv.go:369). ':' is
// NOT in that set, so the separator is '.' — a valid KV key and NATS
// token. The 'agent.' prefix keeps the namespace visible and lets
// validateRuntimeName reject any author name that tries to forge it.
func scopeName(root, name string) string {
	if name == "" {
		panic("scopeName: name must not be empty")
	}
	return scopePrefix(root) + name
}

// scopePrefix is the single source of truth for a tree-root's namespace
// prefix. Both scopeName (the register/spawn key formula) and
// countDefsForRoot (the def-quota scan) derive from it, so a change to the
// namespace shape touches exactly one place.
func scopePrefix(root string) string {
	if root == "" {
		panic("scopePrefix: root must not be empty")
	}
	return "agent." + root + "."
}

// resolveRuntimeLimits fills zero fields with the default consts so a
// zero-value RuntimeLimits inherits the safe caps. Negative values are a
// config-layer concern (server/config.go rejects them); this resolver only
// supplies defaults for the unset (zero) case.
//
// The generation-depth limit is CLAMPED to engine.MaxNestingDepth: the
// orchestrator's handleWorkflowSpawn enforces that ceiling unconditionally,
// so a higher API gate would pass a spawn the orchestrator silently drops —
// handing back a runID for a run never created (a ghost run). Clamping here
// is defense-in-depth alongside server/config.go's load-time rejection, so a
// programmatic caller of NewServiceWithLimits cannot open the hole either.
// The gate can tighten below the ceiling, never above it.
func resolveRuntimeLimits(in RuntimeLimits) RuntimeLimits {
	if in.MaxActiveRunsPerRoot == 0 {
		in.MaxActiveRunsPerRoot = DefaultMaxActiveRunsPerRoot
	}
	if in.MaxDefsPerRoot == 0 {
		in.MaxDefsPerRoot = DefaultMaxDefsPerRoot
	}
	if in.MaxGenerationDepth == 0 || in.MaxGenerationDepth > engine.MaxNestingDepth {
		in.MaxGenerationDepth = engine.MaxNestingDepth
	}
	if in.MaxRegistersPerMinutePerRoot == 0 {
		in.MaxRegistersPerMinutePerRoot = DefaultMaxRegistersPerMinutePerRoot
	}
	return in
}

// RegisterRuntimeWorkflow validates ownerRunID and def.Name, scopes the
// name server-side, runs the existing dag.Validate, and persists the def
// under the scoped key. Returns the scoped name on success. The returned
// kind is "" on success; otherwise it is one of the cpKind* strings.
//
// Grant-gate asymmetry (#380): the NON-promote register/spawn path carries
// NO server-side control-plane grant check. The grant is enforced UPSTREAM
// at the enqueue payload source — effectiveCapabilities strips the
// control-plane capability from an ungranted step, so the worker never
// receives a handle and never reaches this method. Adding a redundant grant
// check here would be belt-on-belt; do not "fix" the apparent gap. Only
// PROMOTION (which a granted worker requests explicitly) is authorized
// here, via authorizePromotion.
//
// When promote is true the def is registered under the reaper-immune
// "promoted.<name>" namespace (no agent. prefix → the ephemeral-def
// reaper's prefix gate never touches it). #377 wires the namespace only;
// authorization for promotion is deferred to #380.
// Named per operation (not after the shared dispatch check) so the span
// name and the method= metric label identify which control-plane
// operation ran, matching startRun/getRun/registerWorkflow.
func (s *Service) RegisterRuntimeWorkflow(
	ctx context.Context, def dag.WorkflowDef, ownerRunID string,
	promote bool,
) (string, string, error) {
	if ctx == nil {
		panic("RegisterRuntimeWorkflow: ctx must not be nil")
	}
	if s.defKV == nil {
		panic("RegisterRuntimeWorkflow: defKV must not be nil")
	}
	var scoped, kind string
	err := s.observed(ctx, "registerRuntimeWorkflow",
		[]attribute.KeyValue{
			attribute.String("run_id", ownerRunID),
			attribute.String("workflow", def.Name),
			attribute.Bool("promote", promote),
		},
		func(ctx context.Context) error {
			var innerErr error
			scoped, kind, innerErr = s.registerRuntimeWorkflow(
				ctx, def, ownerRunID, promote,
			)
			return innerErr
		},
	)
	return scoped, kind, err
}

// registerRuntimeWorkflow carries the register/promote work itself; see
// RegisterRuntimeWorkflow for the contract.
func (s *Service) registerRuntimeWorkflow(
	ctx context.Context, def dag.WorkflowDef, ownerRunID string,
	promote bool,
) (string, string, error) {
	if ctx == nil {
		panic("registerRuntimeWorkflow: ctx must not be nil")
	}
	if s.defKV == nil {
		panic("registerRuntimeWorkflow: defKV must not be nil")
	}
	if ownerRunID == "" {
		return "", cpKindNamespace,
			fmt.Errorf("owner run ID must not be empty")
	}
	if err := validateRuntimeName(def.Name); err != nil {
		return "", cpKindNamespace, err
	}
	scoped := promotedName(def.Name)
	if promote {
		// Promotion is now GOVERNED (#380): only a workflow on the grant
		// policy's promote list may promote. The check keys on the author
		// name (def.Name, already validated above) — the same name the grant
		// policy lists. Deny-by-default: a nil policy denies. This replaces
		// the #378 "ungoverned" tracked gap.
		if kind, err := s.authorizePromotion(def.Name); err != nil {
			s.emitRuntimeAudit(ctx, "runtime.promote", def.Name,
				"denied", ownerRunID, "not_authorized")
			return "", kind, err
		}
	} else {
		root, kind, err := s.resolveRootRunID(ctx, ownerRunID)
		if err != nil {
			return "", kind, err
		}
		// The #378 safety limits (def quota + register rate limit) are
		// applied here ONLY for root-scoped ephemeral defs.
		if kind, err := s.checkRegisterAdmission(ctx, root); err != nil {
			return "", kind, err
		}
		scoped = scopeName(root, def.Name)
	}
	// Persist under the scoped key, not the author-supplied name, so the
	// def is addressable only via the namespaced identity.
	def.Name = scoped
	if err := dag.Validate(def); err != nil {
		return "", cpKindInvalidDef,
			fmt.Errorf("invalid workflow: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return "", cpKindInvalidDef, err
	}
	if _, err := s.defKV.Put(ctx, scoped, data); err != nil {
		return "", cpKindTransport, err
	}
	action := "runtime.register"
	if promote {
		action = "runtime.promote"
	}
	s.emitRuntimeAudit(ctx, action, scoped, "success", ownerRunID, "")
	return scoped, "", nil
}

// emitRuntimeAudit writes one control-plane audit row best-effort (#380).
// Actor is "runtime:<ownerRunID>" — the run that acted. Errors are logged
// and swallowed: an audit gap must never fail the control-plane operation.
func (s *Service) emitRuntimeAudit(
	ctx context.Context, action, target, outcome,
	ownerRunID, reason string,
) {
	data := map[string]any{"owner_run_id": ownerRunID}
	if reason != "" {
		data["reason"] = reason
	}
	evt := auditkv.AuditEvent{
		Actor:   "runtime:" + ownerRunID,
		Action:  action,
		Target:  target,
		Data:    data,
		Outcome: outcome,
	}
	if err := auditkv.Emit(ctx, s.auditKV, s.auditLogger(), evt); err != nil {
		s.auditLogger().Warn("control-plane audit emit failed",
			"action", action, "error", err)
	}
}

// auditLogger returns the service logger or the default, never nil — the
// audit emitter panics on a nil logger.
func (s *Service) auditLogger() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

// authorizePromotion checks the grant policy's promote list for the author
// name (#380). Deny-by-default: a nil holder/policy denies. Returns
// ("", nil) when authorized; otherwise (cpKindDenied, error).
func (s *Service) authorizePromotion(authorName string) (string, error) {
	if authorName == "" {
		panic("authorizePromotion: authorName must not be empty")
	}
	// Holder.Load is nil-safe: an unwired holder → nil holder → nil policy →
	// AllowsPromote returns false. Every layer fails closed (deny-by-default).
	if s.grantPolicy.Load().AllowsPromote(authorName) {
		return "", nil
	}
	return cpKindDenied, fmt.Errorf(
		"workflow %q is not authorized to promote", authorName)
}

// VerifyDispatch proves the caller received the exact dispatch it claims:
// it loads the owner run and checks the echoed nonce equals the stamped
// StepState.DispatchNonce for ownerStepID, AND (defense-in-depth) that the
// owner run is non-terminal with that step Running. A sibling-run worker
// cannot forge another run's nonce — the nonce never left that dispatch.
// Returns ("", nil) on success; (cpKindNamespace, error) on any mismatch.
// An empty ownerStepID/nonce is a denial: a control-plane request must
// carry the run-binding proof (#380, Fix 1).
func (s *Service) VerifyDispatch(
	ctx context.Context, ownerRunID, ownerStepID, nonce string,
) (string, error) {
	if ctx == nil {
		panic("VerifyDispatch: ctx must not be nil")
	}
	if s.store == nil {
		panic("VerifyDispatch: store must not be nil")
	}
	if ownerRunID == "" || ownerStepID == "" || nonce == "" {
		return cpKindNamespace, fmt.Errorf(
			"dispatch proof required (run/step/nonce must be non-empty)")
	}
	run, err := s.store.Load(ctx, ownerRunID)
	if err != nil {
		return cpKindNamespace, fmt.Errorf(
			"verify dispatch: load owner run %q: %w", ownerRunID, err)
	}
	if run.Status.IsTerminal() {
		return cpKindNamespace, fmt.Errorf(
			"verify dispatch: owner run %q is terminal", ownerRunID)
	}
	state, ok := run.Steps[ownerStepID]
	if !ok {
		return cpKindNamespace, fmt.Errorf(
			"verify dispatch: owner step %q not found", ownerStepID)
	}
	// Defense-in-depth: the owning step must be in an active dispatch state.
	// Both Queued and Running are valid — a worker can call the control
	// plane after picking up the task but before the orchestrator has
	// processed its step.started event (an async race). A step in any other
	// state (Completed/Failed/Skipped) is not a live dispatch. The nonce
	// match below is the load-bearing binding; this is belt-and-suspenders.
	if state.Status != dag.StepStatusRunning &&
		state.Status != dag.StepStatusQueued {
		return cpKindNamespace, fmt.Errorf(
			"verify dispatch: owner step %q not in an active state",
			ownerStepID)
	}
	if state.DispatchNonce == "" || state.DispatchNonce != nonce {
		return cpKindNamespace, fmt.Errorf(
			"verify dispatch: nonce mismatch for run %q step %q",
			ownerRunID, ownerStepID)
	}
	return "", nil
}

// checkRegisterAdmission gates a root-scoped runtime register against the
// two safety limits, in cheapest-first order: the register rate limit
// (KV CAS, no scan) then the def quota (a bounded KV key scan). Returns
// ("", nil) when admitted; otherwise a (kind, error) pair the caller echoes
// in the typed reply. A rate-limiter transport failure maps to
// cpKindTransport, not a silent allow — the limiter is a safety control.
func (s *Service) checkRegisterAdmission(
	ctx context.Context, root string,
) (string, error) {
	if ctx == nil {
		panic("checkRegisterAdmission: ctx must not be nil")
	}
	if root == "" {
		panic("checkRegisterAdmission: root must not be empty")
	}
	allowed, _, err := s.registerLimiter.Allow(
		ctx, "runtime_register", root,
		s.limits.MaxRegistersPerMinutePerRoot, registerRatePeriod, 1,
	)
	if err != nil {
		return cpKindTransport,
			fmt.Errorf("register rate limit check: %w", err)
	}
	if !allowed {
		return cpKindRateLimited, fmt.Errorf(
			"register rate limit exceeded for root %q (%d/min)",
			root, s.limits.MaxRegistersPerMinutePerRoot)
	}
	count, err := s.countDefsForRoot(ctx, root)
	if err != nil {
		return cpKindTransport, fmt.Errorf("def quota check: %w", err)
	}
	if count >= s.limits.MaxDefsPerRoot {
		return cpKindQuotaExceeded, fmt.Errorf(
			"def quota exceeded for root %q (%d >= %d)",
			root, count, s.limits.MaxDefsPerRoot)
	}
	return "", nil
}

// resolveRootRunID derives the true tree-root run ID for ownerRunID by
// loading the owning run and applying engine.RootRunIDOf — the same root
// rule the orchestrator stamps at run creation. The worker sends only
// owner_run_id; the server walks to the root, so a worker cannot forge a
// namespace by claiming a different root (forge-proof).
//
// Ousterhout fix 4: a load miss here is a REAL error, not a race. The
// register handler runs inside a live owning run, so its snapshot must
// exist; a miss returns cpKindNamespace rather than silently self-rooting
// (which would mis-scope the def into an orphan namespace).
func (s *Service) resolveRootRunID(
	ctx context.Context, ownerRunID string,
) (string, string, error) {
	if ctx == nil {
		panic("resolveRootRunID: ctx must not be nil")
	}
	if ownerRunID == "" {
		panic("resolveRootRunID: ownerRunID must not be empty")
	}
	if s.store == nil {
		panic("resolveRootRunID: store must not be nil")
	}
	run, err := s.store.Load(ctx, ownerRunID)
	if err != nil {
		return "", cpKindNamespace,
			fmt.Errorf("resolve root for owner run %q: %w",
				ownerRunID, err)
	}
	return engine.RootRunIDOf(run), "", nil
}

// promotedName scopes a promoted def under the reaper-immune "promoted."
// namespace (#377). It carries NO agent. prefix, so the ephemeral-def
// reaper's prefix gate never selects it. Authorization for promotion is
// deferred to #380; this function only fixes the namespace shape.
func promotedName(name string) string {
	if name == "" {
		panic("promotedName: name must not be empty")
	}
	return "promoted." + name
}

// validateRuntimeName rejects names that would corrupt the scope key or
// forge a namespace. A name carrying the 'agent.' prefix, a ':' (the
// reserved logical separator workers also reject), or a '.' (the scope
// key separator — banning it keeps the author name an atomic last
// segment) would let a caller smuggle a pre-scoped or colliding identity
// past the server.
func validateRuntimeName(name string) error {
	if name == "" {
		return fmt.Errorf("workflow name must not be empty")
	}
	if strings.HasPrefix(name, "agent.") {
		return fmt.Errorf("workflow name must not carry the agent. prefix")
	}
	if strings.Contains(name, ":") {
		return fmt.Errorf("workflow name must not contain ':'")
	}
	if strings.Contains(name, ".") {
		return fmt.Errorf("workflow name must not contain '.'")
	}
	return nil
}

// SpawnChildRun launches a child run of childWorkflow under the given
// parent, returning the child run ID. It enforces the nesting-depth cap
// synchronously (the same check handleWorkflowSpawn applies async) so the
// worker's reply carries cpKindDepthExceeded instead of a silently
// dropped spawn. On success it publishes EventWorkflowSpawn and the
// orchestrator's existing path creates the child + links lineage.
// Named per operation (see RegisterRuntimeWorkflow) so a child spawn is
// identifiable as spawnChildRun in traces and metrics.
func (s *Service) SpawnChildRun(
	ctx context.Context,
	childWorkflow, parentRunID, parentStepID string,
	input []byte,
) (string, string, error) {
	if ctx == nil {
		panic("SpawnChildRun: ctx must not be nil")
	}
	var runID, kind string
	err := s.observed(ctx, "spawnChildRun",
		[]attribute.KeyValue{
			attribute.String("child_workflow", childWorkflow),
			attribute.String("parent_run_id", parentRunID),
			attribute.String("parent_step_id", parentStepID),
		},
		func(ctx context.Context) error {
			var innerErr error
			runID, kind, innerErr = s.spawnChildRun(
				ctx, childWorkflow, parentRunID, parentStepID, input,
			)
			return innerErr
		},
	)
	return runID, kind, err
}

// spawnChildRun carries the spawn work itself; see SpawnChildRun for the
// contract.
func (s *Service) spawnChildRun(
	ctx context.Context,
	childWorkflow, parentRunID, parentStepID string,
	input []byte,
) (string, string, error) {
	if ctx == nil {
		panic("spawnChildRun: ctx must not be nil")
	}
	// An empty parent run is a lineage/namespace invariant violation, not
	// an unresolvable workflow name — cpKindUnresolvableName is reserved
	// for the defKV.Get miss below.
	if parentRunID == "" {
		return "", cpKindNamespace,
			fmt.Errorf("parent run ID must not be empty")
	}
	if childWorkflow == "" {
		return "", cpKindUnresolvableName,
			fmt.Errorf("child workflow must not be empty")
	}
	if _, err := s.defKV.Get(ctx, childWorkflow); err != nil {
		return "", cpKindUnresolvableName,
			fmt.Errorf("child workflow %q not found", childWorkflow)
	}
	if s.spawnDepthExceeded(ctx, parentRunID) {
		return "", cpKindDepthExceeded,
			fmt.Errorf("max nesting depth %d exceeded",
				s.limits.MaxGenerationDepth)
	}
	// Active-run quota is best-effort (count-then-act TOCTOU): a small
	// concurrent burst may exceed by the in-flight count. The cap bounds
	// runaway generation, not exact occupancy. Resolve the spawn's tree
	// root from the parent before counting so the quota is per-tree.
	root, kind, err := s.resolveRootRunID(ctx, parentRunID)
	if err != nil {
		return "", kind, err
	}
	active, err := s.countActiveRunsForRoot(ctx, root)
	if err != nil {
		return "", cpKindTransport,
			fmt.Errorf("active-run quota check: %w", err)
	}
	if active >= s.limits.MaxActiveRunsPerRoot {
		return "", cpKindQuotaExceeded, fmt.Errorf(
			"active-run quota exceeded for root %q (%d >= %d)",
			root, active, s.limits.MaxActiveRunsPerRoot)
	}
	childRunID := runid.New()
	if err := s.publishSpawn(
		ctx, childWorkflow, parentRunID, parentStepID, input, childRunID,
	); err != nil {
		return "", cpKindTransport, err
	}
	s.emitRuntimeAudit(ctx, "runtime.spawn", childWorkflow,
		"success", parentRunID, "")
	return childRunID, "", nil
}

// spawnDepthExceeded reports whether a child of parentRunID would breach
// the configured generation-depth cap. The bound is s.limits.MaxGenerationDepth
// (default engine.MaxNestingDepth) — the SAME mechanism the orchestrator
// applies, made configurable rather than forked into a second cap (#378 D1).
// A store-load error breaks the walk (treated as the chain root), matching
// the orchestrator's behavior of stopping the walk on error.
func (s *Service) spawnDepthExceeded(
	ctx context.Context, parentRunID string,
) bool {
	if s.store == nil {
		panic("spawnDepthExceeded: store must not be nil")
	}
	maxDepth := s.limits.MaxGenerationDepth
	if maxDepth <= 0 {
		panic("spawnDepthExceeded: MaxGenerationDepth must be positive")
	}
	depth := 0
	currentID := parentRunID
	for i := 0; i < maxDepth+1; i++ {
		run, err := s.store.Load(ctx, currentID)
		if err != nil || run.ParentRunID == "" {
			break
		}
		depth++
		currentID = run.ParentRunID
	}
	return depth+1 >= maxDepth
}

// countActiveRunsForRoot returns the number of non-terminal runs sharing
// root's spawn tree, via a single bounded ListAll scan (D2: no separate KV
// counter — a counter would drift from truth and spread complexity). The
// count is a point-in-time read; the quota that consumes it is best-effort
// (see SpawnChildRun). ErrNoKeysFound resolves to 0.
func (s *Service) countActiveRunsForRoot(
	ctx context.Context, root string,
) (int, error) {
	if ctx == nil {
		panic("countActiveRunsForRoot: ctx must not be nil")
	}
	if root == "" {
		panic("countActiveRunsForRoot: root must not be empty")
	}
	runs, err := s.store.ListAll(ctx, runtimeRunScanMax)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return 0, nil
		}
		return 0, err
	}
	count := 0
	for i := range runs {
		if engine.RootRunIDOf(runs[i]) == root &&
			!runs[i].Status.IsTerminal() {
			count++
		}
	}
	return count, nil
}

// countDefsForRoot returns the number of ephemeral defs registered under
// root's namespace, via a bounded defKV.Keys scan (D2). The bound is on the
// returned key slice (Keys returns every tree's defs) and symmetric with the
// run scan: a slice larger than runtimeDefScanMax returns an error on the
// request path rather than panicking. ErrNoKeysFound resolves to 0.
func (s *Service) countDefsForRoot(
	ctx context.Context, root string,
) (int, error) {
	if ctx == nil {
		panic("countDefsForRoot: ctx must not be nil")
	}
	if root == "" {
		panic("countDefsForRoot: root must not be empty")
	}
	keys, err := s.defKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(keys) > runtimeDefScanMax {
		return 0, fmt.Errorf(
			"def key scan exceeded bound (%d > %d)",
			len(keys), runtimeDefScanMax)
	}
	prefix := scopePrefix(root)
	count := 0
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count, nil
}

// Budget reports the current-vs-max for the two quota dimensions a runtime
// can hit, computed by the SAME scan code that enforces them (so the number
// an agent reads matches the number the gate checks). Lets the durable agent
// loop self-throttle before tripping a quota_exceeded reply. Token/compute
// metering is deferred (#378 P3) — those fields are intentionally absent.
// Returns the typed kind on resolve/scan failure, "" on success.
// Named per operation (see RegisterRuntimeWorkflow) so a budget read is
// identifiable as budget in traces and metrics.
func (s *Service) Budget(
	ctx context.Context, ownerRunID string,
) (worker.RuntimeBudget, string, error) {
	if ctx == nil {
		panic("Budget: ctx must not be nil")
	}
	var budget worker.RuntimeBudget
	var kind string
	err := s.observed(ctx, "budget",
		[]attribute.KeyValue{
			attribute.String("run_id", ownerRunID),
		},
		func(ctx context.Context) error {
			var innerErr error
			budget, kind, innerErr = s.budget(ctx, ownerRunID)
			return innerErr
		},
	)
	return budget, kind, err
}

// budget carries the budget scan itself; see Budget for the contract.
func (s *Service) budget(
	ctx context.Context, ownerRunID string,
) (worker.RuntimeBudget, string, error) {
	if ctx == nil {
		panic("budget: ctx must not be nil")
	}
	if ownerRunID == "" {
		return worker.RuntimeBudget{}, cpKindNamespace,
			fmt.Errorf("owner run ID must not be empty")
	}
	root, kind, err := s.resolveRootRunID(ctx, ownerRunID)
	if err != nil {
		return worker.RuntimeBudget{}, kind, err
	}
	active, err := s.countActiveRunsForRoot(ctx, root)
	if err != nil {
		return worker.RuntimeBudget{}, cpKindTransport, err
	}
	defs, err := s.countDefsForRoot(ctx, root)
	if err != nil {
		return worker.RuntimeBudget{}, cpKindTransport, err
	}
	return worker.RuntimeBudget{
		ActiveRuns:        active,
		MaxActiveRuns:     s.limits.MaxActiveRunsPerRoot,
		RegisteredDefs:    defs,
		MaxRegisteredDefs: s.limits.MaxDefsPerRoot,
	}, "", nil
}

// BudgetsForRoots computes every tree-root's RuntimeBudget from the run
// snapshots the CALLER already holds plus ONE def-key scan — the batched
// counterpart to the per-root Budget. The /console/agents list page loaded
// runs once, then paid a per-root Budget (a FULL run scan among its three
// store ops) for each root: O(roots x runs). Deriving ActiveRuns in memory
// from `runs` and folding all defs in one scan makes the page O(runs +
// defkeys). Honesty is preserved: real active counts (from runs), real def
// counts (from defKV), real limits — no fabrication. Returns a non-nil map
// keyed by root; a root absent from `runs` is simply absent.
func (s *Service) BudgetsForRoots(
	ctx context.Context, runs []dag.WorkflowRun,
) (map[string]worker.RuntimeBudget, error) {
	if ctx == nil {
		panic("BudgetsForRoots: ctx must not be nil")
	}
	if s.defKV == nil {
		panic("BudgetsForRoots: defKV must not be nil")
	}
	active := make(map[string]int, len(runs))
	for i := range runs {
		root := engine.RootRunIDOf(runs[i])
		delta := 0
		if !runs[i].Status.IsTerminal() {
			delta = 1
		}
		// Assigning active[root]+delta materializes the root key even when
		// every run under it is terminal (delta 0), so an all-terminal tree
		// still reports a budget rather than vanishing from the map.
		active[root] = active[root] + delta
	}
	defs, err := s.defCountsByRoot(ctx, active)
	if err != nil {
		return nil, err
	}
	out := make(map[string]worker.RuntimeBudget, len(active))
	for root, activeCount := range active {
		out[root] = worker.RuntimeBudget{
			ActiveRuns:        activeCount,
			MaxActiveRuns:     s.limits.MaxActiveRunsPerRoot,
			RegisteredDefs:    defs[root],
			MaxRegisteredDefs: s.limits.MaxDefsPerRoot,
		}
	}
	return out, nil
}

// defCountsByRoot folds the whole def-key population into a per-root count via
// ONE bounded defKV.Keys scan (mirroring countDefsForRoot's bound and
// ErrNoKeysFound tolerance). Each key is parsed to its owning root in a single
// O(keys) pass; a def is counted only when its root is in `roots` (a def whose
// tree has no run in the current page is ignored — it wouldn't render anyway).
func (s *Service) defCountsByRoot(
	ctx context.Context, roots map[string]int,
) (map[string]int, error) {
	if ctx == nil {
		panic("defCountsByRoot: ctx must not be nil")
	}
	counts := make(map[string]int, len(roots))
	keys, err := s.defKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return counts, nil
		}
		return nil, err
	}
	if len(keys) > runtimeDefScanMax {
		return nil, fmt.Errorf(
			"def key scan exceeded bound (%d > %d)",
			len(keys), runtimeDefScanMax)
	}
	for _, key := range keys {
		root, ok := rootFromScopedKey(key)
		if !ok {
			continue
		}
		if _, known := roots[root]; known {
			counts[root]++
		}
	}
	return counts, nil
}

// rootFromScopedKey inverts scopePrefix: a def key "agent.<root>.<name>" yields
// (root, true). Any key not carrying the scope shape yields ("", false). Keeps
// the namespace shape a single source of truth alongside scopePrefix.
func rootFromScopedKey(key string) (string, bool) {
	const scope = "agent."
	rest, found := strings.CutPrefix(key, scope)
	if !found {
		return "", false
	}
	dot := strings.IndexByte(rest, '.')
	if dot <= 0 {
		return "", false
	}
	return rest[:dot], true
}

// publishSpawn emits the EventWorkflowSpawn the orchestrator consumes.
// The payload keys match handleWorkflowSpawn's struct exactly; detach is
// always false so lineage links. childRunID is server-generated so the
// orchestrator's child_run_id invariant holds.
//
// Load-bearing for the runtime path: unlike a sub_workflow step, the
// parentStepID here belongs to a NORMAL task step that already completed
// (the gated handler called ctx.Complete after StartRun). The child's
// EventWorkflowChildCompleted is therefore safely absorbed by
// handleChildCompleted's `state.Status != Running` guard
// (orchestrator.go ~3039) — the parent step is intentionally NEVER
// re-advanced by the child. A future refactor of handleChildCompleted
// must preserve that guard or it would silently break this path.
func (s *Service) publishSpawn(
	ctx context.Context,
	childWorkflow, parentRunID, parentStepID string,
	input []byte, childRunID string,
) error {
	if parentStepID == "" {
		// NewStepEvent panics on empty stepID; a runtime spawn always
		// originates from a step, but guard so a malformed request fails
		// as a typed transport error rather than panicking the handler.
		return fmt.Errorf("parent step ID must not be empty")
	}
	payload, err := json.Marshal(map[string]interface{}{
		"child_run_id":   childRunID,
		"child_workflow": childWorkflow,
		"parent_step_id": parentStepID,
		"input":          json.RawMessage(input),
		"detach":         false,
	})
	if err != nil {
		return fmt.Errorf("marshal spawn payload: %w", err)
	}
	evt := protocol.NewStepEvent(
		protocol.EventWorkflowSpawn, parentRunID, parentStepID, payload,
	)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	if _, err := s.tp.JSPublishMsgEvent(ctx, msg, &evt); err != nil {
		return fmt.Errorf("publish spawn event: %w", err)
	}
	return nil
}

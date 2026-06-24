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
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
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
)

// scopeName is the single server-side source of truth for runtime
// workflow naming. root is the namespace root — currently the owning run
// ID; #377 will swap this for the true tree-root run without changing the
// shape callers see. Keeping the formula in one function means the
// register endpoint and any future resolver agree by construction.
//
// The scoped name doubles as the workflow_defs KV key AND the
// child_workflow the spawn lookup resolves, so it MUST satisfy the NATS
// KV key regex `^[-/_=.a-zA-Z0-9]+$` (nats.go@v1.50.0 kv.go:369). ':' is
// NOT in that set, so the separator is '.' — a valid KV key and NATS
// token. The 'agent.' prefix keeps the namespace visible and lets
// validateRuntimeName reject any author name that tries to forge it.
func scopeName(root, name string) string {
	if root == "" {
		panic("scopeName: root must not be empty")
	}
	if name == "" {
		panic("scopeName: name must not be empty")
	}
	return "agent." + root + "." + name
}

// RegisterRuntimeWorkflow validates ownerRunID and def.Name, scopes the
// name server-side, runs the existing dag.Validate, and persists the def
// under the scoped key. Returns the scoped name on success. The returned
// kind is "" on success; otherwise it is one of the cpKind* strings.
func (s *Service) RegisterRuntimeWorkflow(
	ctx context.Context, def dag.WorkflowDef, ownerRunID string,
) (string, string, error) {
	if ctx == nil {
		panic("RegisterRuntimeWorkflow: ctx must not be nil")
	}
	if s.defKV == nil {
		panic("RegisterRuntimeWorkflow: defKV must not be nil")
	}
	if ownerRunID == "" {
		return "", cpKindNamespace,
			fmt.Errorf("owner run ID must not be empty")
	}
	if err := validateRuntimeName(def.Name); err != nil {
		return "", cpKindNamespace, err
	}
	scoped := scopeName(ownerRunID, def.Name)
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
	return scoped, "", nil
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
func (s *Service) SpawnChildRun(
	ctx context.Context,
	childWorkflow, parentRunID, parentStepID string,
	input []byte,
) (string, string, error) {
	if ctx == nil {
		panic("SpawnChildRun: ctx must not be nil")
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
				engine.MaxNestingDepth)
	}
	childRunID := runid.New()
	if err := s.publishSpawn(
		ctx, childWorkflow, parentRunID, parentStepID, input, childRunID,
	); err != nil {
		return "", cpKindTransport, err
	}
	return childRunID, "", nil
}

// spawnDepthExceeded reports whether a child of parentRunID would breach
// the cap. Mirrors orchestrator.nestingDepth + its depth+1 >= max test so
// both spawn entrypoints reject at the identical boundary. A store-load
// error breaks the walk (treated as the chain root), matching the
// orchestrator's behavior of stopping the walk on error.
func (s *Service) spawnDepthExceeded(
	ctx context.Context, parentRunID string,
) bool {
	if s.store == nil {
		panic("spawnDepthExceeded: store must not be nil")
	}
	depth := 0
	currentID := parentRunID
	for i := 0; i < engine.MaxNestingDepth+1; i++ {
		run, err := s.store.Load(ctx, currentID)
		if err != nil || run.ParentRunID == "" {
			break
		}
		depth++
		currentID = run.ParentRunID
	}
	return depth+1 >= engine.MaxNestingDepth
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

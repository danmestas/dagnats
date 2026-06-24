// worker/controlplane.go
// ControlPlane is the worker-side handle a GATED task handler uses to
// author and launch workflows at runtime. It is a DEEP module: the two
// methods below hide def validation, server-side namespacing, run
// lineage, the maxNestingDepth cap, and every NATS round-trip. Handlers
// never see subjects, KV keys, or wire framing — they hold a small
// interface and get back a scoped name or a run ID, or a structured
// typed error they can branch on.
//
// Why worker-side handle over the api micro.Service (not engine logic):
// the validated control-plane boundary already lives on the
// dagnats-api micro.Service (#456). Duplicating register/spawn logic in
// the worker would fork the namespace/lineage/depth rules. Instead the
// handle speaks NATS request/reply to two additive subjects and lets the
// server stay the single source of truth.
//
// Every boundary failure returns *ControlPlaneError so the durable agent
// loop (ADR-002) can self-correct instead of crashing. Panics are
// reserved for programmer errors (nil ctx, nil receiver, empty
// ownerRunID) — never for agent-supplied data.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
)

// ControlPlane lets a gated handler author an ephemeral workflow def at
// runtime and launch a child run of it. nil unless the step declared the
// "control-plane" capability AND the deployment granted it — always
// nil-check before use.
//
// Adding a method here EXTENDS the public interface: any external
// implementation (mock or alternate handle) must add it too. Budget() was
// added in #378; there are no external implementations in-repo, so this is
// source-compatible here, but downstream embedders must update their mocks.
type ControlPlane interface {
	// RegisterWorkflow validates def and persists it under a
	// server-computed scoped name, returning that name. opts.Promote is
	// WIRED (#377): true registers under the reaper-immune "promoted.*"
	// namespace. Promotion is GOVERNED (#380): the server authorizes it
	// against the grant policy's promote list (keyed on the author name) and
	// returns KindDenied when the workflow is not authorized. Promoted defs
	// still bypass the #378 root-scoped quota/rate limits (they have no
	// owning tree). The returned scopedName is what StartRun expects.
	RegisterWorkflow(
		ctx context.Context, def dag.WorkflowDef, opts RegisterOpts,
	) (scopedName string, err error)

	// StartRun launches a child run of the named (scoped) workflow with
	// the given input, returning the child run ID. Lineage and the
	// nesting-depth cap are enforced server-side.
	StartRun(
		ctx context.Context, name string, input []byte,
	) (runID string, err error)

	// Budget reports the owning tree's current-vs-max for the two quota
	// dimensions (active runs, registered defs), computed by the same
	// server-side scan that enforces the quotas. A gated handler reads it
	// to self-throttle before hitting a KindQuotaExceeded reply (#378).
	Budget(ctx context.Context) (RuntimeBudget, error)
}

// RuntimeBudget is the server-computed snapshot of a spawn tree's quota
// usage (#378). It carries real, scan-backed numbers — not a stub. Token /
// compute metering is deferred (#378 P3), so no such field exists here; a
// zero-valued field would lie about being tracked.
type RuntimeBudget struct {
	ActiveRuns        int `json:"active_runs"`
	MaxActiveRuns     int `json:"max_active_runs"`
	RegisteredDefs    int `json:"registered_defs"`
	MaxRegisteredDefs int `json:"max_registered_defs"`
}

// RegisterOpts carries optional knobs for RegisterWorkflow. Promote
// requests the def be registered under the reaper-immune "promoted.*"
// namespace instead of the ephemeral "agent.<root>.*" namespace (#377).
// The worker forwards the flag; the server owns the namespace shape.
// Authorization for promotion is GOVERNED by the grant policy's promote
// list (#380): an unauthorized caller gets KindDenied. Promoted defs remain
// outside the #378 root-scoped quota / rate limits (those bound only
// root-scoped ephemeral defs).
type RegisterOpts struct {
	Promote bool
}

// ControlPlaneErrorKind is the small, closed set of failure categories a
// gated handler may branch on. Stable strings so they can also travel on
// the wire between the server endpoints and the worker handle.
type ControlPlaneErrorKind string

const (
	KindInvalidDef           ControlPlaneErrorKind = "invalid_def"
	KindNamespace            ControlPlaneErrorKind = "namespace"
	KindUnresolvableName     ControlPlaneErrorKind = "unresolvable_name"
	KindPromotionUnsupported ControlPlaneErrorKind = "promotion_unsupported"
	KindTransport            ControlPlaneErrorKind = "transport"
	KindDenied               ControlPlaneErrorKind = "denied"
	KindDepthExceeded        ControlPlaneErrorKind = "depth_exceeded"
	// Additive safety-limit kinds (#378). KindQuotaExceeded covers both the
	// active-run and the def quota; KindRateLimited covers the register
	// rate limit. A gated handler branches on these to back off.
	KindQuotaExceeded ControlPlaneErrorKind = "quota_exceeded"
	KindRateLimited   ControlPlaneErrorKind = "rate_limited"
)

// ControlPlaneError is the single structured error type every boundary
// failure returns. Kind is the branch key; Op names the failing
// operation; Message is human-readable; wrapped carries any underlying
// cause for errors.Is/As against the sentinels below.
type ControlPlaneError struct {
	Kind    ControlPlaneErrorKind
	Op      string
	Message string
	wrapped error
}

func (e *ControlPlaneError) Error() string {
	if e.Message == "" {
		return e.Op + ": " + string(e.Kind)
	}
	return e.Op + ": " + string(e.Kind) + ": " + e.Message
}

func (e *ControlPlaneError) Unwrap() error { return e.wrapped }

// Sentinels for errors.Is. Each wraps its Kind so a freshly constructed
// *ControlPlaneError of the same Kind matches via the Is method below.
var (
	ErrPromotionUnsupported = &ControlPlaneError{
		Kind: KindPromotionUnsupported, Op: "RegisterWorkflow",
		Message: "promotion is not supported in Tier 1 (deferred to #378)",
	}
	ErrInvalidDef       = &ControlPlaneError{Kind: KindInvalidDef}
	ErrNamespace        = &ControlPlaneError{Kind: KindNamespace}
	ErrUnresolvableName = &ControlPlaneError{Kind: KindUnresolvableName}
	ErrTransport        = &ControlPlaneError{Kind: KindTransport}
	ErrDenied           = &ControlPlaneError{Kind: KindDenied}
	ErrDepthExceeded    = &ControlPlaneError{Kind: KindDepthExceeded}
	ErrQuotaExceeded    = &ControlPlaneError{Kind: KindQuotaExceeded}
	ErrRateLimited      = &ControlPlaneError{Kind: KindRateLimited}
)

// Is lets errors.Is match by Kind, so callers can write
// errors.Is(err, ErrPromotionUnsupported) regardless of the concrete
// instance returned.
func (e *ControlPlaneError) Is(target error) bool {
	var t *ControlPlaneError
	if !errors.As(target, &t) {
		return false
	}
	return e.Kind == t.Kind
}

// Subjects for the two additive endpoints on the dagnats-api micro
// service. Additive: existing api.workflows.register / api.runs.start /
// api.runs.get are untouched.
const (
	subjectRuntimesRegister = "api.runtimes.register"
	subjectRunsSpawn        = "api.runs.spawn"
	subjectRuntimesBudget   = "api.runtimes.budget"
)

// requestTimeout bounds every control-plane round-trip. A handler that
// hangs on a missing server must fail fast as KindTransport, not block.
const requestTimeout = 5 * time.Second

// nameMaxLength bounds the workflow name the handler may register or
// start. Over-long names are a namespace error (rejected before any
// request reaches the server).
const nameMaxLength = 256

// runtimeRegisterRequest is the wire request for api.runtimes.register.
// OwnerRunID is the namespace root; the server (not the worker) rewrites
// def.Name into the scoped key.
type runtimeRegisterRequest struct {
	Def        dag.WorkflowDef `json:"def"`
	OwnerRunID string          `json:"owner_run_id"`
	Promote    bool            `json:"promote"`
	// OwnerStepID + Nonce carry the per-dispatch run-binding proof (#380):
	// the server indexes the owner run's step by OwnerStepID and verifies
	// Nonce equals the stamped StepState.DispatchNonce. Additive, omitempty.
	OwnerStepID string `json:"owner_step_id,omitempty"`
	Nonce       string `json:"nonce,omitempty"`
}

// runtimeRegisterReply is the wire reply for api.runtimes.register.
type runtimeRegisterReply struct {
	ScopedName string                `json:"scoped_name,omitempty"`
	Error      string                `json:"error,omitempty"`
	Kind       ControlPlaneErrorKind `json:"kind,omitempty"`
}

// runSpawnRequest is the wire request for api.runs.spawn.
type runSpawnRequest struct {
	ChildWorkflow string          `json:"child_workflow"`
	ParentRunID   string          `json:"parent_run_id"`
	ParentStepID  string          `json:"parent_step_id"`
	Input         json.RawMessage `json:"input,omitempty"`
	// Nonce carries the per-dispatch run-binding proof (#380): the server
	// verifies it against the parent run's ParentStepID DispatchNonce.
	// Additive, omitempty.
	Nonce string `json:"nonce,omitempty"`
}

// runSpawnReply is the wire reply for api.runs.spawn.
type runSpawnReply struct {
	RunID string                `json:"run_id,omitempty"`
	Error string                `json:"error,omitempty"`
	Kind  ControlPlaneErrorKind `json:"kind,omitempty"`
}

// runtimeBudgetRequest is the wire request for api.runtimes.budget. The
// server resolves the tree root from owner_run_id before scanning.
type runtimeBudgetRequest struct {
	OwnerRunID string `json:"owner_run_id"`
	// OwnerStepID + Nonce carry the per-dispatch run-binding proof (#380),
	// so even a read-only budget query proves it came from this dispatch.
	// Additive, omitempty.
	OwnerStepID string `json:"owner_step_id,omitempty"`
	Nonce       string `json:"nonce,omitempty"`
}

// runtimeBudgetReply is the wire reply for api.runtimes.budget: the budget
// snapshot inline plus the standard {error, kind} envelope.
type runtimeBudgetReply struct {
	RuntimeBudget
	Error string                `json:"error,omitempty"`
	Kind  ControlPlaneErrorKind `json:"kind,omitempty"`
}

// workerControlPlane is the production ControlPlane: a thin NATS
// request/reply client scoped to one owning run + step. dispatchNonce is
// the per-dispatch run-binding token (#380) the worker received on the
// TaskPayload; it travels on every control-plane request so the server can
// verify the caller actually received this dispatch (a sibling-run worker
// cannot forge another run's nonce). It flows internally via
// TaskPayload→handle→wire — the public ControlPlane interface is unchanged.
type workerControlPlane struct {
	nc            *nats.Conn
	ownerRunID    string
	stepID        string
	dispatchNonce string
}

// NewControlPlane constructs a ControlPlane bound to nc. The owning run
// and step are bound later, at grant time, via newControlPlaneFor — this
// public constructor is what a deployment wires through WithControlPlane.
// Panics if nc is nil (programmer error at startup).
func NewControlPlane(nc *nats.Conn) ControlPlane {
	if nc == nil {
		panic("NewControlPlane: nc must not be nil")
	}
	// The owner run/step are filled per-dispatch by newControlPlaneFor;
	// the deployment-wired handle only carries the connection.
	return &workerControlPlane{nc: nc}
}

// newControlPlaneFor binds a per-dispatch handle to the owning run and
// step. Called at the gated construction site in startTaskSpan. Panics on
// nil nc or empty ownerRunID — both are programmer errors (the gate only
// fires with a non-nil worker connection and a populated payload).
func newControlPlaneFor(
	nc *nats.Conn, ownerRunID, stepID, dispatchNonce string,
) *workerControlPlane {
	if nc == nil {
		panic("newControlPlaneFor: nc must not be nil")
	}
	if ownerRunID == "" {
		panic("newControlPlaneFor: ownerRunID must not be empty")
	}
	return &workerControlPlane{
		nc: nc, ownerRunID: ownerRunID, stepID: stepID,
		dispatchNonce: dispatchNonce,
	}
}

// RegisterWorkflow validates locally-checkable invariants, then hands the
// def to the server which owns scoping + dag.Validate + the KV write.
func (c *workerControlPlane) RegisterWorkflow(
	ctx context.Context, def dag.WorkflowDef, opts RegisterOpts,
) (string, error) {
	if ctx == nil {
		panic("RegisterWorkflow: ctx must not be nil")
	}
	if c == nil || c.nc == nil {
		panic("RegisterWorkflow: receiver must not be nil")
	}
	if err := validateAuthorName(def.Name); err != nil {
		return "", err
	}
	req := runtimeRegisterRequest{
		Def: def, OwnerRunID: c.ownerRunID, Promote: opts.Promote,
		OwnerStepID: c.stepID, Nonce: c.dispatchNonce,
	}
	reply, err := requestAPI[runtimeRegisterReply](
		ctx, c.nc, subjectRuntimesRegister, req, "RegisterWorkflow",
	)
	if err != nil {
		return "", err
	}
	if reply.Error != "" {
		return "", &ControlPlaneError{
			Kind: orDefault(reply.Kind, KindInvalidDef),
			Op:   "RegisterWorkflow", Message: reply.Error,
		}
	}
	return reply.ScopedName, nil
}

// StartRun launches a child run of the scoped workflow. Lineage and the
// nesting-depth cap are enforced by the server via the spawn-event path.
func (c *workerControlPlane) StartRun(
	ctx context.Context, name string, input []byte,
) (string, error) {
	if ctx == nil {
		panic("StartRun: ctx must not be nil")
	}
	if c == nil || c.nc == nil {
		panic("StartRun: receiver must not be nil")
	}
	if err := validateScopedName(name); err != nil {
		return "", err
	}
	req := runSpawnRequest{
		ChildWorkflow: name,
		ParentRunID:   c.ownerRunID,
		ParentStepID:  c.stepID,
		Input:         json.RawMessage(input),
		Nonce:         c.dispatchNonce,
	}
	reply, err := requestAPI[runSpawnReply](
		ctx, c.nc, subjectRunsSpawn, req, "StartRun",
	)
	if err != nil {
		return "", err
	}
	if reply.Error != "" {
		return "", &ControlPlaneError{
			Kind: orDefault(reply.Kind, KindUnresolvableName),
			Op:   "StartRun", Message: reply.Error,
		}
	}
	return reply.RunID, nil
}

// Budget asks the server for the owning tree's quota usage. The owner run
// is bound at grant time; a handler only needs the snapshot. A reply
// envelope error maps to a typed *ControlPlaneError (KindTransport by
// default when the server omits a kind).
func (c *workerControlPlane) Budget(
	ctx context.Context,
) (RuntimeBudget, error) {
	if ctx == nil {
		panic("Budget: ctx must not be nil")
	}
	if c == nil || c.nc == nil {
		panic("Budget: receiver must not be nil")
	}
	req := runtimeBudgetRequest{
		OwnerRunID: c.ownerRunID, OwnerStepID: c.stepID,
		Nonce: c.dispatchNonce,
	}
	reply, err := requestAPI[runtimeBudgetReply](
		ctx, c.nc, subjectRuntimesBudget, req, "Budget",
	)
	if err != nil {
		return RuntimeBudget{}, err
	}
	if reply.Error != "" {
		return RuntimeBudget{}, &ControlPlaneError{
			Kind: orDefault(reply.Kind, KindTransport),
			Op:   "Budget", Message: reply.Error,
		}
	}
	return reply.RuntimeBudget, nil
}

// validateScopedName guards the StartRun name argument: a name that is
// empty, over the bound, or carries the reserved ':' separator before any
// request goes out. The name here is the SCOPED output of RegisterWorkflow
// (e.g. "agent.<run>.<name>"), so '.' and the "agent." prefix are
// EXPECTED — they are not rejected. The ':' check keeps a caller from
// smuggling a subject-style name past the server.
func validateScopedName(name string) error {
	if name == "" {
		return namespaceErr("validateScopedName", "name must not be empty")
	}
	if len(name) > nameMaxLength {
		return namespaceErr("validateScopedName", "name exceeds max length")
	}
	if strings.Contains(name, ":") {
		return namespaceErr("validateScopedName", "name must not contain ':'")
	}
	return nil
}

// validateAuthorName guards the RegisterWorkflow def.Name: the AUTHOR name
// the server will scope. It mirrors the server's validateRuntimeName
// (runtimes.go) — rejecting ':' plus the scope separator '.' and the
// "agent." prefix — so a forge attempt (e.g. def.Name =
// "agent.other-run.steal") fails fast client-side with a clean typed
// error instead of wasting a round-trip. The server still enforces; the
// client mirror removes an asymmetry that would be a bug-magnet.
func validateAuthorName(name string) error {
	if name == "" {
		return namespaceErr("validateAuthorName", "name must not be empty")
	}
	if len(name) > nameMaxLength {
		return namespaceErr("validateAuthorName", "name exceeds max length")
	}
	if strings.Contains(name, ":") {
		return namespaceErr("validateAuthorName", "name must not contain ':'")
	}
	if strings.HasPrefix(name, "agent.") {
		return namespaceErr("validateAuthorName",
			"name must not carry the agent. prefix")
	}
	if strings.Contains(name, ".") {
		return namespaceErr("validateAuthorName", "name must not contain '.'")
	}
	return nil
}

// namespaceErr builds a KindNamespace *ControlPlaneError for the name
// validators, keeping each guard one line.
func namespaceErr(op, message string) *ControlPlaneError {
	return &ControlPlaneError{Kind: KindNamespace, Op: op, Message: message}
}

// requestAPI performs one bounded NATS request and unmarshals the reply.
// A request error (no responder, timeout) maps to KindTransport; a
// malformed reply maps to KindTransport too. The reply's own error/kind
// envelope is left for the caller to interpret.
func requestAPI[T any](
	ctx context.Context, nc *nats.Conn,
	subject string, req any, op string,
) (T, error) {
	var zero T
	data, err := json.Marshal(req)
	if err != nil {
		return zero, &ControlPlaneError{
			Kind: KindTransport, Op: op,
			Message: "marshal request", wrapped: err,
		}
	}
	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	msg, err := nc.RequestWithContext(reqCtx, subject, data)
	if err != nil {
		return zero, &ControlPlaneError{
			Kind: KindTransport, Op: op,
			Message: "request " + subject, wrapped: err,
		}
	}
	var reply T
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return zero, &ControlPlaneError{
			Kind: KindTransport, Op: op,
			Message: "unmarshal reply", wrapped: err,
		}
	}
	return reply, nil
}

// orDefault returns kind when set, else fallback. A server reply that
// reports an error without a kind still maps to a sensible category.
func orDefault(
	kind, fallback ControlPlaneErrorKind,
) ControlPlaneErrorKind {
	if kind == "" {
		return fallback
	}
	return kind
}

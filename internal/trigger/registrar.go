package trigger

import "context"

// TriggerRegistrar owns the lifecycle of one trigger kind. ADR-016.
//
// Each registrar holds the kind-specific state (subscriptions,
// handler maps, scheduler entries) and exposes reader accessors as
// methods on the concrete type. TriggerService is a thin orchestrator
// that looks up the registrar by kind and delegates.
//
// Invariants the interface relies on:
//
//   - Activate MUST be idempotent. Calling Activate twice with the
//     same def is a no-op on the second call. The KV watcher's
//     DeliverLastPerSubject replay re-delivers definitions that
//     loadAllTriggers already installed (#217 / #221 / #223); without
//     idempotency the second Activate would unsubscribe and re-create
//     the live handler, opening a message-loss window.
//
//   - Deactivate MUST be idempotent. Calling Deactivate for a def
//     that was never activated, or already deactivated, is a no-op
//     that returns nil.
//
//   - ValidateConfig is a pure function of def — no I/O, no state
//     mutation. It runs before Activate and may run independently
//     (e.g. from the API control plane validating a definition
//     before persisting it).
//
// Filename convention: each registrar lives in registrar_<kind>.go
// inside the trigger package. The interface stays in this package
// because a sub-package would create an import cycle with the
// kind-specific types (SubjectTrigger, WebhookHandler, HTTPHandler)
// defined alongside their handler logic. See ADR-016 §Alternatives.
type TriggerRegistrar interface {
	// Activate installs the trigger and starts its live behavior
	// (subscribe, register handler, add to scheduler). Idempotent:
	// a second call with the same def.ID is a no-op returning nil.
	Activate(ctx context.Context, def TriggerDef) error

	// Deactivate removes the trigger and tears down its live
	// behavior. Idempotent: removing an unknown ID is a no-op
	// returning nil.
	Deactivate(ctx context.Context, def TriggerDef) error

	// ValidateConfig checks the kind-specific config fields. Pure
	// function: no I/O, no state mutation. Returns nil when valid.
	ValidateConfig(def TriggerDef) error
}

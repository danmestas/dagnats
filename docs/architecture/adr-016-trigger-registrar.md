# ADR-016: Trigger Registrar Interface

**Status:** Accepted
**Date:** 2026-05-22
**Context:** Parent #273 Phase 1.1 (issue #301)

## Context

`internal/trigger/service.go` carried a four-way `switch` inside `addTrigger` (and a parallel branchy `removeTrigger`) that dispatched on which optional config field a `TriggerDef` had set. Each new trigger kind meant editing three places — the switch, `removeTrigger`, and `Validate()` in `validate.go` — and the orchestrator (`TriggerService`) reached into every kind's storage map directly.

Symptoms this caused in practice:

- The switch was the diff that ADR-013 (HTTP triggers) had to thread through.
- The reader accessors (`WebhookHandler()`, `HTTPRouter()`) duplicated map-walk logic that should live next to the map itself.
- Adding the next kind (e.g. JetStream subject trigger, MQTT bridge) required edits in four files that have nothing structurally in common except "I'm a trigger".

This ADR extracts the kind-specific lifecycle into a small interface and turns the switch into a table dispatch. It is the first concrete refactor under the #273 plan and intentionally avoids any user-visible change.

## Decision

### 1. `TriggerRegistrar` interface

A single interface owns the per-kind lifecycle:

```go
type TriggerRegistrar interface {
    Activate(ctx context.Context, def TriggerDef) error
    Deactivate(ctx context.Context, def TriggerDef) error
    ValidateConfig(def TriggerDef) error
}
```

Invariants (documented on the interface):

- `Activate` is **idempotent**. The KV watcher's `DeliverLastPerSubject` replay re-delivers definitions at startup; without idempotency the second `Activate` would unsubscribe and re-create the live handler, opening the message-loss window the #217 / #221 / #223 fix closed.
- `Deactivate` is **idempotent**. Removing an unknown ID returns nil.
- `ValidateConfig` is a **pure function** of `def` — no I/O, no state mutation.

### 2. Deep ownership

Each registrar owns its kind-specific state. There is no central "kinds" map on `TriggerService` that knows what every kind stores; the registrar is the canonical owner.

- `cronRegistrar` wraps the existing `Scheduler` (which already owns the cron trigger table).
- `subjectRegistrar` owns the `subjects` map and the close-on-deactivate behavior.
- `webhookRegistrar` owns the `webhooks` map and exposes `Handler() http.Handler` for the public webhook endpoint.
- `httpRegistrar` owns the `httpRoutes` map and exposes `Router() http.Handler` for the synchronous HTTP trigger surface.

`TriggerService` becomes a thin orchestrator: `addTrigger` is a table lookup followed by `reg.Activate`; the reader methods (`WebhookHandler()`, `HTTPRouter()`) are one-line proxies through the matching registrar.

### 3. Validators stay co-located with their registrar

Each registrar's `ValidateConfig` delegates to the existing `validate*Config` functions in `validate.go`. The package-level `Validate()` function is unchanged — it is still the entry point for "validate any trigger def, regardless of kind" and is what `addTrigger` calls before dispatch. The functions in `validate.go` were not split into per-file siblings; that would have been shallow-module creep for pure functions that are already small.

### 4. Package layout

All registrar code lives in package `trigger`, in files named `registrar.go` (interface) and `registrar_{cron,subject,webhook,http}.go` (implementations). Each implementation file is under 70 lines (excluding comments and blank lines).

The interface is **not** in a sub-package, despite the issue's literal `internal/trigger/registrar/` path suggestion. See Alternatives §1 below.

## Consequences

- The trigger-kind switch in `addTrigger` is replaced by a one-line table dispatch; the next kind plugs in as a new file plus a single registry entry.
- `removeTrigger` no longer special-cases each kind. It iterates every registrar's `Deactivate` (each is a no-op on unknown IDs, by interface contract). This trades a per-kind branch for a bounded loop — there are 4 registrars, ever.
- The `subjects` / `webhooks` / `httpRoutes` fields remain on `TriggerService`. They reference the **same maps** the registrars own — Go maps are reference types, so the registrar is the canonical owner and the service field is a back-reference for the in-package regression test (#217 / #221 / #223) that observes `SubjectTrigger` pointer identity through `svc.subjects[id]`. That test is the non-negotiable acceptance criterion for the refactor; preserving its exact code path was worth the shared-reference field.
- Net engine LoC change is slightly negative (≈ -50 lines): the switch and the kind-specific reader walks come out; the registrar files are net additions but each is small and the orchestrator shrinks more than the registrars add.

## Alternatives Considered

### 1. Sub-package `internal/trigger/registrar/`

The issue body proposed `internal/trigger/registrar/registrar.go` as the interface home. We did not do that.

The blocker is an import cycle. Each registrar implementation needs to construct kind-specific runtime types — `NewSubjectTrigger`, `NewWebhookHandler`, `NewHTTPHandler` — and those constructors live in the `trigger` package. If the registrar interface (and implementations) live in a sub-package, that sub-package must import `trigger`. But `trigger.TriggerService` must also import the sub-package to look up registrars by kind. Cycle.

Three escape hatches were considered and rejected:

- **Move `TriggerDef` and the configs to a third package.** Heavy: every existing `trigger.TriggerDef` reference in the engine and API control plane changes its import. Outside the scope of a pure refactor.
- **Have the interface signatures take `any` for def.** Loses type safety; every implementation type-asserts. The whole point of a contract is gone.
- **Have the registrar package define a minimal `Def` interface that `TriggerDef` satisfies structurally.** Workable but requires adding accessor methods to `TriggerDef` purely for the abstraction's benefit, and the registrar implementations still need access to `SubjectTrigger` / `WebhookHandler` constructors from `trigger`, which puts us back at the cycle.

In-package files named `registrar_*.go` get every benefit the spec asked for (table-driven dispatch, deep ownership, per-kind ≤ 70 LoC, kind isolated to one file) without the abstraction cost. The ADR records this deviation; future work that genuinely needs an external implementation of `TriggerRegistrar` would justify the move.

### 2. ServiceCtx shallow-ownership variant

The audit explicitly considered (and rejected) a shallow alternative: keep all the maps on `TriggerService`, pass a `ServiceCtx` struct into each registrar's methods. The registrars would mutate the shared ctx.

That variant fails the Ousterhout test:

- The interface gets wider (every method takes a context AND a service handle).
- The complexity of "which map does which kind own" stays in `TriggerService` — it's still the place that knows every kind's storage shape.
- Adding a kind still means editing `ServiceCtx` to add the kind's map.

Deep ownership pulls the complexity into the registrar. The orchestrator no longer needs to know what `subjects` even is — it only knows "look up registrar by kind, call Activate".

### 3. Pass id to Deactivate instead of full def

The interface takes `TriggerDef` in `Deactivate` for symmetry with `Activate`. The implementations only read `def.ID`. We considered a narrower `Deactivate(ctx, id string) error`. Rejected for two reasons:

- Symmetry: the engine could grow per-kind teardown that needs more than the id (e.g. unregistering a Slack interaction key). Locking the signature to `id string` would force a future widening.
- Cost: zero. `TriggerDef{ID: id}` is a stack allocation; the registrar reads what it needs.

## Test Plan

Acceptance criteria from the issue:

- Every existing test in `internal/trigger/*_test.go` passes unchanged.
- New `TestRegistrarsAreIdempotent` (one sub-test per built-in kind): Activate twice → no error, pointer identity preserved. Deactivate twice → no error.
- New `TestServiceReaderSurfaceUnchanged`: `WebhookHandler()`, `HTTPRouter()`, `TriggerCount()` all return non-nil and behave identically to pre-refactor on 404 / 405 / counting paths.
- Local CI gate clean: `gofmt -l .` empty, `go vet ./...` clean, `staticcheck ./...` clean, full `go test ./...` green (modulo pre-existing flakes in `internal/engine` TempDir cleanup and `console` browser smoke tests, both unrelated to triggers).

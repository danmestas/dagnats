---
title: "Runtime-Generated Workflows (Agent Runtimes)"
weight: 9
---

# Runtime-Generated Workflows (Agent Runtimes)

Most workflows are **pre-registered**: you author a DAG, register it, and trigger
runs of it. Agent runtimes lift that restriction for **gated** task handlers — a
running step can **author a brand-new workflow at runtime and launch it**, so an
LLM planner can compose known tools into a *novel* DAG and execute it durably,
crash-recoverable like any other run.

This is the capability ADR-021 Phase A delivers. It is opt-in, deny-by-default,
and bounded on every axis — a runaway agent cannot fork-bomb the engine.

> **Mental model.** A gated handler receives a `ControlPlane` handle on its
> `TaskContext`. Through two methods — `RegisterWorkflow` and `StartRun` — it
> authors an **ephemeral** workflow definition and spawns a **child run** of it.
> The child is linked to the spawning run's lineage, so every run an agent
> generates belongs to one **generation tree** rooted at the top-level run.

## The control-plane handle

```go
type ControlPlane interface {
    // Author an ephemeral workflow def; returns the server-scoped name.
    RegisterWorkflow(ctx context.Context, def dag.WorkflowDef, opts RegisterOpts) (scopedName string, err error)
    // Launch a child run of a (scoped) workflow; returns the child run ID.
    StartRun(ctx context.Context, name string, input []byte) (runID string, err error)
    // Current-vs-max quota usage, so a handler can self-throttle.
    Budget(ctx context.Context) (RuntimeBudget, error)
}
```

`ctx.ControlPlane()` returns the handle **or `nil`** — it is `nil` unless the step
both *declares* the capability **and** is *granted* it (see below). Handlers must
nil-check. Every failure is a typed `*ControlPlaneError` (never a panic), so the
[durable agent loop]({{< ref "agent-loop-pattern" >}}) can inspect `.Kind` and
regenerate-and-retry instead of crashing the engine.

## Using it (SDK)

**1. Declare the capability on the step** that should be able to generate
workflows:

```json
{
  "name": "planner",
  "version": "1.0",
  "steps": [
    { "id": "plan", "task": "plan-task", "type": "normal",
      "required_capabilities": ["control-plane"] }
  ]
}
```

**2. Build the worker with a control plane** and nil-check the handle:

```go
w := worker.NewWorker(nc, worker.WithControlPlane(worker.NewControlPlane(nc)))

w.Handle("plan-task", func(ctx worker.TaskContext) error {
    cp := ctx.ControlPlane()
    if cp == nil {
        return ctx.Fail(fmt.Errorf("control plane not granted"))
    }

    // Author a child workflow AT RUNTIME — it is not pre-registered.
    child := dag.WorkflowDef{
        Name: "do-step", Version: "1",
        Steps: []dag.StepDef{{ID: "work", Task: "child-work", Type: dag.StepTypeNormal}},
    }
    scoped, err := cp.RegisterWorkflow(ctx.Context(), child, worker.RegisterOpts{})
    if err != nil {
        return ctx.Fail(fmt.Errorf("register child: %w", err)) // typed error → loop self-corrects
    }

    runID, err := cp.StartRun(ctx.Context(), scoped, nil)
    if err != nil {
        return ctx.Fail(fmt.Errorf("start child: %w", err))
    }
    return ctx.Complete([]byte(`{"planned":true}`))
})

// The runtime-authored workflow's task must also be handled.
w.Handle("child-work", func(ctx worker.TaskContext) error {
    return ctx.Complete([]byte(`{"done":true}`))
})
```

A complete, runnable version lives in
[`examples/planner/`](https://github.com/danmestas/dagnats/tree/main/examples/planner).

`RegisterWorkflow` returns a **server-computed scoped name**
(`agent.<root-run-id>.<your-name>`) — you pass *that exact string* to `StartRun`.
The worker never constructs the namespace key; the server owns it.

## Enabling it (operator): deny-by-default grant policy

Declaring `control-plane` is necessary but **not sufficient**. The workflow must
also be **granted** the capability in `dagnats.yaml`:

```yaml
policy:
  control_plane:
    grant:                # workflows allowed a ControlPlane handle
      - planner
      - supervisor
    promote:              # subset additionally allowed to promote (must ⊆ grant)
      - supervisor
```

- A workflow **absent from `grant`** gets `ctx.ControlPlane() == nil` even if its
  step declares the capability — deny-by-default is structural (the engine strips
  the capability from the dispatched task; the worker never receives a handle).
- The policy is **hot-reloadable** ([ADR-018]) — editing `dagnats.yaml` flips a
  grant live, no restart.

## The safety model

Agent runtimes open a generative capability, so every axis is bounded. None of
these ever crash the orchestrator — they return a typed `ControlPlaneError`.

| Concern | Defense | Kind |
|---|---|---|
| Privilege escalation | Deny-by-default: capability **declared + granted** | `denied` |
| Acting in a foreign tree | **Server-derived** namespace (`agent.<root>.*`) + a **per-dispatch nonce** that binds the request to the run the worker is actually executing | `namespace` |
| Runaway recursion | **Generation-depth cap** (`max_generation_depth`, ≤ engine ceiling) | `depth_exceeded` |
| Resource exhaustion | Per-tree **quotas**: max active runs, max registered defs | `quota_exceeded` |
| Registration storms | Per-tree **rate limit** | `rate_limited` |
| Unauthorized promotion | Promotion governed by the `promote` policy list | `denied` |
| Leaked ephemeral defs | A bounded, idempotent **reaper** sweeps `agent.<root>.*` defs after the root run is terminal + a grace window |  |

**Namespace binding (the key invariant).** A worker can only `RegisterWorkflow` /
`StartRun` for the run it is *currently executing*. The orchestrator stamps a
fresh random nonce into each task dispatch; the handle carries it; the server
rejects any request whose nonce doesn't match the run's current step. A worker
holding a *different* live run's ID cannot forge another tree's nonce.

## Self-throttling with `Budget()`

A granted handler can read its remaining budget and back off *before* hitting a
`quota_exceeded` reply:

```go
b, _ := cp.Budget(ctx.Context())
if b.ActiveRuns >= b.MaxActiveRuns {
    // pause / re-plan instead of spawning
}
// RuntimeBudget{ ActiveRuns, MaxActiveRuns, RegisteredDefs, MaxRegisteredDefs }
```

## Promotion (durable defs)

By default a registered def is **ephemeral** (namespaced to the tree, reaped when
the root completes). Setting `RegisterOpts{Promote: true}` registers it under the
stable, reaper-immune `promoted.*` namespace — but only if the workflow is in the
policy's `promote` list; otherwise the call returns `denied`. (Promoted defs are
*not* subject to the per-tree quotas — they have no owning tree.)

## Observability

- **Console → Agent runtimes** (Activity section): one row per generation tree,
  showing the spawned-run lineage, per-runtime budget consumption, and a
  "runtime" tag on runs that were spawned by an agent. Live over SSE.
- **`nats micro ls` / `nats micro stats`**: the internal control-plane services
  (`dagnats-api`, `dagnats-trigger`) are discoverable — see
  [Service discovery]({{< ref "/docs/operations/service-discovery" >}}).
- **Audit log** (Console → Audit): every grant decision and control-plane
  mutation (`runtime.register` / `runtime.spawn` / `runtime.promote`, plus
  `denied` outcomes) is recorded.

## Configuration reference

All values are read from `dagnats.yaml` (or the `DAGNATS_*` env override) and are
**per generation tree** (keyed by the root run). Full details in
[Configuration]({{< ref "/docs/operations/configuration" >}}).

| Key | Env | Default | Meaning |
|---|---|---|---|
| `max_active_runs_per_root` | `DAGNATS_MAX_ACTIVE_RUNS_PER_ROOT` | `100` | Max non-terminal runs per tree |
| `max_defs_per_root` | `DAGNATS_MAX_DEFS_PER_ROOT` | `500` | Max ephemeral defs per tree |
| `max_generation_depth` | `DAGNATS_MAX_GENERATION_DEPTH` | `3` | Max spawn nesting (≤ engine ceiling) |
| `max_registers_per_minute_per_root` | `DAGNATS_MAX_REGISTERS_PER_MINUTE_PER_ROOT` | `60` | Register rate limit per tree |
| `policy.control_plane.grant` / `.promote` | — | (empty = deny all) | Capability grant lists |

## Not in Phase A

Deliberately deferred (the control-plane interface is stable; these extend behind
it): per-**step** (vs per-workflow) grant granularity; a Tier-2 supervisor and
`ProvisionFunction` (spawning new *workers*, not just runs); token/compute
metering in `Budget`. Operator worker controls (drain/decommission) stay out —
the Agent-runtimes view is observe-only.

[ADR-018]: https://github.com/danmestas/dagnats/blob/main/docs/architecture/adr-018-dagnats-yaml-hot-reload.md

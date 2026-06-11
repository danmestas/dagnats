# ADR-021: Ad-hoc agent runtimes — a scoped generative control-plane capability

Status: Proposed (Draft / RFC).
Deciders: TBD
Depends on: ADR-001 (agent-harness-gaps), ADR-002 (durable-agent-loop), ADR-012 (engine-resolves-workflow-def), ADR-017 (services-namespace)

## Context

dagnats's stated purpose is "autonomous LLM coding pipelines," but today every
workflow must be **pre-registered**. An agent cannot generate a novel DAG and run
it; it can only invoke workflows a human authored ahead of time. For open-ended
agent work — where the plan is not known until runtime — that is a hard ceiling.

A code audit established that letting a running **function** (a task handler inside
a worker) register new functions/workflows is **blocked at the SDK boundary, not by
the architecture**:

- `TaskContext` (worker/worker.go) is a curated interface — `Input`, `Complete/Fail`,
  `PutStream`, `Checkpoint`, `WaitForSignal`, `SendSignal`. It exposes **no** NATS
  connection, KV, or subscription. A handler is intentionally pure task logic.
- Worker handlers are fixed at `Start()`; there is no post-Start `RegisterHandler`.
- `Service.RegisterWorkflow` (internal/api) *does* write a def to the `workflow_defs`
  KV, and the engine resolves defs **live** from that KV (ADR-012) — but a handler
  has no path to `Service`.
- No application-level auth gates `RegisterWorkflow`; only NATS cluster auth.

So the substrate already supports runtime generation (registering a workflow is a KV
write the engine resolves immediately; `sub_workflow` already spawns child runs from
KV defs). The open question is **how to open a safe, bounded capability** rather than
handing handlers raw NATS.

A second surface raised the same question from the operator side — *should the console
let a user add / remove / mutate workers?* That question is load-bearing here because
it exposes a distinction this ADR turns on (see Decision §8): **a worker is a process,
not a record.** Triggers and workflows are KV data the console can CRUD; a worker is a
running process that self-registers via heartbeat. You cannot "add a worker" by writing
a record — something must start a process. The console's worker view is therefore a
read-only mirror of reality plus a small set of lifecycle controls, not a CRUD surface.

## Decision (proposed)

### 1. A capability-scoped `ControlPlane` handle on `TaskContext`

Expose control-plane operations to a handler **only** when (a) the step/workflow
declares `capabilities: [control-plane]` and (b) deployment policy grants it. Otherwise
`ctx.ControlPlane()` is absent (nil / denied stub). The handle *is* the authority
(capability-based security); the default everywhere is **deny**.

```go
type ControlPlane interface {
    // Register an ephemeral, run-scoped workflow def over the existing function
    // palette. Validated at the boundary; namespaced to the caller; counts to quota.
    RegisterWorkflow(ctx, def dag.WorkflowDef, opts RegisterOpts) (scopedName string, err error)
    // Start a run of a registered workflow (own namespace, or one granted).
    StartRun(ctx, name string, input any) (runID string, err error)
    // Tier 2 only: ask the supervisor to provision a worker that serves new functions.
    ProvisionFunction(ctx, spec FunctionSpec) (handle WorkerHandle, err error)
    Budget() RuntimeBudget
}
```

This is a **deep module** (Ousterhout): a tiny interface hiding DAG validation,
registration, scheduling, namespacing, GC, and scoping. The rejected alternative —
raw NATS in `TaskContext` — is a shallow, leaky pass-through that hands every handler
author the control plane's full complexity *and* unfettered authority.

### 2. Two tiers — the key decomposition

The two asks ("generate workflows" and "provision functions") have very different
risk profiles. Split them.

- **Tier 1 — dynamic workflow generation over the EXISTING function palette (ship first).**
  `RegisterWorkflow` + `StartRun`. An agent composes a novel DAG from *known* functions
  and runs it. This is the dominant case for an LLM planner (compose known tools into a
  new plan). Bounded, finishable, low new surface, no process spawning.
- **Tier 2 — dynamic function / worker provisioning (gated, deferred).** `ProvisionFunction`
  asks a **supervisor** to spawn scoped, ephemeral workers serving new functions. This is
  the open-ended, dangerous part (new processes, new attack surface, the durability
  asymmetry below). Design it now; ship it after Tier 1 proves out.

### 3. Lifecycle, ownership, and the durability asymmetry

KV-stored workflow defs **persist**; functions served by a process **vanish when that
process dies**. A generated workflow that references a vanished function hangs / DLQs.
Resolve by ownership + tiering:

- Every ad-hoc def/run carries an **owner lineage** (the root run that began the chain).
- **Ephemeral (default):** owned by the root run, GC'd when it reaches a terminal state
  (with a TTL grace). Scratch workflows for one agent task.
- **Promoted (explicit, higher capability):** survives under a stable namespace; for
  building durable ad-hoc runtimes. Subject to governance (Open Questions).
- A **reaper** (KV TTL + a sweep keyed on terminal root runs) collects ephemeral defs.

### 4. Namespacing & isolation (ADR-017)

Ad-hoc defs live under `agent:{rootRunID}:{name}` (ephemeral) or a stable
`service::name` (promoted). An agent may register/run/provision **only within its
namespace** — no clobbering another agent's `billing::charge`. Provisioned workers
get NATS credentials scoped to their namespace's subjects.

### 5. Bounds — TigerStyle "bounded everything"

Per-runtime quotas enforced **at the capability boundary** (exceeding a quota returns
an error the agent loop handles — it never crashes the engine): max active runs, max
registered defs, max provisioned workers, a **generation-depth cap** (a counter
propagated through the lineage; a workflow that generates a workflow that generates…
is the recursion the "no recursion / bounded everything" rules forbid — cap it), and a
compute/token budget. Registration is rate-limited (the existing rate-limit KV).

### 6. Validation — Ousterhout "define errors out of existence"

`RegisterWorkflow` runs `dag.Validate` at the boundary and returns **structured errors,
not panics** (cf. the `validateLoopConfig` panic-on-`sub_workflow` fix — agents must get
errors back to self-correct, never crash the orchestrator). Dangling function refs are
surfaced as the existing "no worker" state (already visible in the Functions/Concurrency
views). The **durable agent loop (ADR-002)** is the self-correction mechanism: validate
→ error → regenerate → retry, durably.

### 7. Provenance — the event log makes self-extension auditable

A generative system is opaque unless you can reconstruct *what created what*. New
event types — `workflow.generated`, `function.provisioned`, `runtime.spawned`,
`run.started{by}` — each parented to the generating run. Because dagnats is
**event-sourced**, the provenance tree is a near-free byproduct: every generation is an
event. A new **"Agent runtimes"** console view (Activity layer) renders the generation
tree, per-runtime quota/budget consumption, and spawned workers. This is non-negotiable:
without provenance, ad-hoc generation is an un-auditable black box.

### 8. Operator console stance — observe + rotate, not CRUD

From the worker-management question: **the console treats the worker fleet as
*observe + rotate*, not CRUD.** Concretely:

- **Observe** (always): connected workers, their functions, load, heartbeat, in-flight
  tasks, AckWait windows (the Worker detail view).
- **Drain / decommission** (safe, no supervisor needed): a graceful-stop control signal
  a live worker honors — finish in-flight, stop pulling, exit. Reversible. A real ops
  need (deploys, maintenance, draining a bad host).
- **Evict a stale record** (cosmetic): remove a dead worker's heartbeat from the KV mirror.
- **Provision** (guarded, supervisor-backed, Tier 2): "add a worker" is only meaningful if
  a supervisor can launch a process. Exposed to operators as a deliberate, policy-gated
  action — not a casual button.
- **NOT** a live-worker function editor. Mutating a running worker's capability set
  destroys **fleet reproducibility** (you could no longer rebuild the fleet from
  source/CD; clicked-in handlers diverge from the deployed config and vanish on restart).
  Worker capability changes belong in source / `dagnats.yaml` + redeploy (or hot-reload
  for embedded workers, ADR-018).

The deeper principle, and the reason this ADR unifies the agent and operator questions:
**operators and agents have opposite relationships to reproducibility.** Operators want
a *reproducible* fleet (observe + rotate; mutate via deploy). Agents want *ephemeral
scratch* compute (spawn / provision / discard, bounded + GC'd) — for them, non-durability
is the *point*. The same low-level mechanism (start/stop a worker process), two
philosophies, two surfaces, two guardrail sets.

## Alternatives considered

- **A. Raw NATS in `TaskContext`.** Rejected: leaky, unsafe, pushes control-plane
  complexity and unfettered authority onto every handler.
- **B. External control plane only** (agents run as external processes calling the
  existing API). Works **today**, zero code change — this is the do-nothing baseline. The
  in-runtime capability's value over B is making the **agent itself** a first-class
  dagnats run: durable, event-sourced, observable, crash-recoverable via the agent loop.
  An external script gets none of that.
- **C. A pre-registered meta-workflow with a generic dispatch step.** Limited; composes
  known steps but cannot author real DAGs.
- **D. Raw `RegisterHandler` post-Start as the primary function mechanism.** Rejected as
  primary (durability asymmetry, fragility); kept only as an advanced escape hatch.

## Consequences

- **Category shift:** workflow engine → generative substrate / "agent OS." dagnats becomes
  self-extending. Powerful and the point of the product; also the source of every risk below.
- **Loss of static analyzability; gain of emergent behavior.** Provenance (§7) and bounds
  (§5) are the mitigations.
- **New attack surface** (privilege escalation from the data plane). Mitigated by
  capability scoping (§1), namespace isolation + scoped NATS creds (§4), and the audit log.
- **Real distributed-GC complexity** (the durability asymmetry, §3) — tiering + lineage GC.
- **LLM failure modes** — hallucinated defs (→ boundary validation §6), infinite generation
  (→ depth cap §5), cost (→ budget §5). All map onto existing primitives.
- **Makes the "autonomous LLM pipelines" promise real.**

## Rollout

- **Phase A:** Tier 1 (`RegisterWorkflow` / `StartRun`, ephemeral, bounded, provenance) +
  the "Agent runtimes" console view + a reference `planner` function (generate a def,
  register, run).
- **Phase B:** operator worker controls (drain / decommission / evict) in the console.
- **Phase C:** Tier 2 (supervisor + `ProvisionFunction`) — gated, after A/B prove out.

## Open questions

1. **Supervisor:** build vs. adopt? Process / container / k8s? (iii's `iii-supervisor` is a
   reference model.) Tier 2 and operator-provision both depend on this.
2. **Promoted defs:** who may promote, how are they versioned and GC'd, and how is the
   "agent's scratch" vs "shared durable" boundary governed?
3. **Budget accounting:** unify compute + LLM-token budgets into `RuntimeBudget`; where is
   it metered?
4. **Default deny everywhere** — which deployment profiles opt into Tier 1 / Tier 2 / operator
   provisioning, and how is the capability grant expressed in `dagnats.yaml`?

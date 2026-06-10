# Console understandability plan

**Status:** Proposal / design note. Companion to the #274 console-UX-overhaul proposal and ADR-017 (services namespace / `service::name`). Not yet ratified as an ADR — #274 is the *what-to-build* IA & feature list; this note supplies the *why*: a single operator conceptual model and a framework (Don Norman's gulfs of execution and evaluation) for ordering the work around understandability.

**Method:** the dagnats docs (`core-design.md`, `agent-system.md`, `control-plane-ui-design.md`, ADR-014/015/017, `workflow-schema.md`, `wire-protocol.md`) were read for the operator's conceptual model; the iii runtime (`~/references/iii`) was read for a contrasting, deliberately simpler model; and the current console + the interactive redesign prototype were audited through Norman's design principles.

---

## 1. Diagnosis — Norman's lens

Norman's test for an understandable system: it presents a clear **conceptual model**, and the user can cross the **gulf of execution** ("how do I do X?") and the **gulf of evaluation** ("what state is it in / what just happened?") *without already understanding the implementation*. The console fails that test in four ways.

### A. The conceptual model is doubled and leaky
An operator must hold two models at once and the UI never reconciles them:

- **Activity-first** — a Run is a timeline of immutable events (`WORKFLOW_HISTORY` stream is the source of truth; the orchestrator replays it on restart).
- **Inventory** — what is registered/connected: workflows deployed, workers online, task types subscribed, services grouped (metadata in KV buckets).

Worse, **implementation nouns leak into the operator surface**. `Streams`, `KV`, consumers, leases, `MaxAckPending`/`AckWait`, `NakWithDelay`, heartbeat intervals sit at the same altitude as `Workflows`. Debugging task delivery requires JetStream knowledge, not dagnats domain logic. Norman: *do not expose the system's internal structure as the user's model.*

### B. Naming overload breaks the model before it forms
The same idea wears four names:

| Name | Where it comes from | What it really is |
|---|---|---|
| **task name** | routing subject `task.{type}.>` | the routing key |
| **task type** | workflow step `task` field | the unit definition |
| **`service::task`** | SDK convention (ADR-017) | a naming convention the engine never reads |
| **function** | agent SDK vocabulary | the same unit, different word |

ADR-017 explicitly makes services metadata-only with no engine gating. An operator cannot build a stable mental model on shifting vocabulary.

### C. The gulfs are widest exactly where dagnats is most powerful
The richest behaviours have **no console affordance** — you must drop to `nats stream view` / KV inspection to evaluate state:

- **Gulf of evaluation:** the run event timeline; **agent-loop iteration** state; **wait-for-event** correlation/pending; **child-workflow** run trees; **"what's blocking?"** — the two concurrency scopes (global `MaxTaskConcurrency`, retried via `SLEEP_TIMERS` ~1s, vs per-run `MaxSteps`, enqueue-blocked) have no unified view; DLQ root cause; and the event-log-vs-KV-snapshot divergence is the operator's debugging burden.
- **Gulf of execution:** run a workflow, fire a trigger, retry/discard a DLQ entry, send a signal — partially buried or CLI-only.

### D. Signifier & affordance defects
Rows and nav items are clickable `<div>`s (no keyboard, no focus ring); jargon appears without the glossary tooltip that already exists in the console.

### What is already right — keep and lean on it
These are dagnats wins iii does **not** have; the plan preserves them:

1. **Datastar SSE live feedback** everywhere (best-in-class evaluation feedback; iii polls).
2. **DLQ as first-class** (top-nav, detail, redrive/discard/soft-discard).
3. The **side-sheet** inspection pattern.
4. **Command palette** (cmd-k).
5. **Glossary tooltips** (built-in jargon explainer).
6. **Read-only mode** (`CONSOLE_READ_ONLY=1`) — a real Norman constraint.
7. **Metrics dashboard with sparklines + anomaly detection.**

---

## 2. The simpler model — borrow iii's *framing*, keep dagnats's *power*

iii collapses all distributed work to **Worker → Function → Trigger** and leads its console with **"what exists"** (inventory-first), hiding consumers/retention/acks/event-sourcing entirely. A developer writes `worker.trigger({function_id, payload})` — never "create a subject, configure a consumer, handle backpressure." `service::name` is presentation-layer grouping, not a routing contract. States are a first-class reactive primitive.

We do **not** flip dagnats's engine. We re-layer the *console* into three altitudes with progressive disclosure:

| Layer | Question it answers | Nouns | Norman role |
|---|---|---|---|
| **1 · Inventory** | What can this system do, what's plugged in? | **Workflows**, **Functions** (unified `service::name`), **Workers**, **Triggers** | the conceptual model, surfaced first |
| **2 · Activity** | Is anything on fire? why? | **Dashboard**, **Runs** (+ detail), **DLQ**, **Logs**, **Metrics** | evaluation — dagnats's strength |
| **3 · System** | How is it wired? (power users) | **Streams**, **KV**, **Consumers/Leases**, **Concurrency** | hide implementation; reveal on demand |

**The one sentence the UI must teach** (put it on the landing / Config self-portrait as the conceptual-model anchor):

> **Workers** register **Functions**. **Triggers** fire **Workflows**. A **Workflow** is a DAG of steps, each step calls a **Function**. Every firing is a **Run** you can watch live and replay from its event log.

That single model dissolves the task/type/service/function overload — **pick "Function" (`service::name`) everywhere**: copy, glossary, and a Functions registry page — and demotes the JetStream nouns out of the operator's path.

---

## 3. Functionality decomposition mapped to the model

| Engine capability | Operator-facing noun | Layer | Console surface today | Gap |
|---|---|---|---|---|
| Workflow DAG + versions | Workflow | Inventory | list + detail | DAG/step view thin |
| Task type / `service::task` | **Function** | Inventory | (none, lives under Workers) | **missing registry** |
| Worker process + subscriptions | Worker | Inventory | under Ops | promote |
| Cron/subject/webhook/manual | Trigger | Inventory | list | fire-now affordance |
| Run + event log | Run | Activity | list + detail | events/IO/timeline |
| Failed tasks | DLQ | Activity | first-class ✓ | keep |
| slog / trace context | Logs | Activity | (none) | **missing destination** |
| Metrics + anomalies | Metrics | Activity | ✓ | keep |
| Agent loop | (Run sub-state) | Activity | (none) | **iteration inspector** |
| Wait-for-event | (Run sub-state) | Activity | (none) | **correlation panel** |
| Child workflows | (Run tree) | Activity | (none) | **run tree** |
| Concurrency (2 scopes) | Concurrency | System | (none) | **"what's blocking?"** |
| JetStream streams | Streams | System | top-nav (leaked) | demote |
| KV buckets | KV | System | top-nav (leaked) | demote |
| Consumers / leases | Consumers | System | (CLI only) | optional power view |

"Fully featured" = closing the **Activity** gaps (the engine already has the data; the UI hides it) and adding the **Concurrency** view. "Understandable" = the layering, the vocabulary unification, and teaching the model.

---

## 4. Phased plan (each item: the Norman gap it closes · #274 cross-ref)

**Phase 0 — Vocabulary & model unification** *(cheapest, highest leverage)*
- Adopt **Function** (`service::name`) as the one operator-facing term for task types; sweep copy + glossary; ship the **Functions registry** page. *(conceptual model · #274 R11 · ADR-017/#273)*
- Add the one-sentence model + a small concept diagram to the **Config self-portrait**. *(conceptual model · #274 R3)*

**Phase 1 — Inventory-first IA**
- Re-group the rail into **Inventory / Activity / System**; promote **Functions/Workers**, demote **Streams/KV/Consumers/Leases** under **System**. *(mapping; hide implementation · #274 R1/R6)*
- Educational empty states + header-tile counts on every list. *(discoverability · #274 R4/R5)*

**Phase 2 — Close the evaluation gulfs** *(most of "fully featured")*
- **Run detail:** Events / IO / Timeline tabs. *(evaluation)*
- New views the engine already has data for but the UI hides: agent-loop **iteration inspector**; wait-for-event **correlation panel**; child-workflow **run tree**; **"What's blocking?"** concurrency view unifying the two scopes; **DLQ detail** with payload + error + delivery count. *(evaluation — the headline feature gap)*
- **Logs** destination (severity / search / trace-id / top-sources). *(evaluation · #274 R2)*

**Phase 3 — Close the execution gulfs + fix affordances**
- Inline **Run / Fire-now / Retry / Discard / Signal** on the relevant rows, all respecting read-only mode. *(execution · #274 R7/R8)*
- Convert `onClick` `<div>`s → real `<button>`/`<a>`; keyboard + focus states; glossary tooltips on every jargon term. *(signifiers, affordances, error tolerance)*

**Phase 4 — Teach & polish**
- Trend sparklines (Datatype font), a mono data face (IoskeleyMono / Berkeley-Mono style), the borderless / typography pass. *(perception · #274 R10)*

---

## 5. Relationship to other work & reference artifact

- **#274** (console UX overhaul) — this note reorders #274's eleven recommendations around the conceptual model and the two gulfs; #274 remains the detailed file-level task list.
- **#273 / ADR-017** (`service::name` namespacing) — makes "Functions" real; Phase 0 depends on it for the registry's grouping axis.
- **Reference comp:** an interactive prototype on the team's MagicPath canvas already demonstrates the Inventory/Activity/System rail, the Functions registry, the Config self-portrait, the empty states, inline actions, the "What's blocking?" concurrency view, trend sparklines, and the borderless/mono typography pass. It is a *visual spec only* — the real console is Go templates + Datastar + Basecoat (single binary), so each prototype view translates to a Basecoat-classed template with Datastar bindings, not React.

# Step State Transitions via Lifecycle Events — Design Spec

**Date:** 2026-05-02
**Author:** Dan Mestas + Claude
**Status:** Draft (pending implementation)
**Scope:** Fix for [issue #137](https://github.com/danmestas/dagnats/issues/137) — engine doesn't transition step state from `queued` to `running` when worker pulls a task. Designed to make follow-up fixes for [#140](https://github.com/danmestas/dagnats/issues/140) (timeout doesn't fire) and [#141](https://github.com/danmestas/dagnats/issues/141) (fast-fail wedges) land as small additive PRs.

## Summary

The engine has no input that tells it "worker has started executing this step." `EventStepStarted` is defined in `protocol/protocol.go:59` but never published; `EventStepQueued` similarly defined and never published. The orchestrator's `onEvent` switch (`internal/engine/orchestrator.go:250-310`) has no case for either. Step state stays at `queued (attempts: 0/4)` from the moment the engine queues the task until completion, regardless of whether a worker has pulled and started executing.

The fix wires the missing lifecycle events: engine emits `step.queued` at dispatch, worker emits `step.started` when it picks up a task, engine handles both with monotonic state guards. A small protocol change (`AttemptNumber` field on `protocol.Event` with `omitempty`) keeps each attempt's events distinct in the JetStream history stream — without it, the existing `Nats-Msg-Id` dedupe drops retried `step.started` events.

## Outcome (the contract this PR ships)

After merge:

1. `dagnats run events <run>` shows the full lifecycle: `workflow.started` → `step.queued` → `step.started` → `step.completed` (or `step.failed` → eventual terminal). Each event has `Timestamp`. Operators can see queue depth (queued→started gap) and work duration (started→completed gap).
2. `dagnats run status` shows `Status=Running, Attempts=1` while a worker is mid-execution. Operators can distinguish "worker is actively running this" from "wedged."
3. The engine's internal step-state model (`dag.StepState`) has accurate `Status` and `Attempts` fields driven by external events through a monotonic state machine.

## Non-goals (explicit, scoped out)

- Fixing #140 (timeout doesn't fire). The new `step.queued`/`step.started` events provide the natural arming point for a timer; the timer's design is #140's job. See §7.2.
- Fixing #141 (fast-fail wedges). The `step.started` event makes attempts immediately visible, but the `handleTaskError` regular-error branch (`worker/worker.go:767-774`) still needs to publish `step.failed`. 3-line follow-up; see §7.1.
- Fixing #147 (retry never re-dispatches). Distinct surface — retry-scheduler / re-publish path. The `AttemptNumber = NumDelivered` choice in §3 is robust to whichever retry model #147 lands on. See §7.3.
- Changing the step state machine's enum (`Pending`, `Queued`, `Running`, `Completed`, `Failed` stay as-is per `dag/types.go`).
- Changing the history stream layout, the consumer pattern, or any subject conventions.
- Adding `AttemptNumber` to `step.completed` / `step.failed` in this PR. Their wire-format stays unchanged; the deferred `step.failed` extension is part of #141's follow-up.

---

## §1. Protocol change — `Event.AttemptNumber`

Single field on `protocol.Event` plus a one-line tweak to `NATSMsgID()`. Foundation for per-attempt event identity.

### `protocol/protocol.go` — Event struct

```go
type Event struct {
    Type          EventType       `json:"type"`
    RunID         string          `json:"run_id"`
    StepID        string          `json:"step_id,omitempty"`
    Timestamp     time.Time       `json:"timestamp"`
    Payload       json.RawMessage `json:"payload,omitempty"`
    TraceParent   string          `json:"trace_parent,omitempty"`
    TraceState    string          `json:"trace_state,omitempty"`
    WorkerID      string          `json:"worker_id,omitempty"`
    AttemptNumber int             `json:"attempt_number,omitempty"` // NEW
}
```

`omitempty` preserves the existing wire format for events that don't set it (`workflow.*`, `agent.loop.*`, `compensate.*`, `approval.*`). Old events on the stream deserialize unchanged — `AttemptNumber` is just zero.

**Why on `Event` rather than per-event `Payload`:** `AttemptNumber` is structural identity (it influences `NATSMsgID` for dedupe), not event-specific data. Multiple event types need it (`started` now, `queued` now, `failed` eventually). Top-level placement avoids both per-payload duplication and unmarshal cost in `NATSMsgID`.

### `NATSMsgID()` — append attempt suffix when set

```go
func (e Event) NATSMsgID() string {
    if e.StepID == "" {
        return e.RunID + "." + string(e.Type)
    }
    base := e.RunID + "." + e.StepID + "." + string(e.Type)
    if e.AttemptNumber > 0 {
        return base + "." + strconv.Itoa(e.AttemptNumber)
    }
    return base
}
```

Resulting msg-ids:

| Event | AttemptNumber | NATSMsgID |
|---|---|---|
| `workflow.started` for run X | 0 | `X.workflow.started` |
| `step.queued` run X step ingest, attempt 1 | 1 | `X.ingest.step.queued.1` |
| `step.started` run X step ingest, attempt 1 | 1 | `X.ingest.step.started.1` |
| `step.started` run X step ingest, attempt 2 (retry) | 2 | `X.ingest.step.started.2` |
| `step.completed` run X step ingest | 0 (not set this PR) | `X.ingest.step.completed` |

Why `step.completed` / `step.failed` keep `AttemptNumber=0` in this PR: keeps the diff small, no behavioral change in the failure-handling code paths. #141's follow-up extends `step.failed` to set `AttemptNumber` per §7.1.

### Tests (pure unit, no NATS)

`protocol/protocol_test.go`:

- `TestNATSMsgID_NoAttempt` — unchanged behavior when `AttemptNumber == 0`.
- `TestNATSMsgID_WithAttempt` — `1` produces `.1` suffix, `42` produces `.42`.
- `TestNATSMsgID_WorkflowEventIgnoresAttempt` — empty StepID path doesn't append attempt even if set.
- `TestEvent_MarshalRoundTrip_PreservesAttemptNumber` — JSON round-trip preserves the field.
- `TestEvent_UnmarshalLegacyMissingAttempt` — old events without the field deserialize to zero.

---

## §2. Worker emits `step.started`

The publish happens at `worker/worker.go:~707` (right after `tc.workerID = w.workerID`, before `err = handler(tc)`). The dispatch loop stays clean by delegating to a new dedicated helper — `publishEvent` (the existing generic helper) is **not modified**.

### New helper: `tc.publishStarted(msg)`

`worker/context.go` gains one new method. The existing `publishEvent` is untouched.

```go
// publishStarted publishes step.started for the current attempt.
// Reads the attempt number from the original NATS message metadata
// (NumDelivered increments on each redelivery: NAK retries, AckWait
// expiry). Returns error if metadata read or publish fails — caller
// should NAK the original message to allow retry.
//
// Called once per attempt, before invoking the user's task handler.
// Hides the metadata-read + AttemptNumber-assignment + publish chain
// from the dispatch loop.
func (c *taskContext) publishStarted(msg jetstream.Msg) error {
    if msg == nil {
        panic("publishStarted: msg must not be nil")
    }
    if c.runID == "" {
        panic("publishStarted: runID must not be empty")
    }
    meta, err := msg.Metadata()
    if err != nil {
        return err
    }
    evt := protocol.NewStepEvent(
        protocol.EventStepStarted, c.runID, c.stepID, nil,
    )
    evt.WorkerID = c.workerID
    evt.AttemptNumber = int(meta.NumDelivered)
    outMsg := &nats.Msg{
        Subject: evt.NATSSubject(),
        Header: nats.Header{
            "Nats-Msg-Id": {evt.NATSMsgID()},
        },
    }
    observe.InjectTraceContext(c.ctx, outMsg, &evt)
    data, err := evt.Marshal()
    if err != nil {
        return err
    }
    outMsg.Data = data
    _, err = c.js.PublishMsg(c.ctx, outMsg)
    return err
}
```

This mirrors the existing `publishEvent` body (`context.go:293`) but is specialized: the AttemptNumber assignment and the metadata read are encapsulated. Callers don't carry knowledge of the `Event.AttemptNumber` field or `msg.Metadata().NumDelivered`.

The existing `publishEvent` (used by `Complete` and `Fail*` paths) is **unchanged**. No "pass 0 for legacy" anywhere.

### Worker dispatch — three lines of integration

Inserted in `worker/worker.go` between current lines 706-707:

```go
tc.workerID = w.workerID

if err := tc.publishStarted(msg); err != nil {
    slog.ErrorContext(ctx, "failed to begin attempt — NAK and retry",
        "error", err,
        "task_type", taskType,
        "run_id", payload.RunID,
        "step_id", payload.StepID,
    )
    msg.NakWithDelay(1 * time.Second)
    return
}

err = handler(tc)
```

The dispatch loop's reader doesn't need to think about NumDelivered, msg metadata, or AttemptNumber assignment. Those live entirely inside `publishStarted`.

### Failure-mode rationale

- Publish failure → log + NAK with short delay → NATS redelivers → next attempt's NumDelivered is one higher → publish retried.
- Handler never runs if `step.started` couldn't be published. Avoids the engine seeing `step.completed` for an attempt it never saw `step.started` for. Lifecycle stays consistent.
- Short NAK delay (1s) because publish failures are typically transient (NATS reconnect). If they persist past `MaxDeliver` retries (currently unlimited per ADR-006), the engine never gets the started event and the operator sees the original `queued` symptom — same as today's bug, no regression. Flagged in §8.1.

### Why `nil` payload for `step.started`

The event carries enough information in its envelope (`RunID`, `StepID`, `WorkerID`, `AttemptNumber`, `Timestamp`). No additional fields needed for this PR. Future PRs can extend with a `StepStartedPayload` struct mirroring `StepFailedPayload` at `protocol.go:46` if `expected_duration_ms` or similar becomes useful (e.g., for #140's timer arming).

### Tests (worker integration with embedded NATS)

- `TestWorker_PublishesStepStartedBeforeHandler` — dispatch a task, register a handler that captures the moment of invocation, drain the history stream, assert `step.started` arrives BEFORE the handler observed its input. `AttemptNumber=1`, `WorkerID` populated.
- `TestWorker_PublishStartedFailure_NaksAndRetries` — close the JetStream connection mid-test (or inject error), dispatch a task, assert handler never invoked, message NAK'd, redelivery happens. Verifies the failure-mode policy.
- `TestWorker_AttemptNumberFromNumDelivered` — register a handler that errors on first call (NAK path), succeeds on second. Assert two `step.started` events with `AttemptNumber=1` and `AttemptNumber=2`.
- `TestPublishStarted_PanicsOnNilMsg` — defends the assertion contract.
- `TestPublishStarted_PanicsOnEmptyRunID` — defends the assertion contract.

---

## §3. Engine emits `step.queued`

The orchestrator already transitions step state to `Queued` at the dispatch site (`orchestrator.go:1492` per the explore agent). At the same point, it publishes `step.queued` for observability and downstream consumers (including future #140 timer arming).

### Helper: `internal/engine/event_publisher.go` (new)

```go
// internal/engine/event_publisher.go
// Lifecycle event publisher for engine-emitted events. Mirrors the
// worker's publishEvent pattern at worker/context.go but engine-side
// — there's no per-task context, just a JetStream handle.
package engine

import (
    "context"

    "github.com/danmestas/dagnats/protocol"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"
)

// publishLifecycleEvent publishes an Event to the history stream with
// proper Nats-Msg-Id dedup. Caller has already populated the Event;
// this function only handles the marshal + publish.
func publishLifecycleEvent(
    ctx context.Context, js jetstream.JetStream, evt protocol.Event,
) error {
    if evt.RunID == "" {
        panic("publishLifecycleEvent: evt.RunID must not be empty")
    }
    if evt.Type == "" {
        panic("publishLifecycleEvent: evt.Type must not be empty")
    }
    data, err := evt.Marshal()
    if err != nil {
        return err
    }
    msg := &nats.Msg{
        Subject: evt.NATSSubject(),
        Data:    data,
        Header: nats.Header{
            "Nats-Msg-Id": {evt.NATSMsgID()},
        },
    }
    _, err = js.PublishMsg(ctx, msg)
    return err
}
```

No trace-context propagation in this version — engine isn't running inside an OTEL span at dispatch time the way the worker is inside a handler span. If telemetry surfaces a need for engine-side trace propagation later, it's a one-line addition.

### Orchestrator dispatch — add the publish

Locate the existing TASK_QUEUES publish site (the place that publishes the task payload to subject `task.<type>.<run>`). Right after that publish succeeds:

```go
// (existing) publish task to TASK_QUEUES — kept verbatim
if _, err := js.PublishMsg(ctx, taskMsg); err != nil {
    // existing error handling
}

// NEW: emit lifecycle event
qEvt := protocol.NewStepEvent(
    protocol.EventStepQueued, runID, stepID, nil,
)
qEvt.AttemptNumber = 1   // this PR: only initial dispatch fires this
if err := publishLifecycleEvent(ctx, js, qEvt); err != nil {
    slog.ErrorContext(ctx, "failed to publish step.queued",
        "error", err,
        "run_id", runID,
        "step_id", stepID,
    )
    // do NOT roll back the task dispatch — the task is already on
    // the queue and a worker will pick it up. step.queued is
    // observability-only at this point in the design; missing it
    // is not correctness-fatal.
}
```

### Failure-mode contrast with §2

Worker's `step.started` failure NAKs because publish failure means the engine has no record of the attempt. Engine's `step.queued` failure logs but doesn't roll back, because:

1. The task is already published to TASK_QUEUES — rolling back would require a NATS-side delete of the queue message, which is fragile.
2. The worker will still pull the task, emit `step.started`, and the engine still gets the state transition. The only loss is the observability of "engine dispatched at time T" — operator sees `started` without a preceding `queued`.
3. Producing a duplicate task on the queue (by retrying the dispatch) is worse than producing missing observability.

Deliberate asymmetry. Documented inline.

### Why AttemptNumber=1 only

In the NATS-native retry model (CLAUDE.md `NakWithDelay` + worker's existing path at `worker.go:774`), retries don't go through the engine's dispatch code — NATS redelivers the same message after the NAK delay. Engine doesn't know retries are happening. So `step.queued` only fires on initial dispatch.

For the engine-driven retry model that #147 might land on (re-publish-on-failure), the same dispatch site fires again with `AttemptNumber=N`. The Nats-Msg-Id rule in §1 prevents dedupe collisions across attempts. The field's value is forward-compatible regardless of which retry model wins.

### Tests (engine integration)

- `TestOrchestrator_PublishesStepQueuedOnDispatch` — register a workflow, start a run, drain the history stream, assert `step.queued` appears before `step.started` with `AttemptNumber=1`.
- `TestOrchestrator_StepQueuedMsgIdIsDeterministic` — assert the Nats-Msg-Id matches `<run>.<step>.step.queued.1` exactly.
- `TestOrchestrator_DispatchProceedsIfQueuedPublishFails` — inject a publish error on the history stream, dispatch a task, assert the task is still on TASK_QUEUES (worker can pick it up), engine logged the publish failure but didn't roll back.

---

## §4. Engine handlers for `step.queued` + `step.started`

The orchestrator's `onEvent` switch at `internal/engine/orchestrator.go:250-310` gets two new cases. Both designed for monotonic state — they never roll a step backward (a stale `step.started` arriving after `step.completed` is ignored, not regressed).

### `step.started` handler

```go
case protocol.EventStepStarted:
    if err := o.handleStepStarted(ctx, evt); err != nil {
        return err
    }
```

```go
// handleStepStarted transitions the step from Queued to Running and
// updates the attempt counter. Monotonic: refuses to roll the state
// machine backward — a stale step.started arriving after the engine
// already saw step.completed/step.failed is logged and ignored.
func (o *Orchestrator) handleStepStarted(
    ctx context.Context, evt protocol.Event,
) error {
    if evt.RunID == "" {
        panic("handleStepStarted: evt.RunID must not be empty")
    }
    if evt.StepID == "" {
        panic("handleStepStarted: evt.StepID must not be empty")
    }

    state, err := o.loadRunState(ctx, evt.RunID)
    if err != nil {
        return err
    }
    step, ok := state.Step(evt.StepID)
    if !ok {
        slog.WarnContext(ctx,
            "step.started for unknown step",
            "run_id", evt.RunID,
            "step_id", evt.StepID,
        )
        return nil
    }

    // Monotonic guard — don't regress a terminal state.
    if step.Status == dag.StepStatusCompleted ||
        step.Status == dag.StepStatusFailed {
        slog.WarnContext(ctx,
            "stale step.started ignored — step is terminal",
            "run_id", evt.RunID,
            "step_id", evt.StepID,
            "current_status", step.Status,
            "event_attempt", evt.AttemptNumber,
        )
        return nil
    }

    step.Status = dag.StepStatusRunning
    if evt.AttemptNumber > step.Attempts {
        step.Attempts = evt.AttemptNumber
    }
    return o.persistRunState(ctx, state)
}
```

The exact method names (`loadRunState`, `persistRunState`, `state.Step`) are placeholders; implementer swaps to the actual helpers used by existing handlers (`onStepCompleted`, `onStepFailed`). In the current code these are `o.store.Load(ctx, runID)` (returns `dag.WorkflowRun`), `o.saveSnapshot(ctx, run)`, and `run.Steps[stepID]` (a value — mutate then re-assign).

**`StepState` does not have a `StartedAt` field** in the current `dag/types.go`. We don't add one in this PR — §8.4 explicitly scopes timestamp-on-state out, and the lifecycle event's own `Timestamp` carries the start time on the wire. If a future PR (e.g., #140's timer arming) needs a stored timestamp, that's its decision.

### `step.queued` handler

```go
case protocol.EventStepQueued:
    if err := o.handleStepQueued(ctx, evt); err != nil {
        return err
    }
```

```go
// handleStepQueued is mostly a no-op during normal operation — the
// engine's dispatch path already set Status to Queued before it
// emitted this event. The handler exists for state recovery on engine
// restart, where the history stream is replayed and the engine
// reconstructs run state from events alone. Same monotonic guard.
func (o *Orchestrator) handleStepQueued(
    ctx context.Context, evt protocol.Event,
) error {
    if evt.RunID == "" {
        panic("handleStepQueued: evt.RunID must not be empty")
    }
    if evt.StepID == "" {
        panic("handleStepQueued: evt.StepID must not be empty")
    }

    state, err := o.loadRunState(ctx, evt.RunID)
    if err != nil {
        return err
    }
    step, ok := state.Step(evt.StepID)
    if !ok {
        slog.WarnContext(ctx,
            "step.queued for unknown step",
            "run_id", evt.RunID, "step_id", evt.StepID,
        )
        return nil
    }
    if step.Status == dag.StepStatusCompleted ||
        step.Status == dag.StepStatusFailed ||
        step.Status == dag.StepStatusRunning {
        // already past Queued — don't roll back
        return nil
    }
    step.Status = dag.StepStatusQueued
    if evt.AttemptNumber > step.Attempts {
        step.Attempts = evt.AttemptNumber
    }
    return o.persistRunState(ctx, state)
}
```

### Why two separate handlers (not a combined dispatcher)

Each has a distinct monotonic precondition:
- `step.queued` is rejected by `Running`, `Completed`, `Failed` (anything ≥ Running).
- `step.started` is rejected only by `Completed`, `Failed` (terminal). Can transition from `Pending` or `Queued`.

A combined dispatcher would have to encode both rules and risk getting one wrong. Two small handlers, each with one job, is clearer and matches the existing pattern (`onStepCompleted`, `onStepFailed` are also separate).

### Tests

**Step-state handlers (load-bearing logic):**
- `TestOnEvent_StepStarted_TransitionsQueuedToRunning`
- `TestOnEvent_StepStarted_IncrementsAttempts`
- `TestOnEvent_StepStarted_IsIdempotentOnSameAttempt`
- `TestOnEvent_StepStarted_IgnoredAfterCompleted`
- `TestOnEvent_StepStarted_IgnoredAfterFailed`
- `TestOnEvent_StepStarted_AttemptsMonotonic_NeverDecreases` — seed `Attempts=5`, fire event with `AttemptNumber=2`, assert `Attempts` stays 5.
- `TestOnEvent_StepQueued_DuringReplay_ReconstructsState` — replay `[workflow.started, step.queued, step.started, step.completed]`, assert final state is `Completed` with `Attempts=1`.
- `TestOnEvent_StepQueued_NoRollback_FromRunning` — seed `Status=Running`, fire `step.queued`, assert state unchanged.

---

## §5. End-to-end lifecycle tests

The "the bug is fixed" tests, in `worker/` package (where embedded NATS + real orchestrator can co-locate per existing convention).

- **`TestEndToEnd_LifecycleEventsFire`** — start a workflow, register a worker that completes successfully. Drain the history stream. Assert event order: `workflow.started`, `step.queued (attempt 1)`, `step.started (attempt 1)`, `step.completed`, `workflow.completed`. With timestamps: `queued.Timestamp ≤ started.Timestamp ≤ completed.Timestamp`.
- **`TestEndToEnd_AttemptsVisibleDuringRun`** — the #137 repro. Start a long-running task (handler sleeps 2s). Sample run state during execution. Assert `Status=Running, Attempts=1` while in flight (transitioning from initial `Status=Queued, Attempts=0`). Bounded timeout.
- **`TestEndToEnd_RetryViaNakIncrementsAttempts`** — handler returns regular `error` first call, succeeds second. Assert run final state has `Attempts=2`, history contains both `step.started (attempt 1)` and `step.started (attempt 2)`. Exercises the NumDelivered → AttemptNumber path through the actual NAK redelivery cycle.

---

## §6. Test infrastructure note

`TestWorker_PublishStartedFailure_NaksAndRetries` requires injecting a JetStream publish failure. Two options:

1. Close the underlying NATS connection mid-test (existing `t.Cleanup` pattern can race).
2. Wrap the JS handle with an error-injecting decorator.

If neither is cheap (like Task 16's failure-mode tests in #136), the deferral fallback is the same: `t.Skip` with a reference to a follow-up issue. Plan should budget ~1 hour and pivot to skip-with-follow-up if injection escalates.

---

## §7. Forward-compat — what #140, #141, #147 need on top of this

Option-B promise from the brainstorming: spell out concretely what each follow-up looks like so they're additive, not redesigns.

### §7.1 #141 — fast-fail wedges (3-line follow-up)

**Root cause** (independent of #137): `worker/worker.go:767-774` — the regular-error branch of `handleTaskError` calls `NakWithDelay` but never publishes `step.failed`. Engine never observes the failure, just an eventual redelivery.

**Fix after #137 lands:** add `tc.publishFailed(msg, payload)` helper following the same per-event-type pattern as `publishStarted`, then call it from `handleTaskError`'s regular-error branch.

```go
// worker/context.go — NEW (parallel to publishStarted):
func (c *taskContext) publishFailed(
    msg jetstream.Msg, payload protocol.StepFailedPayload,
) error {
    if msg == nil {
        panic("publishFailed: msg must not be nil")
    }
    meta, err := msg.Metadata()
    if err != nil {
        return err
    }
    data, err := json.Marshal(payload)
    if err != nil {
        return err
    }
    evt := protocol.NewStepEvent(
        protocol.EventStepFailed, c.runID, c.stepID, data,
    )
    evt.WorkerID = c.workerID
    evt.AttemptNumber = int(meta.NumDelivered)
    /* same publish-msg pipeline as publishStarted */
}

// worker/worker.go — handleTaskError's regular-error branch:
slog.Error("task handler returned error, will retry", ...)
w.stepRetries.Add(context.Background(), 1)

if err := tc.publishFailed(msg, protocol.StepFailedPayload{
    Error:       handlerErr.Error(),
    FailureType: protocol.FailureTypeRetriable,
}); err != nil {
    slog.Error("failed to publish step.failed", "error", err, ...)
    // proceed with NAK regardless — engine missing the event is
    // fixable by replay; double-NAKing is fixable by msg dedup.
}

msg.NakWithDelay(5 * time.Second)
```

**What changes:** `step.failed` event now has `AttemptNumber` set, so msg-id is `<run>.<step>.step.failed.<N>` — distinct per attempt. Engine's existing `step.failed` handler processes the event. The `Complete` and `FailRetryAfter`/`FailPermanent` paths still go through the existing `publishEvent` (unchanged) — they don't need attempt-numbering yet because each fires only once per step.

**Tests:** end-to-end where handler returns regular error, assert: history contains `step.failed (attempt 1)` BEFORE the NAK redelivery, engine state shows `Attempts=1, Status=Failed` (or Queued for retry depending on engine's existing logic).

**Why not bundled with #137:** distinct subsystem (error-handling path vs. happy-path-start path), distinct test surface, mostly orthogonal correctness. Bundling adds review surface without simplifying anything.

### §7.2 #140 — step timeout doesn't fire

**Root cause:** No timer service. The engine doesn't watch elapsed time of an in-flight step against its declared `timeout`.

**Inputs #137 provides:**
- `step.started` event has `Timestamp` and `AttemptNumber` — engine knows when the attempt began.
- The `step.started` event itself carries `Timestamp` on the wire — #140's timer logic can read it directly from history (or the event handler can stamp a new `StartedAt` field on `StepState` if persistence is needed). Adding `step.Timeout` reading from the workflow definition gives all the inputs the deadline computation needs.

**What #140 needs to add (its own design):**

1. A timer/scheduler primitive. Two NATS-native options:
   - **`AckWait + msg.InProgress` heartbeat.** Set ConsumerConfig `AckWait` to step's timeout. Worker calls `tc.Heartbeat()` periodically during long handlers (already exists at `context.go:326`). If worker dies/hangs, AckWait expires, NATS redelivers, worker emits `step.started` for next attempt. Engine sees the gap. Bounded by `MaxDeliver` for total retry budget.
   - **Delayed event via KV TTL or sleep-stream.** Engine writes a "watchdog" entry to a KV with TTL = step.timeout. When TTL expires (KV watch), engine checks if step is still Running and triggers timeout handling. More machinery, more flexible.
2. Engine handler for "deadline crossed" — terminate or retry the step per workflow policy.

**Recommendation captured here, not decided:** `AckWait + Heartbeat` is the NATS-native path (CLAUDE.md priority) and reuses existing primitives. The KV-TTL path is more flexible but adds infrastructure. #140 evaluates.

**What #140 doesn't need:** changes to event protocol, worker dispatch flow, or engine's `step.started` handler. All built by this PR.

### §7.3 #147 — retry never re-dispatches

Distinct surface, deferred. The `AttemptNumber = NumDelivered` decision in §2 is robust to whichever retry model #147 lands on:

- If #147 keeps NATS-native retries (current path): NumDelivered already gives correct attempt counts on redelivery. Worker side stays as-is.
- If #147 moves to engine-driven re-publish: switch worker to read `payload.Attempt` (already exists in `protocol.TaskPayload`), one-line change. Engine's monotonic `max()` rule still produces correct attempts.

The engine's `step.started` handler doesn't care which model runs — it just consumes events and updates state monotonically.

### §7.4 What §7 is NOT

A contract for the follow-up PRs, not a commitment to ship them in order. Each follow-up files its own design conversation when it starts. This section just ensures #137's design doesn't paint #140/#141/#147 into corners.

---

## §8. Risk, rollback, PR checklist

### §8.1 Risk inventory

| Risk | Surface | Mitigation |
|---|---|---|
| Adding `AttemptNumber` field breaks JSON consumers that strictly typecheck unknown fields | `protocol.Event` deserialization elsewhere | `omitempty` preserves old wire format. Go's default JSON unmarshal ignores unknown fields. `TestEvent_UnmarshalLegacyMissingAttempt` guards. |
| `step.queued` + `step.started` doubles event volume on history stream | History stream size growth | Events are ~200 bytes each. Stream retention policy unchanged. Worth measuring on a realistic workload but not blocking. |
| Stale `step.started` events arrive after engine has already seen `step.completed` (out-of-order delivery, replay-during-live-run) | Engine state corruption — could regress to `Running` from terminal | Monotonic guard in handler: reject any started event when current status is `Completed` or `Failed`. Unit test guards. |
| Worker `step.started` publish failure NAKs the message → infinite retry loop if NATS publish stays broken | Worker stuck redelivering same task forever | Bounded by `MaxDeliver` from ADR-006 (currently unlimited). In practice publish failures are transient. Original #137 symptom (`queued` forever) reappears in this edge case — same UX as today, no regression. |
| `NumDelivered = AttemptNumber` conflates AckWait redelivery (worker crashed) with NAK retry (handler failed) | Operator sees `attempts=2` after worker crash even though handler never failed | Documented in §2 + §7.3. Tasks are idempotent so no correctness issue. Distinct event types (`step.crashed` vs `step.failed`) is a future refinement, not blocking. |
| Engine's `step.queued` publish failure is silent (no rollback) | Operators see `started` without preceding `queued` event | Asymmetric failure-mode is deliberate (§3) — task is already on the queue, rolling back is fragile. Logged at ERROR. Rare in practice. |

### §8.2 Rollback

The PR is contained to `protocol/`, `worker/`, and `internal/engine/`. **Rollback is clean — no NATS state migration required.**

- `protocol.Event.AttemptNumber` field added with `omitempty`. Old code reading new events ignores unknown field (standard Go JSON unmarshal). New code reading old events sees `AttemptNumber=0`.
- New events on the history stream (`step.queued`, `step.started`) post-revert: old code's `onEvent` switch has no case for these types, falls through to default branch (verify in implementation). Worst case: old code logs "unknown event type" warnings. Not corruption.
- Engine state in KV (`Step.Attempts`): post-fix runs may have `Attempts > 0` while in `Status=Running` (driven by `step.started`). Pre-fix code expects `Attempts > 0` only after a failure. Slightly inconsistent display but no functional break — engine's `step.completed`/`step.failed` handlers still function on these state values.

**Operational rollback procedure:** none. Just revert the merge.

### §8.3 PR checklist

- [ ] `protocol.Event.AttemptNumber` added with `omitempty` JSON tag.
- [ ] `protocol.NATSMsgID()` includes attempt suffix when `AttemptNumber > 0`, tested with `TestNATSMsgID_*` cases.
- [ ] `worker.taskContext.publishStarted(msg)` method added. Reads `NumDelivered` from msg metadata, sets `AttemptNumber`, publishes step.started. Existing `publishEvent` unchanged.
- [ ] Worker publishes `step.started` at `worker/worker.go:~707` (before `handler(tc)` invocation), with `attempt = msg.Metadata().NumDelivered`. Failure-mode is log + NAK with 1s delay.
- [ ] Engine publishes `step.queued` at the dispatch site (post-`PublishMsg` to TASK_QUEUES). Failure-mode is log + proceed (no rollback). New helper `internal/engine/event_publisher.go`.
- [ ] Engine `onEvent` switch (`internal/engine/orchestrator.go:250-310`) gains cases for `EventStepQueued` and `EventStepStarted`, each routing to a dedicated handler with monotonic guards.
- [ ] All tests from §1–§5 land. End-to-end §5 tests pass.
- [ ] Original #137 repro: `dagnats run status` shows `Status=Running, Attempts=1` while a task is mid-execution. `dagnats run events <run>` shows full lifecycle including `step.queued` and `step.started`.
- [ ] If `TestWorker_PublishStartedFailure_NaksAndRetries` (publish-failure injection) is hard to wire, defer to a follow-up issue per §6. File the issue, reference from the test file.
- [ ] Branch is feature, not main. PR awaits manual merge per global CLAUDE.md.
- [ ] Local CI (Go test + vet + gofmt) green. Note: known pre-existing flake `TestSuperclusterTopologyFormed` may surface; re-run if it does.

### §8.4 What §8 deliberately doesn't cover

- Implementation sequencing — that's the writing-plans phase, next.
- Performance benchmarks for the doubled history-stream volume — none warranted up front; revisit if real workloads show it.
- Whether `step.completed`/`step.failed` should also gain `AttemptNumber` — out of scope; #141's follow-up handles `step.failed` extension naturally.

---

## Appendix A — File-level changes

For orientation, not as a prescription. Writing-plans owns the actual sequencing.

- `protocol/protocol.go` — add `AttemptNumber int` field to `Event`, update `NATSMsgID()` to include suffix.
- `protocol/protocol_test.go` — append `TestNATSMsgID_*`, `TestEvent_MarshalRoundTrip_PreservesAttemptNumber`, `TestEvent_UnmarshalLegacyMissingAttempt`.
- `worker/context.go` — add new `publishStarted(msg)` method. **Existing `publishEvent` is unchanged.**
- `worker/worker.go` — insert 3-line `tc.publishStarted(msg)` call between current lines 706-707, with NAK-on-failure path. The helper hides metadata + AttemptNumber + publish.
- `worker/lifecycle_event_test.go` (new) or append to existing `worker/consumer_subscribe_test.go` — `TestWorker_PublishesStepStartedBeforeHandler`, `TestWorker_PublishStartedFailure_NaksAndRetries`, `TestWorker_AttemptNumberFromNumDelivered`, `TestPublishEvent_AppliesAttemptNumber`.
- `internal/engine/event_publisher.go` (new) — `publishLifecycleEvent` helper.
- `internal/engine/orchestrator.go` — add `step.queued` publish at dispatch site; add `EventStepQueued` and `EventStepStarted` cases to `onEvent` switch; new methods `handleStepQueued` and `handleStepStarted`.
- `internal/engine/orchestrator_test.go` — append `TestOnEvent_StepStarted_*`, `TestOnEvent_StepQueued_*`, `TestOrchestrator_PublishesStepQueuedOnDispatch`, `TestOrchestrator_StepQueuedMsgIdIsDeterministic`, `TestOrchestrator_DispatchProceedsIfQueuedPublishFails`.
- End-to-end tests — `worker/lifecycle_e2e_test.go` (new) or append to existing — `TestEndToEnd_LifecycleEventsFire`, `TestEndToEnd_AttemptsVisibleDuringRun`, `TestEndToEnd_RetryViaNakIncrementsAttempts`.

## Appendix B — Source citations (for the implementer)

- The bug location: `internal/engine/orchestrator.go:250-310` (`onEvent` switch — no case for `EventStepStarted` or `EventStepQueued`).
- Event types defined but unused: `protocol/protocol.go:58-59` (`EventStepQueued`, `EventStepStarted`).
- Worker dispatch flow: `worker/worker.go:680-725` (`handleMessage`).
- `"executing task"` log site: `worker/worker.go:697`.
- Existing event publish helper: `worker/context.go:291-322` (`publishEvent`).
- Existing event publish callsites: `worker/context.go:113` (Completed), `worker/context.go:223` (Failed).
- Step state model: `dag/types.go` — `StepStatus` enum (`Pending`, `Queued`, `Running`, `Completed`, `Failed`), `StepState.Attempts`.
- Internal Queued transition (no event today): `internal/engine/orchestrator.go:1492`.
- Worker error-handling regular-error branch (#141's gap): `worker/worker.go:767-774`.
- GitHub issue: [#137](https://github.com/danmestas/dagnats/issues/137).
- Related cluster: [#140](https://github.com/danmestas/dagnats/issues/140), [#141](https://github.com/danmestas/dagnats/issues/141), [#147](https://github.com/danmestas/dagnats/issues/147).

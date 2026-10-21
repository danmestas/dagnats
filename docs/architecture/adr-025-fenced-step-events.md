# ADR-025: Fenced step events

**Status:** Accepted (2026-10-21)
**Deciders:** Dan Mestas
**Supersedes:** the checkpoint mechanics of ADR-002 (the per-iteration model stays)
**Issue:** #801

## Context

An agent step runs as an `agent_loop`. Each iteration the handler calls
`Checkpoint` (a plain KV `Put` on `checkpoints/{run}.{step}`) and then
`Continue` (a `step.continue` publish). The engine increments the iteration
and queues the next task. Three bugs follow, each reproduced by an
integration test that fails every run:

1. **A crash between `Checkpoint` and `Continue` repeats a turn.** The
   redelivered task for iteration N loads the state the crashed attempt
   already advanced to N+1.
2. **A duplicate `step.continue` advances the loop twice.** The event carries
   no iteration, and `advanceContinue` increments unconditionally. Two tasks
   are queued and two workers run the loop.
3. **A stalled worker can complete a step after another worker took it
   over.** `handleStepCompleted` only checks for a terminal run. Because
   `step.completed` message IDs are `{run}.{step}.{type}.{attempt}`, the same
   for every iteration, JetStream deduplication can also drop the real
   completion as a duplicate of the stale one.

The dispatch nonce (#380, #626) already exists on every task, but it only
fences control-plane calls. JetStream redelivers the same task message to a
replacement worker, so a stalled worker and its replacement share the nonce
and attempt.

## Decision

Lifecycle events carry the identity of the dispatch they belong to, the
engine drops anything that does not match the step's current dispatch, and
an agent's state travels inside its `step.continue` event so that saving and
continuing are one write.

### Event identity

`step.continue`, `step.completed` and `step.failed` carry `iteration`,
`attempt` and `dispatch_nonce`. So does the synthetic `step.failed` the step
timeout watchdog publishes. The fields are additive. Their presence is
detectable (pointer fields or an event schema version), because iteration 0
from a new worker must not look like a field an old worker left out.

### Acceptance rule

The engine accepts a lifecycle event only when:

- the step is Running;
- the event's iteration equals the step's iteration;
- the event's attempt equals the step's attempt;
- the event's dispatch nonce, when present, equals the step's.

Anything else is dropped and counted in a metric. A dropped event is never
an error: it is the expected outcome for stale and duplicate events.

### Stable message IDs

Lifecycle message IDs become `{run}.{step}.{type}.{attempt}.{iteration}`.
The UnixNano suffix on `Continue`'s ID goes away. A redelivery inside the
history stream's dedup window is caught by JetStream; one outside it is
caught by the acceptance rule.

### State in the continue event

`Checkpoint` buffers the state and `Continue` sends it in the event. The
engine stores it in the step's state in the run snapshot, in the same
compare-and-swap save that advances the iteration, and hands it to the next
task. Retries (same iteration, new attempt) and dispatch recovery read it
from the snapshot, so a retry resumes from the last committed state as
`LoadCheckpoint` does today. `on_failure` and compensation steps resolve
their own input and never see loop state.

State is capped per step, 256 KiB by default and configurable, because the
whole snapshot is one KV value bounded by the server's `max_payload`
(1 MiB by default). Over the cap, `Continue` fails with an error naming the
size and the cap. Larger state belongs in the agent journal (ADR-026).

### Dispatch recovery on the continue path

The continue path sets `DispatchUnsent` on its snapshot save and clears it
once the next task is published, as `enqueueReady` does (#794). Without
this, the acceptance rule would strand a loop after an engine crash between
the save and the publish: the redelivered `step.continue` is now correctly
dropped, and nothing else would publish the next task.

### Durable `loop_delay`

`loop_delay` is scheduled on `SLEEP_TIMERS`, like retry backoff, instead of
an in-memory timer that an engine restart loses.

### Compatibility

Events that carry identity fields are always fenced. Events without them,
from workers and bridge clients built before this change, are accepted by
default: `legacy_step_events` defaults to true, and the engine logs a warning
at startup while it is on. Downstream services pin older worker SDKs, and
dropping their events would strand every run they work on. While the flag
is on, bug 2 remains possible for those old workers. Operators set it to
false once every worker is upgraded.

Engines must be upgraded before workers. A new worker detects an old engine
(its task payload carries no state cap) and also writes the legacy
checkpoint key, so a mixed rollout keeps loop state either way.

Map instances and `respond` steps are not fenced: a map instance has no
per-instance dispatch state to compare against, and a `respond` step is
resolved by the engine and saved before its event.

## Requirements

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in
all capitals.

| ID | Requirement | Proven by |
|---|---|---|
| R1 | A worker MUST include `iteration`, `attempt` and `dispatch_nonce` on every lifecycle event, and the engine MUST include them on the step timeout watchdog's synthetic `step.failed`. | `TestLifecycleEventsCarryIdentity` |
| R2 | The engine MUST NOT apply a lifecycle event unless the step is Running and the event's iteration and attempt equal the step's. | `TestAcceptStepEvent` (table) |
| R3 | The engine MUST NOT apply a lifecycle event whose present dispatch nonce differs from the step's. | `TestAcceptStepEvent` (table) |
| R4 | A duplicate `step.continue` for one iteration MUST advance the loop at most once. | `TestStepContinue_DuplicateForSameIterationAdvancesOnce` |
| R5 | A stale worker's terminal event MUST NOT complete or fail a step that has moved on. | `TestAgentLoop_StaleWorkerCompleteAfterTakeoverIsRejected` |
| R6 | A redelivered iteration MUST load the state committed before it, not state its crashed attempt wrote. | `TestAgentLoop_RedeliveredIterationSeesPreIterationCheckpoint` |
| R6a | A retry of an iteration (same iteration, new attempt) MUST load the last state committed by `Continue`. | `TestStepContinue_RetryResumesFromCommittedState` |
| R7 | An engine crash between the continue save and the next publish MUST NOT strand the loop. | `TestStepContinue_CrashBetweenSaveAndPublishRecovers` |
| R8 | `Continue` with state over the cap MUST fail with an error naming the size and the cap. | `TestContinue_OverCapStateFailsBeforePublishing`, `TestStepContinue_OverCapStateFailsLoop` |
| R9 | A `loop_delay` MUST survive an engine restart. | `TestStepContinue_LoopDelaySurvivesEngineRestart` |
| R10 | With `legacy_step_events` off, the engine MUST drop lifecycle events that lack identity fields. | `TestStepCompleted_LegacyEventAcceptedOnlyWithFlag` |
| R11 | The step timeout watchdog MUST fire for an agent loop regardless of how many iterations have passed. | `TestStepTimeout_FiresForLoopAfterContinues` |

## Alternatives considered

- **The agent journal (ADR-026) for these bugs.** It fixes them too, but
  needs a new stream and protocol. Carrying the state in the event fixes all
  three with one write and no new infrastructure.
- **Leases (ADR-024) to fence workers.** The engine would verify a token on
  every event and holders would renew on a clock. The dispatch identity is
  already on every task, so it costs nothing.
- **An object store for large state.** Deferred: it adds a store with no
  garbage collection story. The journal handles large state instead.

## Consequences

- Duplicate and stale events become no-ops for every step type, not only
  agents.
- A stalled worker and its replacement still share a nonce and attempt, so
  for plain steps the first accepted result wins (at-least-once execution,
  exactly-once acceptance). Telling the two apart needs the journal.
- Agent state larger than the cap needs the journal.
- `Continue(output)` no longer drops its payload on the floor; the payload is
  the next iteration's state.

## Security considerations

The dispatch nonce is not a secret against a worker that already holds the
task: it rides in the task payload. It stops a worker of a different run or
step from completing a step it never claimed. Workers are authenticated by
NATS credentials or worker tokens as before; this ADR adds no new trust.
State in the continue event is stored in the run snapshot and history with
the same retention and access as step outputs today, so handlers MUST NOT
put secrets in it.

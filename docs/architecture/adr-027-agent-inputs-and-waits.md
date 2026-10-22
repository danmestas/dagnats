# ADR-027: Agent inputs and durable waits

**Status:** Accepted (2026-10-21)
**Deciders:** Dan Mestas
**Depends on:** ADR-026
**Issue:** #806

## Context

A running agent cannot receive input today. Signals hold one value per name,
are never consumed, and `WaitForSignal` holds the worker's claim for at most
an hour. An agent that waits for a person holds a worker the whole time, and
two messages sent quickly overwrite each other.

pi-durable accepts inputs as submissions keyed by a caller-supplied request
ID, queues them while the agent is busy, and places them at turn boundaries:
a steer joins the current run after a tool round, a follow-up starts the next
run after a final answer.

## Decision

### Submitting

`POST /runs/{run}/steps/{step}/inputs` takes a required `request_id`, the
content (JSON), and a mode of `steer` or `follow_up`. `GET` on the same path
plus the request ID returns its status, with an optional long-poll (`wait`,
and `after`, the status the caller last saw). `DELETE` withdraws it while
still queued. The same operations exist as NATS micro endpoints
(`api.inputs.submit`, `api.inputs.get`, `api.inputs.withdraw`).

A submission is created with a KV `Create` on the `submissions` bucket,
keyed `{run}.{step}.{request_id}`. A repeat with the same request ID returns
the existing submission, whatever its content. This holds beyond a stream's
deduplication window, which is seconds to minutes; the bucket's TTL is seven
days. The bucket is provisioned by `natsutil.SetupAll` at the cluster's
replica count, with a one-percent share of the store budget taken from the
headroom under the 85% guard, as `AGENT_JOURNAL`'s was. The status moves
from queued to placed to done, or from queued to unanswered.

After creating the submission the API publishes `step.input.queued` on the
run's history, keyed by the request ID, so the engine learns of it through
the one consumer it already has.

### Placing

The SDK reads queued inputs at turn boundaries: `PlaceInputs(ModeSteer)`
after a tool round, `PlaceInputs(ModeFollowUp)` after a final answer. It
needs the step's journal. Placement is an `input.placed` record in the
journal (ADR-026), carrying the content, so the journal is the source of
truth and the KV status a projection. The journal knows every request ID
placed in any iteration, so a redelivery, or the next iteration, never
places one twice: one found placed is skipped and its status repaired.
When the iteration commits, the inputs it placed are projected to done.

A status that disagrees with the journal is repaired from it on read: a
queued or placed submission whose journal holds an `input.placed` record is
placed, and done once a commit follows the record; a queued submission on a
finished step is unanswered. Inputs are enumerated from the bucket's stream
(`natsutil.ListKeysUnder`), not `ListKeys`, which can miss keys written
concurrently.

### Waiting

`Continue` takes options: `Delay(d)`, `Until(t)` and `UntilInput()`. They are
variadic options on the existing `Continue(output []byte, opts
...ContinueOption)` rather than a second method or a wait set on the
context beforehand: every existing caller compiles unchanged, the wait is
visible at the one call that commits it, and the options combine (a time and
`UntilInput()` wake on whichever comes first). The wait rides the
`step.continue` payload (`wait`) and the journal's commit record, so a
republished commit asks for the same wait.

The engine saves the wait on the step (`wait_until`, `wait_input`) in the
same save that advances the iteration. A time is a `loop_wait` timer on
`SLEEP_TIMERS`, which publishes `step.wait.elapsed` naming the dispatch it
was armed for; an input wait is the saved flag itself, woken by
`step.input.queued`. Both wakeups are history events handled under the run
lock: the engine clears the flags, saves, and sends the iteration. A wakeup
that finds the flags cleared, or another iteration or nonce, is stale and
dropped. A resend after an engine crash rebuilds the wait from the saved
state. No worker holds a claim while the step waits, and the step's
`loop_delay` does not apply to an iteration that waits.

### No lost wakeups

An input can arrive while the turn that will commit a wait is still running.
Three rules close the gap:

1. The SDK refuses to commit `UntilInput()` while inputs are queued
   (`ErrInputsQueued`): the handler places them and runs another turn.
2. The engine checks for queued inputs when it enters the wait and, finding
   any, sends the iteration instead of waiting. An input created after that
   check is its own event, which lands on the history after the continue
   and so is handled after the wait is saved, however many engines share
   the consumer: a concurrent engine that handles it first marks the step
   as having inputs, and the continue's revision-checked save then retries
   and finds them.
3. An input submitted after the step has finished is settled unanswered.
   The save that ends a step with inputs (`has_inputs`, set by the first
   `step.input.queued` for it) sweeps what is still queued; one that arrives
   later is settled by its own event; and the API re-reads the step from
   the stream leader after creating a submission and settles it itself when
   the step finished in between.

## Requirements

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in
all capitals.

| ID | Requirement | Proven by |
|---|---|---|
| R1 | A submission MUST be created at most once per `request_id` per step. | `TestSubmit_CreatesOnceAndRepeatReturnsOriginal`, `TestRESTInputs_SubmitOnceAndRepeatReturnsOriginal` |
| R2 | An input MUST be placed at most once, across any crash schedule. | `TestInputs_ExactlyOnceAcrossCrashAndRedelivery`, `TestPlaceInput_ReopenedJournalKnowsPlacements`, journal checker invariant 5 |
| R3 | Every accepted submission MUST end done or unanswered. | `TestContinueWait_CompletionSweepSettlesQueuedInputs`, `TestRESTInputs_FinishedStepRefusesAndSettles`, `TestCheckInputs_FinishedStepLeavesNothingQueued` |
| R4 | A step waiting for input MUST wake when an input is queued, including one queued while the wait was being committed. | `TestContinueWait_UntilInputWakesOnSubmission`, `TestContinueWait_InputQueuedDuringCommitWakesTheStep`, `TestInputs_ContinueUntilInputRefusesWhileQueued` |
| R5 | A waiting step MUST NOT hold a worker claim. | The wait tests: the task is acknowledged before the wait, no task is in flight until the wake |
| R6 | Delays and deadlines MUST survive an engine restart. | `TestContinueWait_DelaySurvivesEngineRestart` |
| R7 | A withdrawn submission MUST NOT be placed, and a placed one MUST NOT be withdrawn. | `TestWithdraw_OnlyWhileQueued`, `TestRESTInputs_WithdrawOnlyWhileQueued` |

## Alternatives considered

- **Extend signals.** Signals are a single value per name with no
  consumption or ordering. Reusing the name would change their meaning for
  existing callers.
- **The events stream and `wait_for_event`.** Those are workflow-level and
  engine-resolved; inputs are addressed to one running step and must be
  placed by its handler.
- **JetStream dedup alone for request IDs.** A stream's dedup window is
  seconds to minutes; users retry later than that.
- **A KV watch in the engine as the waiter.** Not durable across a restart
  and one watcher per engine; a history event is handled once by whichever
  engine holds the consumer, and is replayed after a restart.
- **The timer sending the task itself, like `loop_delay`.** The timer cannot
  tell that an input already woke the step; only the engine's saved flags
  under the run lock can, and a duplicate task outside the dedup window
  would run an iteration twice.
- **Sweeping on every terminal save.** One listing per finished step per
  save; the `has_inputs` mark keeps the cost to steps someone wrote to.

## Consequences

- Agents can hold a conversation with people and other agents without
  holding a worker.
- A new bucket, two history event types, a timer action and three
  endpoints to document and secure.
- A step's `timeout` and a loop's `max_duration` keep counting while the
  step waits: a step that waits for a person should carry neither, or a
  bound that allows for the wait.
- Signals stay as they are.

## Security considerations

Submitting input to a step is a write into a running agent's context and
MUST require the same authorization as signalling the run. The input
endpoints are served by the same REST router and micro service as
`POST /runs/{id}/signal/{name}`, so whatever fronts one fronts the other;
neither has finer-grained authorization today. Inputs are model input: a
submission from an untrusted party is a prompt-injection vector, and the
step's tools run with the step's permissions. A submission is at most
1 MiB and its `request_id` is restricted to `[A-Za-z0-9_-]`, so it cannot
shape a KV key or an event msg-id beyond its own slot.

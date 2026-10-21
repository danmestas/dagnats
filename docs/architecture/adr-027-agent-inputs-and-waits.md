# ADR-027: Agent inputs and durable waits

**Status:** Proposed (2026-10-21)
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
content, and a mode of `steer` or `follow_up`. `GET` on the same path plus
the request ID returns its status, with an optional long-poll. `DELETE`
withdraws it while still queued. The same operations exist as NATS micro
endpoints.

A submission is created with a KV `Create` on the `submissions` bucket,
keyed `{run}.{step}.{request_id}`. A repeat with the same request ID returns
the existing submission. This holds beyond a stream's deduplication window,
which is seconds to minutes. The status moves from queued to placed to done or unanswered.

### Placing

The SDK reads queued inputs at turn boundaries: steers after a tool round,
follow-ups after a final answer. Placement is an `input.placed` record in
the journal (ADR-026), so the journal is the source of truth and the KV
status is a projection: a status that disagrees with the journal is repaired
from it. Inputs are enumerated from the bucket's stream, not `ListKeys`,
which can miss keys written concurrently.

### Waiting

`Continue` gains three options: `Delay(d)`, `Until(t)` and `UntilInput()`.
The engine schedules the next iteration on `SLEEP_TIMERS` for the first two
and registers a waiter for the third. No worker holds a claim while the step
waits.

### No lost wakeups

An input can arrive while the turn that will commit a wait is still running.
Three rules close the gap:

1. The SDK refuses to commit `UntilInput()` while inputs are queued.
2. The engine checks for queued inputs when it enters the wait and wakes the
   step at once if there are any.
3. An input submitted after the step has finished is settled as unanswered
   by a sweep when the step completes.

## Requirements

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in
all capitals.

| ID | Requirement | Proven by |
|---|---|---|
| R1 | A submission MUST be created at most once per `request_id` per step. | Inputs exactly-once test |
| R2 | An input MUST be placed at most once, across any crash schedule. | Crash-point tests, journal checker |
| R3 | Every accepted submission MUST end done or unanswered. | Journal checker |
| R4 | A step waiting for input MUST wake when an input is queued, including one queued while the wait was being committed. | Lost-wakeup test |
| R5 | A waiting step MUST NOT hold a worker claim. | Wait tests |
| R6 | Delays and deadlines MUST survive an engine restart. | Restart tests |

## Alternatives considered

- **Extend signals.** Signals are a single value per name with no
  consumption or ordering. Reusing the name would change their meaning for
  existing callers.
- **The events stream and `wait_for_event`.** Those are workflow-level and
  engine-resolved; inputs are addressed to one running step and must be
  placed by its handler.
- **JetStream dedup alone for request IDs.** A stream's dedup window is
  seconds to minutes; users retry later than that.

## Consequences

- Agents can hold a conversation with people and other agents without
  holding a worker.
- A new bucket and three endpoints to document and secure.
- Signals stay as they are.

## Security considerations

Submitting input to a step is a write into a running agent's context and
MUST require the same authorization as signalling the run. Inputs are model
input: a submission from an untrusted party is a prompt-injection vector,
and the step's tools run with the step's permissions.

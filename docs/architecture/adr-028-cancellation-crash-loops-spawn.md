# ADR-028: Cancellation, crash loops and idempotent spawn

**Status:** Proposed (2026-10-21)
**Deciders:** Dan Mestas
**Depends on:** ADR-025; ADR-026 for the crash-loop guard and the per-append cancel check
**Issues:** #802, #803, #807

## Context

Three gaps let an agent's work escape its run:

- **Cancel never reaches a running handler.** The handler's context is the
  tracing span's context and is never cancelled. Cancel cascades to child
  runs, but a handler already running finishes its model and tool calls.
- **A crashing worker loops forever.** Task consumers redeliver without
  limit, and a worker that dies before publishing `step.failed` never uses
  up its retry budget. Only an optional step timeout stops it.
- **A replayed spawn makes a second child.** `api.runs.spawn` mints a fresh
  random child run ID, so a tool that spawns a child and is replayed after a
  crash spawns again.

## Decision

### Cancellation reaches the handler

`tc.Context()` is cancelled when the run is cancelled. On cancel the engine
publishes `dagnats.cancel.{run}` on core NATS. Because that is at-most-once,
the worker also checks run state on each heartbeat and, for journal-enabled
steps, on each journal append. A plain handler returns when `ctx` is
done; a journal-enabled handler also writes an aborted commit. Events it publishes afterwards are dropped by the terminal-run guard. The
bridge reports cancellation on the next resolve or heartbeat.

Abort order stays top-down: the parent run finalizes and its children are
cancelled through the existing cascade. pi-durable waits for owned work to
drain first, so an abort handler can see its children's outcomes; dagnats
has no abort handler that needs this, and late child events are already
dropped.

### Crash-loop guard

A journal-enabled step fails with reason `restart_limit` after
`restart_limit` consecutive `claim` records for one iteration with nothing
else between them (default 5). A `pause` record counts as progress, so
pausing never trips the guard. The existing run retry resumes from the
journal.

JetStream's delivery count cannot stand in for this: `Pause` is a delayed
negative acknowledgement, so it redelivers too. Steps without a journal keep
the step timeout as their backstop.

### Idempotent spawn

`api.runs.spawn` takes an optional `key`. The child run ID is claimed with a
KV `Create` on a hash of (parent run, parent step, key); a repeat returns
the existing child. The spawn publish is deduplicated by child run ID, so a
call that claimed the ID but failed to publish can be retried. The SDK's
tool helper (ADR-026) defaults the key to the tool call's idempotency key.

## Requirements

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in
all capitals.

| ID | Requirement | Proven by |
|---|---|---|
| R1 | A running handler's context MUST be cancelled within a bounded time of its run being cancelled. | Cancel test, #802 |
| R2 | A worker MUST NOT rely on the core cancel signal alone. | Missed-signal test, #802 |
| R3 | A journal step MUST fail with `restart_limit` after the configured number of claims without progress. | Crash-loop test, #807 |
| R4 | Pausing MUST NOT count toward `restart_limit`. | Pause test, #807 |
| R5 | Two spawns with the same key MUST produce one child run. | Spawn tests, #803 |
| R6 | A spawn that claimed its child ID but failed to publish MUST produce exactly one child when retried. | Spawn tests, #803 |

## Alternatives considered

- **Park instead of fail at the restart limit**, as pi-durable does. dagnats
  already has run retry; a new parked state would need its own console and
  API surface.
- **`MaxDeliver` on task consumers.** Counts pauses, and the consumer
  configuration deliberately leaves retry budgets to the engine.
- **Interrupting handlers by killing the worker process.** Loses every other
  task on that worker.

## Consequences

- Handlers MUST honour `ctx` for cancel to take effect; a handler that
  ignores it behaves as today.
- Crash loops end with a visible failure instead of silent redelivery.
- Spawning from tools becomes safe to replay.

## Security considerations

The cancel subject carries only a run ID. A NATS client allowed to publish
on `dagnats.cancel.>` could make workers abandon work early, though not
change run state; deployments that grant broad publish rights should
restrict that subject to the engine. Spawn keys are hashed before use as KV
keys, so callers cannot inject subject tokens.

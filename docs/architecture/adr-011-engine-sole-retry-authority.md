# ADR-011: Engine as Sole Retry Authority — Failure Path Fixes

**Status:** Accepted (2026-05-02)
**Deciders:** Dan Mestas
**Closes:** [#141](https://github.com/danmestas/dagnats/issues/141), [#147](https://github.com/danmestas/dagnats/issues/147), [#140](https://github.com/danmestas/dagnats/issues/140)
**Related:** ADR-006 (durable consumers), ADR-009 (actor removal — single orchestrator), PR #137 (lifecycle events / Attempts via max() rule)

## Context

Three production bugs in the same code path — worker failure → engine
retry → step timeout — all surfaced from a single triage of a wedged
run. Each was a distinct root cause, but they share design assumptions
that didn't hold up:

1. **#147 — Retriable step.failed never retried.** `handleStepFailed`
   incremented `Attempts` and saved the snapshot, then returned. The
   "existing backoff behavior" comment was load-bearing — there was no
   backoff scheduler. Retry policies for transient failures were
   non-functional in production.

2. **#141 — Generic worker error wedged the run.** `handleTaskError`
   for any non-typed error called `msg.NakWithDelay(5*time.Second)` and
   emitted nothing to the engine. JetStream redelivered, the worker
   failed again, NAK again — the engine never saw `step.failed`,
   `attempts` stayed at `0/N`, and the run sat in `running` forever.

3. **#140 — Step timeout never fired.** `StepDef.Timeout` was a real
   field on the type and documented in workflow JSON, but no engine
   path scheduled a watchdog. A wedged worker held its task forever.

The bugs interacted: #141 prevented `step.failed` from arriving;
#147 prevented retries when it did; #140 left no escape if a worker
hung. Together they made wedged runs a routine operational problem
that required manual `dagnats run cancel`.

## Decision

**Engine becomes the sole retry authority.** Workers report failures;
the engine decides when (and whether) to retry. Three coordinated
changes:

### 1. Retry-backoff scheduler (closes #147)

Add `TimerActionRetryBackoff` and `o.scheduleRetryBackoff(...)`,
modeled after the existing `scheduleRetryAfter`. After a retriable
`step.failed` with attempts remaining, the engine schedules a
`SLEEP_TIMERS` entry whose delay comes from `dag.CalculateDelay(policy,
attempt)` — the same calculator used everywhere else for backoff. On
fire, the timer re-publishes the task with the next attempt number.

The fire path is shared with `TimerActionRetryAfter` via a
`republishTask(tm, kind)` helper: only the dedup-MsgId suffix differs
(who chose the delay — worker for RetryAfter, policy for Backoff).

**MsgId fix as a side effect:** `Schedule` now embeds the timer Action
in the dedup MsgId (`{run}.{step}.{attempt}.{action}` instead of
`{run}.{step}.{attempt}.sleep`). Without this, scheduling a step
timeout and a retry backoff for the same `(run, step, attempt)` would
collide on dedup and the second `Schedule` would silently no-op.

### 2. Worker generic-error path publishes step.failed (closes #141)

In `handleTaskError`, the generic Go error branch now calls
`tc.Fail(err)` (publishes `step.failed` with `failure_type=retriable`)
and `msg.Ack()`s the original message. Engine retry kicks in via the
scheduler from #147.

Fallback: if `tc.Fail` itself errors (NATS unreachable mid-task), NAK
with the old 5s delay so JetStream eventually retries. Acking with no
published `step.failed` would re-introduce the wedge symptom.

The hardcoded 5s NAK delay no longer overrides the policy's
`initial_delay`/`multiplier`/`max_delay`. Workers stop being a
parallel retry authority that silently shadowed engine policy.

### 3. Step-timeout watchdog (closes #140)

Add `TimerActionStepTimeout` and `o.scheduleStepTimeout(...)`. In
`handleStepStarted`, when a step transitions to Running, if
`stepDef.Timeout > 0` schedule a watchdog timer for that duration. On
fire, `fireStepTimeout` reloads the run and only acts if the step is
still Running on the same Attempts count that was current when we
scheduled. Otherwise the fire is stale (the step already completed,
failed, or moved to a new attempt) and we drop it.

When the timer is live, the engine publishes a synthetic `step.failed`
with `failure_type=retriable` and an error message of
`"step timeout exceeded (Xs)"`. The synthetic event sets
`AttemptNumber = tm.Attempt`, so its dedup MsgId matches what a
worker `step.failed` for the same attempt would have used — concurrent
worker-completes-while-timer-fires cases collapse via dedup.

Retriable was deliberate: a timeout is most often transient. Workflow
authors who want hard-fail on timeout set `retry: {max_attempts: 1}`.

## Consequences

- **Single source of truth.** Engine owns Attempts incrementing,
  retry timing, and timeout enforcement. Worker reports facts,
  doesn't make policy decisions.

- **Worker NAK loops disappear from the happy retry path.** NAK
  remains as a fallback for "publish step.failed itself failed" and
  for typed `RateLimitError` (which still uses `FailRetryAfter`).

- **dag.CalculateDelay is now the universal backoff.** Workers no
  longer use a hardcoded 5s; whatever the policy says, the policy
  gets.

- **Step-level timeout is enforceable.** Workflow authors can set
  per-step timeouts in JSON and they actually fire.

- **Compounding fixes:** #140 only works because #147 schedules the
  retry; #141 only works because #147 dispatches what it publishes.
  All three had to land together to ship a coherent failure path.

- **Three new TimerActions** (`RetryBackoff`, `StepTimeout`) plus a
  bug fix to dedup MsgId scoping. Future timer kinds for the same
  `(run, step, attempt)` are now safe.

- **Tests:** four new e2e files exercising real engine + real worker
  + real NATS:
  - `worker/retry_backoff_e2e_test.go` — exponential backoff retries
    until success, max-attempts exhaustion, no-policy fail
  - `worker/fail_fast_e2e_test.go` — generic error retries, mixed
    success path, no-policy path
  - `worker/step_timeout_e2e_test.go` — hang-and-fail, timeout +
    retry combination, no-fire-on-success, no-fire-after-retry
    (staleness invariant)
  - Plus updates to `worker/worker_test.go`,
    `worker/lifecycle_event_test.go`, and
    `worker/consumer_subscribe_test.go` for tests that assumed the
    old NAK-loop semantics.

## Alternatives Considered

**(A) Keep worker NAK + also publish step.failed.** Both retry paths
fire — worker NAK redelivers the original message, engine timer
re-publishes a new one. Two attempts run for what should be one
retry. Rejected: violates single source of truth, doubles task
load, and `Attempts` accounting becomes ambiguous.

**(B) Use `MaxDeliver` on the JetStream consumer to bound retries
instead of engine policy.** This is what NATS provides natively. But
it can't honor per-step `retry.max_attempts` (consumer is per
task-type, not per step), can't apply per-step backoff curves
(consumer-level only), and forces the engine to learn JetStream
metadata to know "is this attempt 1 or 4?" Rejected as a design;
ADR-006 already documented "DLQ routing and retry budgets are the
engine's responsibility, not NATS's. We leave NATS unbounded so
engine policy isn't silently shadowed."

**(C) Step-timeout via worker-side context cancellation.** Worker
sets a `context.WithTimeout`, hands it to the handler, returns an
error if it exceeds. Rejected: workers can crash, hang in syscalls,
or be uncooperative — the engine watchdog remains authoritative
even when the worker is unreachable. The worker side is a separate
follow-up if we ever want hint-style cancellation.

---
title: Durable Agents
weight: 3
---

A durable agent is an [agent loop](/docs/step-types/agent-loops) whose handler opts into a per-step journal. Recovery then happens at the level of one model response or one tool call, not one iteration. A crash in the middle of a turn does not repeat the model calls and tool calls that already finished, and an unsafe tool is never started a second time.

## What survives what

| Failure | Without the journal | With the journal |
|---------|---------------------|------------------|
| Worker process crashes mid-turn | The iteration runs again from its last committed state. Every model call and tool call in it repeats. | The next delivery reuses the recorded model responses and tool results and continues from the first call that has no result. |
| Worker stalls past `AckWait`, then wakes up | The stalled handler and its replacement both run. | The replacement's claim fences the stalled handler. Its next write is refused and it stops. |
| Engine crashes or fails over | The engine rebuilds from history and the run snapshot. Waits and `loop_delay` timers are on `SLEEP_TIMERS`, so they survive. | The same. The journal does not depend on an engine. |
| A node or machine is lost | Survives only if the stream or bucket is replicated. | Survives only if `AGENT_JOURNAL` is replicated. In a [cluster](/docs/operations/clustering) it uses the cluster's replica count. On a single node it is a file on that node's disk. |
| A worker crashes on every delivery | Redelivered without limit until a step timeout fires, if one is set. | Fails with `restart_limit` after five claims in a row with no progress, then the step's retry policy resumes it from the journal. |

Three things are not covered. A tool already running on a fenced worker can still finish its side effect (see [Tools](#tools)). Streamed tokens sent with `PutStream` are a live preview, not a record; a crash mid-stream sends the request again. And the journal is only as safe as the NATS storage under it.

## The journal

A step opts in by calling `Journal()` on its task context. Nothing in the workflow definition changes, and a handler that never calls it runs as before.

`Journal()` does four things before it returns:

1. It reads the journal tail from the stream leader. If the tail belongs to a later iteration, this task is a late redelivery. It returns `journal.ErrStale` and the worker acknowledges the task without running anything.
2. It appends a `claim` record. A claim fences every earlier claim for the same or an earlier iteration.
3. If this attempt already committed the iteration, the previous holder crashed between committing and publishing its event. The worker republishes that event and returns `journal.ErrCommitted`. The turn is not repeated.
4. If the last `restart_limit` records are all claims, it fails the step and returns `journal.ErrRestartLimit`.

The handler returns any of these errors unchanged. They are not failures; the worker has already settled the task.

Every write to the journal is a JetStream publish with `Nats-Expected-Last-Subject-Sequence` set to the last sequence its writer saw. A writer that lost the tail cannot write. This is what makes fencing work: a stalled worker and its replacement receive the same task message, so they share every identifier, and the tail check is the only thing that tells them apart.

An iteration ends with one `commit` record that holds the next action and the state. After the commit is stored, the worker publishes the lifecycle event (`step.continue`, `step.completed` or `step.failed`), which carries the commit's sequence as `journal_seq`. For a journal step the commit is the state of record. The state in `Checkpoint()` and `LoadCheckpoint()` travels through the journal, and the `loop_state_max_bytes` cap does not apply.

The journal lives in the `AGENT_JOURNAL` stream, one subject per step (`journal.{run}.{step}`). It is kept 30 days and purged with the run. It holds model inputs, model outputs and tool results, so give it the same access control as run history, and do not write secrets into records.

## Tools

`j.Tool(ctx, call, replay, fn)` runs a tool call durably. It records the intent before running `fn` and the result after. The `replay` argument is a declaration about the tool:

- `journal.Safe`: running it twice is harmless. A read, or a write keyed on the idempotency key.
- `journal.Unsafe`: running it twice is not harmless. Send a message, open a pull request, charge a card.

On recovery within one attempt, the SDK walks the same calls in the same order:

- A recorded result is reused, whatever the declaration says.
- An intent with no result is run again only if both the recorded and the current declaration say `Safe`.
- Otherwise the SDK records an `interrupted` result, with any partial output the earlier holder wrote, and the model sees it.

`fn` returns the tool's output as a JSON value, recorded as is. Encode plain text, such as a command's output, with `json.Marshal`. Output that is not JSON is an error from `Tool`.

Calls are matched by position. If the handler issues a different call at a position than the one recorded, `Tool` returns `journal.ErrReplayMismatch` and writes nothing. A tool's own error is part of its result; it does not stop the step.

A retry is different from a redelivery. When the engine starts a new attempt of an iteration (after a failure or a timeout), it means "do it again": safe calls and model requests run fresh. Only unsafe intents from the earlier attempt are honoured, matched by name and input, so a retry cannot repeat a charge the first attempt already started.

Each call carries an idempotency key, `{run}.{step}.{iteration}.{call}`, in `call.Key`. It is the same on every delivery and attempt. Pass it to any external system that deduplicates.

### What "at most once" means

The SDK starts an unsafe call at most once per position. A successor never starts a call whose intent is already recorded. That is not the same as the effect happening at most once. A holder that stalled after writing the intent but before the tool's side effect can still complete that effect after its successor has recorded `interrupted`. If a tool must be exactly-once, pass `call.Key` to a system that deduplicates on it.

### Model calls

`j.Model(ctx, fn)` returns the response this attempt already recorded for the call at this position. Otherwise it reads the tail first, so a holder that is already fenced does not spend a model call, then calls `fn` and records the response. A request that fails records nothing. `journal.ScriptedModel` is a fake for tests; it counts requests, so a test can tell a reused response from a new request.

## A handler

This handler runs one turn of a tool-using agent. `turn` is your decoded model reply (`Done`, `Tool`, `Input`), and `callModel`, `runTool`, `appendTurn`, `appendInputs` and `answered` are your own code. `answered` reports whether the history ends in a final answer. The imports are `context`, `encoding/json`, `errors`, `time`, `worker`, `journal` and `inputs`.

```go
func agentTurn(tc worker.TaskContext) error {
    j, err := tc.Journal()
    if err != nil {
        return err // stale, fenced, committed or restart limit: already settled
    }
    history, err := tc.LoadCheckpoint()
    if err != nil {
        return err
    }
    if history == nil {
        history = tc.Input()
    }
    placed, err := tc.PlaceInputs(inputs.ModeSteer)
    if err != nil {
        return err
    }
    // A turn after a final answer was woken by a follow-up, and the
    // follow-up is what this turn answers.
    if answered(history) {
        followUps, err := tc.PlaceInputs(inputs.ModeFollowUp)
        if err != nil {
            return err
        }
        placed = append(placed, followUps...)
    }
    history = appendInputs(history, placed)

    raw, err := j.Model(tc.Context(), func(ctx context.Context) ([]byte, error) {
        return callModel(ctx, history)
    })
    if err != nil {
        return err
    }
    var next turn
    if err := json.Unmarshal(raw, &next); err != nil {
        return tc.FailPermanent(err)
    }
    if !next.Done {
        replay := journal.Unsafe
        if next.Tool == "search" {
            replay = journal.Safe
        }
        call := journal.ToolCall{Name: next.Tool, Input: next.Input}
        result, err := j.Tool(tc.Context(), call, replay,
            func(ctx context.Context, call journal.ToolCall) ([]byte, error) {
                return runTool(ctx, call.Name, call.Input, call.Key)
            })
        if err != nil {
            return err
        }
        if err := tc.Checkpoint(appendTurn(history, raw, result.Output)); err != nil {
            return err
        }
        return tc.Continue(nil)
    }

    if err := tc.Checkpoint(appendTurn(history, raw, nil)); err != nil {
        return err
    }
    err = tc.Continue(nil, worker.UntilInput(), worker.Delay(time.Hour))
    if errors.Is(err, worker.ErrInputsQueued) {
        return tc.Continue(nil) // the next turn places what was queued
    }
    return err
}
```

One tool call per turn keeps the example short. Several calls in one turn work the same way: each `j.Tool` takes the next position.

## Inputs and steering

An outside caller can send a message to a running agent step. The step does not need to be waiting.

```bash
curl -X POST http://localhost:8080/runs/$RUN/steps/agent/inputs \
  -d '{"request_id": "msg-42", "mode": "steer", "content": {"text": "use the staging database"}}'
```

The response is `201` for a new submission and `200` for a repeat of the same `request_id`. A repeat returns the original, whatever its content, so a client can retry safely for seven days. Read the status with `GET /runs/$RUN/steps/agent/inputs/msg-42?wait=30s`, and withdraw a queued input with `DELETE` on the same path. The full request and response are in the [REST API reference](/docs/reference/rest-api#step-inputs). The same operations exist as the NATS endpoints `api.inputs.submit`, `api.inputs.get` and `api.inputs.withdraw`.

There are two modes:

- `steer` joins the current run after the next tool round.
- `follow_up` starts the next run after the agent gives a final answer.

The handler decides where inputs enter the conversation by calling `PlaceInputs(mode)` at those boundaries. `PlaceInputs` needs the journal. It claims each input with a compare-and-swap, then records it as an `input.placed` record carrying the content. The record is the truth about placement. A redelivery or the next iteration reads the journal and never places the same `request_id` twice. A redelivery of the same iteration starts again from the last checkpoint, so `PlaceInputs` returns the inputs an earlier delivery of the iteration placed, once, before any new ones. A withdrawal that arrives first wins, so a `204` is never followed by a placement.

A submission ends as `done` (the iteration that placed it committed) or `unanswered` (the step finished before it was placed). No accepted input is left queued on a finished step.

Submitting input writes into a running agent's context, and the step's tools run with the step's permissions. The input endpoints sit on the same router as run signals, so they need the same authorization, and an input from an untrusted party is a prompt-injection vector.

## Durable waits

`Continue` takes options that ask the engine to wait before sending the next iteration:

| Option | Wakes |
|--------|-------|
| `worker.Delay(d)` | After `d` |
| `worker.Until(t)` | At `t` |
| `worker.UntilInput()` | When an input is submitted for the step |

Options combine. A time and `UntilInput()` wake on whichever comes first. The wait is saved on the step and rides the commit, and a time wait is a timer on `SLEEP_TIMERS`. No worker holds a claim while the step waits, so an agent that waits a day for a person holds no worker for that day, and an engine restart does not lose the wait.

`Continue(nil, worker.UntilInput())` returns `worker.ErrInputsQueued` while inputs are queued. Place them and run another turn instead. Together with a check the engine makes when it enters the wait, this closes the gap where an input arrives while the turn that would wait is still running.

A step's `timeout` and a loop's `max_duration` keep counting while the step waits. A step that waits for a person should carry neither, or a bound that allows for the wait. `loop_delay` does not apply to an iteration that waits.

## Cancellation

`tc.Context()` is cancelled when the run is cancelled. Pass it to model calls, tool calls and the journal helpers, and return when it ends. A handler that ignores it behaves as before: it runs to the end and the engine drops its events.

The engine announces a cancel on core NATS, which is at-most-once. The worker therefore also checks run state on each `Heartbeat()`, which returns `worker.ErrRunCancelled` once the run is cancelled, so a long handler should heartbeat. Cancelling a parent still cascades to its child runs. See [Cancellation](/docs/reliability/cancellation).

## Restart limit

A journal step that is claimed `restart_limit` times in a row with nothing between the claims fails with reason `restart_limit`. The count is per iteration and includes claims from earlier attempts that ended without a commit. The default is 5. Set it per worker with `worker.WithRestartLimit(n)`, from 1 to 1000. A `Pause` counts as progress.

The failure is committed to the journal and published as a retriable `step.failed`, so the step's retry policy applies and the retry resumes from the journal. The count can overstate real crashes by one when two workers claim the same delivery at once. Treat the limit as a backstop, not an exact budget. Steps without a journal keep the step timeout as their backstop.

## Reading the journal from a client

`GET /runs/{id}/steps/{step}/journal?from={seq}&follow=1` serves the records to clients as a page or as a server-sent event stream. The cursor is the record's sequence, and a reconnect resumes with `Last-Event-ID`. There is no separate per-iteration event: the `commit` record with action `continue` marks the end of an iteration. See the [REST API reference](/docs/reference/rest-api#step-journal).

`dagnats journal check <run-id>` audits a finished or running journal. It checks one commit per iteration, no record after the claim that fenced its writer, every tool intent answered by one result, every input placed once, and the engine's iteration count against the commits. It exits 1 on a violation.

## HTTP bridge workers

A worker that polls over the [HTTP bridge](/docs/workers/http-bridge) gets the same fencing. The first `journal.append` or `journal.read` claims the task's journal. Later actions answer `409` with code `fenced` when a successor owns the task, `410` (`stale` or `committed`) when the task is finished, and `503 not_provisioned` when the stream is missing. `complete`, `fail`, `continue` and `pause` commit before they publish once the task has a claim. `load_checkpoint` returns the task's state and says where it came from. A cancelled run is a `409` on resolve and on heartbeat.

The bridge exposes the journal records and the claim. The replay rules for `Tool` and `Model`, `PlaceInputs`, and the `Delay`, `Until` and `UntilInput` waits are in the Go SDK. A bridge client that wants them writes the matching records itself, following the [wire protocol](/docs/reference/wire-protocol).

## Upgrading

Upgrade in this order:

1. **Engines first.** An older engine ignores the `wait` on a continue, so a worker that asks for `UntilInput()` is sent its next iteration at once and hot-loops until `max_iterations`. An older engine also stamps no attempt on the first dispatch, and without it, iteration 0 is not durable: a redelivery looks like a new attempt and repeats the turn.
2. **Then workers that call `Journal()`.**
3. **Then turn off `legacy_step_events`.** By default (`DAGNATS_LEGACY_STEP_EVENTS`, default true) the engine still accepts `step.continue`, `step.completed` and `step.failed` events that carry no dispatch identity, from workers and bridge clients built before the fence. While it is on, a duplicate continue from such a worker can still advance a loop twice, and the engine logs a warning at startup. Set it to false once every worker is upgraded.

## Limits

- **One goroutine per journal handle.** Replay matches calls by position, so calls from several goroutines would be recorded in one order and replayed in another. The handle panics on concurrent use. If the model asks for parallel tool calls, run them through the journal one at a time. Concurrent calls keyed by the model's call IDs are not built.
- **Opt in for the whole fleet.** Opt-in is per task type. A journal step's `step.continue` carries no state, so an older build of the handler that picks up the next iteration would read stale state. Roll `Journal()` out to every worker of that task type together. The restart limit has the same condition: an older worker claims and crashes without checking the count.
- **Iteration 0 needs the engine upgraded first.** See [Upgrading](#upgrading).
- **Bounded calls per iteration.** An iteration has a fixed upper bound on tool and model calls. A step that outgrows it has outgrown a per-step journal.
- **Delivery is not exactly once for effects.** See [What "at most once" means](#what-at-most-once-means).

## How it is verified

The SDK, the journal and the engine call a crash seam at each boundary where a crash leaves different durable state: before and after the claim, after the tool intent, inside the tool, after the result, after the commit, after the lifecycle event, and in the engine between saving a dispatch and publishing it. Production installs no hook, and the seam is internal to the module.

Two tests run on top of it, with a scripted agent (a safe tool, an unsafe tool, a safe tool, an answer) whose tools count their starts and effects outside the journal:

- **The crash-point matrix** runs in the normal test suite. For each crash point it kills the holder, and separately stalls it past `AckWait` so a zombie tries to write, on iteration 0, on the unsafe iteration and on the iteration after it, with two engines sharing the history consumer as a cluster does. Each run must finish with the same answer as a crash-free run, `journal check` must pass, and the unsafe tool must have started at most once.
- **The chaos soak** is opt-in (`make test-chaos`, and a nightly job). It runs many runs in parallel under a seeded random schedule of kills, stalls, duplicate deliveries and, in cluster mode, stream-leader step-downs, then checks the answer, the journal and the counters after each run. The seed is printed so a failing schedule can be reproduced.

Each guard was deleted once to confirm a test catches it: the expected-sequence header, the claim identity, the engine's acceptance rule, the restart limit, and the replay rules for safe and unsafe tools. Running the harness found four real bugs, including a leak of journal reader consumers that would have failed claims after a few hundred runs. All four are fixed.

## Related

- [Agent Loops](/docs/step-types/agent-loops) -- the step type and its bounds
- [Agent Loop Pattern](/docs/ai-patterns/agent-loop-pattern) -- the reasoning cycle
- [Human in the Loop](/docs/ai-patterns/human-in-the-loop) -- inputs, signals and approval gates
- [Checkpoints](/docs/coordination/checkpoints) -- state across iterations
- [Wire Protocol](/docs/reference/wire-protocol) -- the journal records and headers
- [Journal SDK](/docs/reference/sdk/journal) -- API reference

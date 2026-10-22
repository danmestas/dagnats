# ADR-026: Agent journal

**Status:** Accepted (2026-10-21)
**Deciders:** Dan Mestas
**Depends on:** ADR-025
**Reverses:** ADR-002's rejection of per-tool-call durability
**Issues:** #804, #805, #807

## Context

ADR-025 makes one iteration of an agent loop one fenced write. That fixes
the loop's bugs, but an iteration is still the unit of recovery: a crash
mid-iteration repeats every model call and tool call in it. Tools with side
effects (send a message, open a pull request, charge a card) then run twice.

pi-durable, the durable harness behind the Pi coding agent, recovers at the
level of a single model response or tool call. It commits a tool's intent
before running it and, after a crash, reruns the tool only if it is declared
safe to replay; otherwise the model is told the tool was interrupted. dagnats
should give its agents the same guarantee.

ADR-002 rejected per-tool-call durability on an estimated 50 ms per call.
Measured against nats-server 2.15.1 on a laptop over loopback, a fenced
1 KiB append costs 0.15 ms at p50 on one node and 0.29 ms on three (p99
about 5 ms). The estimate was off by two orders of magnitude.

ADR-025 cannot tell a stalled worker from its replacement: JetStream
redelivers the same message, so both hold the same nonce and attempt.

## Decision

Each agent step gets an append-only journal, and every write to it is
checked against the journal tail its writer last saw.

### Stream

`AGENT_JOURNAL`, subjects `journal.{run}.{step}`, file storage, the
cluster's replica count, a 30-day max age like `WORKFLOW_HISTORY`, a
two-minute dedup window, purged with the run (`dagnats clean`). Direct get
is off: a direct get may be answered by a replica that lags, and with it off
`GetLastMsgForSubject` goes through the JetStream API, which only the stream
leader serves. That is how "read the tail from the leader" is implemented.
Provisioned by `natsutil.SetupAll` with the other streams.

### Appends

Every append carries `Nats-Expected-Last-Subject-Sequence` set to the last
journal sequence its writer saw, and `Nats-Msg-Id` set to
`{run}.{step}.{attempt}.{iteration}.{stream_seq}.{delivered}.{index}`,
where `stream_seq` is the task message's sequence in the task stream,
`delivered` its delivery count and `index` the record's position within
its claim (the claim is 0). The delivery count is part of
the claim identity because the engine stamps the attempt on every
dispatch (`dispatchIdentity`), so a stalled worker and the AckWait
redelivery that replaces it hold the same run, step, iteration, attempt
and nonce; without it their records would share msg-ids, the successor's
claim would be answered with the stalled claim's sequence inside the
dedup window, and nothing would ever fence. The stream sequence closes the
same gap for a dispatch resend: a fresh task message, delivery count 1
again, with the nonce, attempt and iteration of an original still in
flight. Redeliveries share their message's stream sequence, a resend does
not, so the resend's claim fences the original's. JetStream rejects an append
whose expected sequence is stale (error 10071, naming the real tail), and,
on a clustered stream, one that arrives while another expected-sequence
publish on the subject is still in process (10164, found while running the
conformance suite on three nodes). A worker that lost the tail cannot write.

A rejection does not by itself mean the claim is lost. Three benign cases
also cause one: an append that landed but whose acknowledgement was lost; a
rival publish still applying on the same subject; and a late claim for an
earlier iteration landing just before the writer's own claim. On rejection
the writer re-reads the tail from the stream leader. If the tail is the
record it just sent (same `Nats-Msg-Id`), the append succeeded. If it is
another record of its own claim, or any record from an earlier iteration,
it retries with the actual sequence; if the tail has not moved it waits
briefly for the in-process publish and retries. At most 8 resyncs. Only a
record from another claim for the same or a later iteration means the
writer is fenced.

### Claims

A step opts in when its handler calls `TaskContext.Journal()`; nothing in
the workflow definition or the task payload changes, and a handler that
never calls it runs exactly as before. Opt-in is per task type and must be
fleet-wide: a journal step's `step.continue` carries no state, so an older
handler build picking up iteration N+1 would read a stale
`TaskPayload.state`. Roll a handler's journal use out to every worker of
that task type together. The worker then reads the tail from
the stream leader. If the tail belongs to a later iteration than the task's,
the task is a late redelivery: the worker stops without writing and
acknowledges the message. Otherwise it reads the journal back, and appends
a `claim` record carrying the iteration, attempt, dispatch nonce and
delivery count. A claim fences every earlier claim for the same or an
earlier iteration, in journal order, and never a worker on a later one. A
fenced worker publishes no further lifecycle event and does not touch the
task message: the message belongs to the successor, whose acknowledgement
settles it. Racing claimers for one iteration may each land a claim; what
holds is that afterwards exactly one of them can still write.

### Records

| Type | Written by | Purpose |
|---|---|---|
| `claim` | SDK | Takes ownership; counted by the crash-loop guard (ADR-028) |
| `pause` | SDK | Written by `Pause` before its delayed redelivery |
| `model.response` | SDK | A completed model response, reused on recovery |
| `tool.intent` | SDK | A tool call about to run, with its replay declaration |
| `tool.result` | SDK | Its result, real or `interrupted` |
| `input.placed` | SDK | Inputs moved from the inbox into the conversation (ADR-027) |
| `entry` | Handler | Application records, opaque to dagnats |
| `commit` | SDK | Ends an iteration: next action and state |

Each record is a JSON body. Its identity is in headers so a tail read can
classify it without decoding the body: `Dagnats-Journal-Type`,
`Dagnats-Journal-Iteration`, `Dagnats-Journal-Attempt`,
`Dagnats-Journal-Index` and `Dagnats-Dispatch-Nonce`. The model, tool and
input types are written by `Tool` and `Model` (below) and by the input
helpers of ADR-027; `claim`, `pause`, `commit` and the handler's `entry`
by #804.

### Commits and recovery

An iteration ends with one `commit` message holding the action its
lifecycle event will carry (`continue`, `complete` or `fail`; a pause is
its own record, not a commit) and the state. It is one message, not an
atomic batch: on a one-node file stream the server stages a batch in a store
it syncs to disk, which costs 25 to 30 ms, against 0.15 ms for one fenced
message. After the commit is stored, the worker publishes the lifecycle
event of ADR-025 with the commit's journal sequence as `journal_seq`.

For a journal step the commit record replaces the state ADR-025 carries in
`step.continue`: the event carries no state, the engine keeps whatever it
had, and the next delivery reads the last committed state from the journal.
One source of truth, and the `loop_state_max_bytes` cap no longer applies
to journal steps, which is where ADR-025 said larger state belongs. The
engine is unchanged.

A worker that claims iteration N reads the journal back from the last
commit:

- If a commit exists for this attempt and iteration, from any delivery,
  the previous holder crashed between committing and publishing. The
  worker republishes that commit's lifecycle event (its message ID is
  stable) and acknowledges the task. The turn is not repeated. Recovery
  matches on attempt and iteration, not on delivery: the engine stamps
  the attempt on every dispatch, iteration 0 included (finding 1 below),
  so a redelivery of any iteration finds the previous delivery's commit
  as its own.
- Otherwise it rebuilds from the last committed state plus the records of
  iteration N so far. Recorded model responses and tool results are reused,
  not redone.

### Tools and model calls

Both helpers live on the journal handle, not on `TaskContext`: adding
methods to that interface breaks every out-of-repo implementation of it
(see the `LogOut` note on the interface), and the handle already owns the
records a recovery reuses.

`j.Tool(ctx, call, Safe|Unsafe, fn)` appends `tool.intent` carrying the
call (name, input), its position in the iteration, its idempotency key
and the replay declaration; runs `fn`; and appends `tool.result` with the
output or the tool's own error. Calls are matched by position within the
iteration, and the recorded call at a position must be the same call
(name and input as canonical JSON), else `ErrReplayMismatch`: a handler
that issues its calls in another order on replay is not replaying, and
nothing is reused or written for it.

Recovery is per attempt. Within an attempt (the deliveries and claims
of one dispatch, before any commit), a successor walks the same calls:

- a recorded result is reused, whatever either declaration says;
- an intent without a result is rerun only if both the recorded and the
  current declaration say `Safe`;
- otherwise an `interrupted` result is recorded with any partial output
  (the `entry` records the previous holder appended after the intent,
  as a JSON array) and the call's key. The model sees it.

A new attempt of the iteration (after a fail commit, or a higher
stamped attempt, on any iteration) is the engine saying
"do it again": safe calls and model requests run afresh, with a new
intent, and only unsafe intents from the prior attempt are honoured,
matched by content rather than position because the new attempt's model
may plan differently: the first prior unsafe intent with the same name
and input not yet honoured in this attempt answers the call under its
own key, with its recorded result (the effect happened) or as
`interrupted`, and takes no position in the new attempt. Reusing safe
results across attempts would turn every retry into a replay of the
failure.

A retry at iteration 0 without a commit (the engine timed the attempt
out, or the restart limit failed it) is told from a redelivery by the
stamped attempt, as on every other iteration: the retry carries a
higher attempt and repeats the turn from scratch. Before finding 1 the
attempt followed the delivery count there, so such a retry reused the
failed attempt's safe results and model responses as a redelivery
would.

The guarantee is that the SDK starts an unsafe call at most once per
position: a successor never starts a call whose intent is recorded. It
is not that the effect happens at most once: a holder stalled between
its intent and the tool's effect may complete that effect after the
successor recorded `interrupted`. The result carries the call's key so
the handler, or the external system, can deduplicate on it.

A journal handle belongs to one goroutine. Replay matches calls by
position, so calls issued from several goroutines would take positions
in one order and replay in another; a mutex alone would not fix that. A
handler that runs a model's parallel tool calls issues them through the
journal one at a time for now, and the handle panics on concurrent use
(a re-entrant owner check, so a tool function may call back into the
journal from the owning goroutine for partial-output entries).
Concurrent tool calls, keyed by the model's call ids rather than by
position, are future work.

The successor writes no second intent for a call it answers, so each
call has one intent answered by one result, in order (checker invariant
4, keyed by iteration and call rather than by claim; an intent left
unanswered is tolerated once its attempt ended, by a fail commit or by
a higher attempt's intent for the call, and the last one is tolerated
when the step ended failed). A tool's own error is part of its result,
not a reason to stop the step; the error `Tool` returns is the journal's
(`ErrFenced`, `ErrCommitted`, `ErrReplayMismatch`, transport).

Each call has a stable idempotency key, `{run}.{step}.{iteration}.{call}`,
the same on every delivery and attempt, for tools to pass to external
systems that deduplicate (ADR-028's spawn key defaults to it).

`j.Model(ctx, fn)` returns the `model.response` this attempt recorded for
the call at this position, and otherwise reads the tail first (a holder
already fenced must not spend a model call), requests, and records the
response. A request that fails records nothing and does not consume the
position, so the next request, or the next delivery, records at the
same position. `journal.ScriptedModel` is the fake
for tests: scripted responses in order, and a count of requests served,
so a test can tell a reused response from a re-requested one. Streaming
tokens stay on `PutStream`, which is a non-durable preview: a crash
mid-stream re-sends the request on the next delivery.

### Crash-loop guard

`Open` counts the claims on the task's iteration at the tail with
nothing between them (`Restarts`); the worker fails the step when the
count reaches its restart limit. ADR-028 has the decision.

### Compatibility

`Checkpoint` and `LoadCheckpoint` keep working on journal steps:
`Checkpoint` buffers the state the next commit carries, and
`LoadCheckpoint` returns, in order, the state checkpointed this delivery,
the pause marker `{"pause_resume": name}` when the iteration's newest
record is a `pause`, else the last committed state. `Pause` writes the
`pause` record instead of the `checkpoints` marker. A step without a
journal is untouched. The legacy `checkpoints` bucket expires its entries
after 30 days, upgraded in place on existing deployments.

The HTTP bridge exposes the journal through the resolve actions
`journal.append`, `journal.read` and `load_checkpoint`. The first
`journal.append` or `journal.read` claims through the same `journal`
package, with the polled message's delivery count in the claim, so a
redelivered bridge task fences the stalled bridge worker exactly as it
fences a Go worker. `complete`, `fail`, `continue` and `pause` commit
before they publish once the task has a claim. The outcomes map to
statuses: `409 fenced` (the successor owns the task; nothing is
acknowledged), `410 stale` and `410 committed` (the task is acknowledged;
a committed attempt's event is republished), `503 not_provisioned`.

### Checker

`journal.Check` and the `dagnats journal check <run>` command audit a run.
They read the journal, the run's history (for the attempt the engine
accepted and its iteration count) and the `submissions` bucket (for input
outcomes), and assert:

1. Each iteration has exactly one `commit`, from the attempt the engine
   accepted.
2. No record from a fenced claim appears after the claim that fenced it.
3. An unsafe tool's intent has at most one execution, counted by a
   side-effect counter the test tools write outside the journal.
4. Every `tool.intent` has exactly one `tool.result`, real or `interrupted`.
5. Every placed input appears once, and every accepted `request_id` ends
   done or unanswered.
6. The engine's iteration count equals the number of commits.

Invariant 3 needs the external counter, so it holds in tests only; the
command reports it as skipped and checks the other five on any run, and
reports invariant 5 as skipped while the `submissions` bucket does not
exist. The accepted commit of an iteration is the one the latest
lifecycle event for that iteration in history names by `journal_seq`:
history records what workers published, not what the engine applied, and
a retry's `step.continue` after an attempt's `step.failed` names a second
commit for the same iteration, which supersedes the first. Only for
events without `journal_seq` does the checker fall back to the highest
`step.started` attempt among the dispatch nonces that claimed the
iteration. The command exits 1 on a
violation so it can gate a soak.

A gap inherited from ADR-025, not closed here: the worker publishes
`step.started` before the handler can call `Journal()`, so a late stale
redelivery raises the step's `Attempts` through `handleStepStarted`'s max
rule before `ErrStale` acknowledges it.

### How durability is verified

`internal/chaos` is the crash seam (#809). The SDK, the journal and the
engine call `chaos.At` at every boundary where a crash leaves distinct
durable state: before and after the claim, after `tool.intent`, inside
the tool (effect done, result not written), after `tool.result`, after
the commit, after the lifecycle event (before the ack), and in the
engine between a dispatch save and its publish, and between the
publish and the sent marker, on both the enqueue and the continue path.
Production never installs a hook, so a boundary is one atomic load and
a nil check; the package is internal, so nothing outside the module can
install one. A kill is the holder returning `chaos.ErrKilled` up its
stack and then acknowledging, negatively acknowledging and publishing
nothing, which is what a dead process leaves behind; a stall is the hook
blocking past AckWait and then letting the zombie proceed, so it must
be fenced.

`e2e/durability` runs on top of it, with a scripted agent (a safe tool, an
unsafe tool, a safe tool, an answer) whose tools count starts and
effects in a KV bucket outside the journal:

- `TestCrashPointMatrix`, in the normal suite (and the race job), on
  two engines sharing the history durable as a cluster does: one run
  per crash point, killed and stalled, on iteration 0, on the unsafe
  iteration and on the safe iteration after it; the run must finish
  with the crash-free answer, exactly, except that the unsafe call is
  `interrupted` when the crash hit `after_tool_intent` or `inside_tool`
  on its iteration (a safe call there must be rerun), `Check` must
  pass, and the unsafe tool must have started at most once.
- `TestCrashLoop_RestartLimitFailsThenRetries`: a kill after every claim
  until the restart limit fails the step, which the retry resumes.
- `TestChaosSoak`, opt-in (`DAGNATS_CHAOS=1`, `make test-chaos`, the
  `Chaos soak` workflow on the `chaos` PR label and nightly): two
  orchestrators sharing the history durable as a cluster does, runs in
  parallel under a seeded random schedule of kills, stalls drawn
  around AckWait, the duplicate deliveries those cause (AckWait
  redelivery, a `DispatchUnsent` resend) and, in cluster mode,
  stream-leader step-downs; the crash-free answer (the unsafe call
  interrupted exactly when the run's journal shows a takeover found
  its intent unanswered: a stall is drawn around AckWait, so the
  schedule alone does not say whether one happened), `Check` and the
  counters after every run; the seed is printed and
  `DAGNATS_CHAOS_SEED` reproduces the schedules. Making it green after
  findings 1 to 4 took two harness corrections: that expectation (the
  harness had required `interrupted` for every scheduled stall, which
  a stall shorter than AckWait cannot produce), and `journal.Read`
  retrying the server's "JetStream system temporarily unavailable"
  (10008), which a leader step-down answers with, as it already
  retried no-responders and timeouts.

Mutation checks, each guard deleted once against the matrix and the
crash-loop test:

| Guard deleted | Caught by |
|---|---|
| `Nats-Expected-Last-Subject-Sequence` on appends | every stall cell: the zombie's append lands (6 cells) |
| `stream_seq` and `delivered` in the claim msg-id | the redelivery's claim is deduplicated against the stalled one: kill cells wedge, stall cells double-write, the crash loop never trips (7 cells) |
| the engine acceptance rule | `engine.continue_saved/kill`: the redelivered continue advances the loop twice |
| the restart limit | the crash loop never fails the step |
| tool replay safety (unsafe intents rerun) | `after_tool_intent/stall`, `inside_tool/kill`, `inside_tool/stall`: the unsafe tool starts twice |
| safe intents never rerun (answered interrupted) | the four `after_tool_intent`/`inside_tool` cells on the safe iteration: the answer carries `interrupted` for a safe call |

What the harness found, and the fixes that followed #809 in their own
PRs. The iteration-0 cells and the two engines are in the default
matrix since findings 1 to 3 were fixed (they were the red tests of
that fix).

| # | Finding | Status |
|---|---|---|
| 1 | On iteration 0 the attempt followed the delivery count, so every redelivery was a new attempt by construction: `Open` could not recognise the previous delivery's commit as its own and reran the iteration, a late redelivery after the engine had moved on reran it again until the next iteration's claim fenced it (an intent without a result), and, worse, each delivery's `step.started` raised `Attempts` by the max rule while the journal picked its winner by journal order, so the winner's `step.continue` could carry a lower attempt than the engine held, be dropped by the acceptance rule, and the winner's ack then ended redelivery: the run wedged (`reason=attempt` with `event_attempt=2 step_attempt=3`). | Fixed: `dispatchIdentity` stamps `Attempts + 1` on every new dispatch, so the first dispatch is attempt 1 and a redelivery is a recovery of the same attempt; `priorAttempt` lost its iteration-0 clause. `TestDispatchIdentity_NewAttempt`, `TestFirstDispatch_CarriesAttemptOne`, `TestReplay_Iteration0RetryRerunsSafeCalls`, the iteration-0 matrix cells. |
| 2 | Two engines: a `step.continue` handled by one engine before the other has applied the same delivery's `step.started` was dropped for a Running step by the attempt comparison. | Same root as 1, seen through the two-engine window: the dropped continue was a redelivery's, numbered higher than the first delivery's `step.started` the step still held; with the attempt stamped, every delivery of a dispatch carries the step's attempt and the Running rule accepts it. The two-engine matrix at iterations 1 and 2 was green on the unfixed code (3 runs of 32 cells); every drop in the failing runs was `reason=attempt` on iteration 0. `TestStepContinue_RedeliveryContinueBeforeItsStarted_Advances` replays the order. The acceptance rule of ADR-025 is unchanged. |
| 3 | An engine that died between a continue's save and its publish while the step was still Queued (the previous iteration's `step.started` overtaken, as in 2) had its resend planned as a first dispatch, whose msg-id `{run}.{step}.queued` `TASK_QUEUES` deduplicated: the iteration was never sent (`engine.continue_saved/kill` with two engines: `Queued, Iterations:1, DispatchUnsent`, resend `tasks=1`, nothing delivered). | Fixed: `planTaskResend` treats a loop past iteration 0 as a continue resend whatever the status. With it, `journal.Open` fences an older delivery of the same task message (same `stream_seq`, lower delivery count) that opens after a newer one, so a delivery held in a stalled consumer's prefetch buffer cannot take the iteration back. `TestStepContinue_CrashBetweenSaveAndPublishWhileQueued_Resends`, `TestOpen_NewerDeliveryOfSameMessageFencesOlder`, `TestOpen_ResendAndUnknownSequenceClaimInJournalOrder`. |
| 4 | Journal reader consumers leak: `journal.Open` and `journal.Read` each leave an ordered consumer on `AGENT_JOURNAL` that is never deleted (measured: 110 after 20 runs and 50 reads, unchanged after 16s), so a soak hits nats-server's default of 1000 consumers per stream after about 300 runs and every claim fails with error 10026. | Fixed (#827): `natsutil.OpenConsumer` and `OpenStreamConsumer` return an `*OrderedConsumer` whose `Close` every caller defers, and journal reads close theirs on each retry. `TestConsumerLeak_OpenAndReadReleaseTheirConsumer`, `TestConsumerLeak_FailedReadReleasesItsConsumer`, `TestConsumerLeak_HistoryReadReleasesItsConsumer`. |

Consequences of stamping the first attempt, weighed when fixing 1:

- A crash redelivery (AckWait expiry, a NAK after a failed
  `step.started` publish, a `Pause` resume) no longer spends retry
  budget: `Attempts` counts the engine's own dispatches, which is what
  `max_attempts` was written against. A crash loop is bounded in
  deliveries by the restart limit for journal steps (ADR-028) and in
  time by the step timeout otherwise, both of which fail the attempt
  and retry it as a new one. A step with neither was unbounded before
  as well (the task consumer has no `MaxDeliver`); only its `Attempts`
  grew.
- The step-timeout watchdog already re-armed a redelivery for the
  attempt's remaining time from iteration 1 (#743); it now does so on
  iteration 0 too.
- Workers that predate the stamp already preferred `TaskPayload.attempt`
  when set, so they follow it; the `NumDelivered` fallback stays in the
  worker SDK and the bridge for engines that predate it, with the old
  behaviour. `RetryCount()` keeps its documented 0 on the first dispatch
  and the bridge's `attempt` was already 1 there.
- Map instances, retries, DLQ replays and on-fail compensation go
  through the same builder and are unchanged past the first dispatch.
- Iteration-0 durability needs the engine upgraded before the workers.
  A task from an engine that predates the stamp carries attempt 0, so
  a new worker falls back to the delivery count and a redelivery on
  iteration 0 regresses to finding 1 until that engine is upgraded.
  From iteration 1 nothing changes, since every engine stamped those.

## Requirements

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in
all capitals.

| ID | Requirement | Proven by |
|---|---|---|
| R1 | Every journal append MUST carry `Nats-Expected-Last-Subject-Sequence` and a stable `Nats-Msg-Id`. | `TestConformance_AppendCarriesFenceAndStableMsgID` |
| R2 | A worker MUST stop handling a task once a record from another claim follows its own. | `TestConformance_RecordFromAnotherClaimFences`, `TestJournal_StalledWorkerIsFencedBySuccessor`; `SIGSTOP` takeover in `e2e/cluster` (`TestMultiProcessCluster_JournalTakeoverFencesStalledWorker`); bridge redelivery in `TestBridgeJournalRedeliveryFencesFirstWorker` |
| R3 | A worker MUST NOT treat a rejection as fencing before re-reading the tail from the leader. | `TestConformance_FalseRejectionLostAck`, `TestConformance_FalseRejectionOwnRecordAtTailViaOtherNode`, `TestConformance_RacingClaimsLeaveExactlyOneWriter` (in-process rival, three nodes) |
| R4 | A claim MUST NOT fence a worker on a later iteration. | `TestConformance_EarlierIterationClaimNeverFencesLater`, `TestConformance_StaleIterationStopsWithoutWriting` |
| R5 | An iteration MUST end with exactly one commit for its attempt. | `TestConformance_OwnAttemptCommitIsHonoured`, `TestCheckStep_OneCommitPerIterationFromAcceptedAttempt`, `TestCrashPointMatrix` and `TestChaosSoak` (e2e/durability) |
| R6 | Recovery within an attempt MUST reuse recorded model responses and tool results; a new attempt MUST rerun safe calls and model requests and MUST honour prior unsafe intents. | `TestReplay_Matrix` (result present), `TestReplay_ModelResponseReusedNotReRequested`, `TestReplay_NewAttemptRerunsSafeCallsAndModelButKeepsUnsafeOutcomes`, `TestReplay_FailedModelRequestKeepsItsPosition` |
| R7 | An unfinished tool call MUST NOT be rerun unless both declarations are `Safe`. | `TestReplay_Matrix` (result absent) |
| R8 | The SDK MUST NOT start an unsafe call more than once per position across any crash schedule, and MUST report it interrupted with its key. | `TestReplay_UnsafeSideEffectAtMostOnceAcrossCrashPoints` (four crash points, each with a redelivery of the same task and the stalled worker resuming), `TestToolReplay_StalledUnsafeToolIsInterruptedNotRerun` (real Workers), `TestCrashPointMatrix` and `TestChaosSoak` (e2e/durability) |
| R9 | A recorded call at a position MUST match the current call, else `ErrReplayMismatch`. | `TestReplay_MismatchedCallOnRecoveryIsAnError` |
| R10 | The engine MUST stamp the attempt on every dispatch, the first included, so a redelivery of a task message carries the same attempt as its first delivery. | `TestDispatchIdentity_NewAttempt`, `TestFirstDispatch_CarriesAttemptOne`, `TestStepContinue_RedeliveryContinueBeforeItsStarted_Advances`, the iteration-0 cells of `TestCrashPointMatrix` |
| R11 | A resend of an unsent continue MUST be sent as a continue (its own msg-id) whatever the step's status. | `TestStepContinue_CrashBetweenSaveAndPublishWhileQueued_Resends`, `engine.continue_saved/kill` in `TestCrashPointMatrix` with two engines |
| R12 | An older delivery of a task message MUST NOT claim over a newer delivery of the same message. | `TestOpen_NewerDeliveryOfSameMessageFencesOlder`, `TestOpen_ResendAndUnknownSequenceClaimInJournalOrder` |

## Alternatives considered

- **Leases (ADR-024) for ownership.** A lease needs a clock and renewal, and
  the engine would check a token per event. The journal's sequence check
  fences on the same write that records progress.
- **Atomic batches for every commit.** Measured at 25 to 30 ms on a one-node
  stream. Kept only for state too large for one message.
- **Opting in through `AgentLoopConfig` or the task payload.** Would touch
  the schema, the engine's dispatch path and the bridge for a bit the
  handler already knows. A `Journal()` call is the smallest surface, and a
  step with no journal costs nothing.
- **Carrying the committed state in `step.continue` as well.** Two copies
  to keep equal and the 256 KiB cap kept on journal steps, for an engine
  that would not read the second copy.
- **pi-durable's storage model** (a session store with indexed tables). Its
  reads need indexes that JetStream does not keep; an append-only journal
  read back from the last commit needs none.

## Consequences

- Recovery granularity becomes one model response or tool call.
- An unsafe tool runs at most once, and the model is told when it was
  interrupted. A tool already mid-flight on a worker the successor
  fenced completes its side effect.
- The engine and worker can tell a stalled worker from its replacement.
- A new stream to operate and retain, and a protocol the bridge must expose.
- Claims are ordered by journal order across messages, and by delivery
  count within one message. A consumer stalled past AckWait can hold an
  older delivery in its prefetch buffer; processed after the consumer
  resumes, that delivery finds the newer delivery's claim (same
  `stream_seq`, higher count) and is fenced without writing. Ordering by
  delivery count across messages would be unsafe: the dispatch resend
  republishes the same dispatch as a fresh message whose count restarts
  at 1, so a resend claims in journal order.
- The legacy `checkpoints` bucket can be retired.
- `PutStream` output is documented as a non-durable preview.
- A retry (new attempt) repeats the iteration's safe calls and model
  requests and keeps only its unsafe outcomes; a redelivery within an
  attempt repeats nothing.
- At-most-once for unsafe tools is at-most-one-start per position; the
  effect of a call interrupted between intent and effect may still land.
  Tools that must be exactly-once pass the key to a system that
  deduplicates.

## Security considerations

The journal holds model inputs, model outputs and tool results, which can
include sensitive data. It has the same access and retention as run history:
readable by anyone with access to the run's subjects, kept 30 days, purged
with the run. Handlers MUST NOT write secrets into records. A worker with
NATS write access to `journal.>` could forge records for another run; the
same holds for history today. Per-run subject permissions (NATS accounts or
worker tokens scoped by run) close this and are out of scope here.

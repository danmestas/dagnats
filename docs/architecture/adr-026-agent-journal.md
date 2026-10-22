# ADR-026: Agent journal

**Status:** Accepted (2026-10-21)
**Deciders:** Dan Mestas
**Depends on:** ADR-025
**Reverses:** ADR-002's rejection of per-tool-call durability
**Issues:** #804, #805

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
`{run}.{step}.{attempt}.{iteration}.{delivered}.{index}`, where
`delivered` is the task message's delivery count and `index` the record's
position within its claim (the claim is 0). The delivery count is part of
the claim identity because from iteration 1 the engine stamps the attempt
on the payload (`dispatchIdentity`), so a stalled worker and the AckWait
redelivery that replaces it hold the same run, step, iteration, attempt
and nonce; without it their records would share msg-ids, the successor's
claim would be answered with the stalled claim's sequence inside the
dedup window, and nothing would ever fence. JetStream rejects an append
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
| `claim` | SDK | Takes ownership; feeds the crash-loop guard (ADR-028) |
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
input types are reserved here and written by the helpers of #805 and
ADR-027; this ADR's implementation writes `claim`, `pause`, `commit` and
the handler's `entry`.

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
  matches on attempt and iteration, not on delivery; on iteration 0 the
  attempt follows the delivery count, so there a crash between commit and
  publish still repeats the turn under the next attempt. The fix belongs
  in the engine: `dispatchIdentity` stamping the attempt on iteration 0
  as it does from iteration 1, deferred from this ADR.
- Otherwise it rebuilds from the last committed state plus the records of
  iteration N so far. Recorded model responses and tool results are reused,
  not redone.

### Tools and model calls

Deferred to #805; this ADR records the shape they build on. `tc.Tool(call,
Safe|Unsafe, fn)` appends `tool.intent`, runs the tool and appends
`tool.result`. On recovery:

- a recorded result is reused;
- an intent without a result is rerun only if both the recorded and the
  current declaration say `Safe`;
- otherwise an `interrupted` error result is recorded with any partial
  output, and the model sees it.

Each call has a stable idempotency key, `{run}.{step}.{iteration}.{call}`,
for tools to pass to external systems. `tc.Model` records each completed
model response. Streaming tokens stay on `PutStream`, which is documented as
a non-durable preview: a crash mid-stream re-sends the request.

### Compatibility

`Checkpoint` and `LoadCheckpoint` keep working on journal steps:
`Checkpoint` buffers the state the next commit carries, and
`LoadCheckpoint` returns, in order, the state checkpointed this delivery,
the pause marker `{"pause_resume": name}` when the iteration's newest
record is a `pause`, else the last committed state. `Pause` writes the
`pause` record instead of the `checkpoints` marker. A step without a
journal is untouched. The `checkpoints` bucket TTL and the HTTP bridge
actions (`journal.append`, `journal.read`, `load_checkpoint`) are the
follow-up to #804; the bridge's claim must carry the task message's
delivery count, which the bridge already derives from `NumDelivered` for
the attempt.

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

## Requirements

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in
all capitals.

| ID | Requirement | Proven by |
|---|---|---|
| R1 | Every journal append MUST carry `Nats-Expected-Last-Subject-Sequence` and a stable `Nats-Msg-Id`. | `TestConformance_AppendCarriesFenceAndStableMsgID` |
| R2 | A worker MUST stop handling a task once a record from another claim follows its own. | `TestConformance_RecordFromAnotherClaimFences`, `TestJournal_StalledWorkerIsFencedBySuccessor`; `SIGSTOP` takeover in `e2e/cluster` (follow-up) |
| R3 | A worker MUST NOT treat a rejection as fencing before re-reading the tail from the leader. | `TestConformance_FalseRejectionLostAck`, `TestConformance_FalseRejectionOwnRecordAtTailViaOtherNode`, `TestConformance_RacingClaimsLeaveExactlyOneWriter` (in-process rival, three nodes) |
| R4 | A claim MUST NOT fence a worker on a later iteration. | `TestConformance_EarlierIterationClaimNeverFencesLater`, `TestConformance_StaleIterationStopsWithoutWriting` |
| R5 | An iteration MUST end with exactly one commit for its attempt. | `TestConformance_OwnAttemptCommitIsHonoured`, `TestCheckStep_OneCommitPerIterationFromAcceptedAttempt`, chaos soak |
| R6 | Recovery MUST reuse recorded model responses and tool results. | Crash-point tests |
| R7 | An unfinished tool call MUST NOT be rerun unless both declarations are `Safe`. | Tool replay matrix |
| R8 | An unsafe tool's side effect MUST happen at most once across any crash schedule. | Crash-point tests, chaos soak |

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
  interrupted.
- The engine and worker can tell a stalled worker from its replacement.
- A new stream to operate and retain, and a protocol the bridge must expose.
- The legacy `checkpoints` bucket can be retired.
- `PutStream` output is documented as a non-durable preview.

## Security considerations

The journal holds model inputs, model outputs and tool results, which can
include sensitive data. It has the same access and retention as run history:
readable by anyone with access to the run's subjects, kept 30 days, purged
with the run. Handlers MUST NOT write secrets into records. A worker with
NATS write access to `journal.>` could forge records for another run; the
same holds for history today. Per-run subject permissions (NATS accounts or
worker tokens scoped by run) close this and are out of scope here.

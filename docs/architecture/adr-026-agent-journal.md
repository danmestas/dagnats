# ADR-026: Agent journal

**Status:** Proposed (2026-10-21)
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
cluster's replica count, a 30-day max age like `WORKFLOW_HISTORY`, purged
with the run.

### Appends

Every append carries `Nats-Expected-Last-Subject-Sequence` set to the last
journal sequence its writer saw, and `Nats-Msg-Id` set to
`{run}.{step}.{attempt}.{iteration}.{record}`. JetStream rejects an append
whose expected sequence is stale, so a worker that lost the tail cannot
write.

A rejection does not by itself mean the claim is lost. Three benign cases
also cause one: an append that landed but whose acknowledgement was lost; a
batch still applying on the same subject; and a tail read from a stale
replica. On rejection the writer re-reads the tail from the stream leader.
If the tail is its own record, the append succeeded. If it is another
record of its own claim, it retries with the actual sequence, at most 8
times. Only a record from another claim means the writer is fenced.

### Claims

On picking up a task the worker reads the tail from the stream leader (a
direct get can be served by a stale replica). If the tail belongs to a
later iteration than the task's, the task is stale and the worker stops
without writing. Otherwise it appends a `claim` record carrying the
iteration, attempt, dispatch nonce and delivery count. A claim fences claims
for the same or an earlier iteration, and never a worker on a later one.

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

Each record is a JSON body with a `Dagnats-Journal-Type` header and the
dispatch nonce in a header.

### Commits and recovery

An iteration ends with one `commit` message holding the next action
(continue, complete, fail or wait) and the state. It is one message, not an
atomic batch: on a one-node file stream the server stages a batch in a store
it syncs to disk, which costs 25 to 30 ms, against 0.15 ms for one fenced
message. After the commit is stored, the worker publishes the lifecycle
event of ADR-025 with the commit's journal sequence.

A worker that claims iteration N reads the journal back from the last
commit:

- If a commit exists for this attempt and iteration, the previous holder
  crashed between committing and publishing. The worker republishes that
  commit's lifecycle event (its message ID is stable) and acknowledges the
  task. The turn is not repeated.
- Otherwise it rebuilds from the last committed state plus the records of
  iteration N so far. Recorded model responses and tool results are reused,
  not redone.

### Tools and model calls

`tc.Tool(call, Safe|Unsafe, fn)` appends `tool.intent`, runs the tool and
appends `tool.result`. On recovery:

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

`Checkpoint` and `LoadCheckpoint` keep working on journal-enabled steps by
writing and reading the latest state. The `checkpoints` bucket gets a TTL
and is retired once no worker writes it. The HTTP bridge gains
`journal.append`, `journal.read` and `load_checkpoint` actions.

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
command checks the other five on any run.

## Requirements

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in
all capitals.

| ID | Requirement | Proven by |
|---|---|---|
| R1 | Every journal append MUST carry `Nats-Expected-Last-Subject-Sequence` and a stable `Nats-Msg-Id`. | Journal conformance suite |
| R2 | A worker MUST stop handling a task once a record from another claim follows its own. | `SIGSTOP` takeover test, `e2e/cluster` |
| R3 | A worker MUST NOT treat a rejection as fencing before re-reading the tail from the leader. | False-rejection tests (lost ack, batch in flight, stale replica) |
| R4 | A claim MUST NOT fence a worker on a later iteration. | Journal conformance suite |
| R5 | An iteration MUST end with exactly one commit for its attempt. | Journal checker, chaos soak |
| R6 | Recovery MUST reuse recorded model responses and tool results. | Crash-point tests |
| R7 | An unfinished tool call MUST NOT be rerun unless both declarations are `Safe`. | Tool replay matrix |
| R8 | An unsafe tool's side effect MUST happen at most once across any crash schedule. | Crash-point tests, chaos soak |

## Alternatives considered

- **Leases (ADR-024) for ownership.** A lease needs a clock and renewal, and
  the engine would check a token per event. The journal's sequence check
  fences on the same write that records progress.
- **Atomic batches for every commit.** Measured at 25 to 30 ms on a one-node
  stream. Kept only for state too large for one message.
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

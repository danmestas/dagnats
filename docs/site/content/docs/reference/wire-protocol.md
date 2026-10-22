---
title: Wire Protocol
weight: 3
---

DagNats supports two transport modes for workers: **NATS** (native) and **HTTP** (via bridge). Both use the same JSON schemas for task payloads and resolutions, ensuring consistent semantics across languages and runtimes.

## NATS Transport

Workers connect directly to NATS JetStream and subscribe to task subjects.

### Task Subjects

Task subjects follow the pattern `task.{type}[.={workerGroup}].{runID}`. The run ID is always the last token. A run ID is a 32-character lowercase hex string, so it is exactly one subject token.

| Subject | Matches |
|---------|---------|
| `task.llm.*` | All `llm` tasks, any run |
| `task.llm.run-abc` | The `llm` task for run `run-abc` only |
| `task.build.*` | All `build` tasks. Does **not** match `task.build.linux.*` |
| `task.render.=gpu.*` | All `render` tasks in worker group `gpu`. Does **not** match `task.render.gpu.*` |

A step's `task` is published verbatim, so workflow registration rejects a value that could not dispatch correctly. It must be 1 to 128 bytes of `A-Za-z0-9_.-`, must not start or end with `.`, and must not contain an empty token (`a..b`). Dots are legal: `dagger.call` is a valid task type. A step's `worker_group` follows the same rule.

The `=` before a group name keeps a grouped pair distinct from a dotted task type. Without it, task `render` in group `gpu` and the task type `render.gpu` would derive the same subject. `=` is rejected in both fields, so it cannot be forged.

A consumer's filter is anchored on exactly one trailing `*` token, not `>`. An ungrouped worker and a grouped worker for the same task type therefore never see each other's work. Durable consumer names follow the same rule: `workers-render` for the ungrouped pair, `workers-render-=gpu` for the grouped one.

Workers create durable pull consumers with manual ACK.

### TaskPayload Schema

Published to task subjects when the engine dispatches a step:

```json
{
  "task_id": "run-1.step-a",
  "run_id": "run-1",
  "step_id": "step-a",
  "iteration": 0,
  "attempt": 1,
  "input": {"key": "value"}
}
```

Canonical Go type: `protocol.TaskPayload` in `protocol/protocol.go`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `task_id` | string | Yes | Unique task identifier: `{run_id}.{step_id}` |
| `run_id` | string | Yes | Workflow run identifier |
| `step_id` | string | Yes | Step identifier from workflow DAG |
| `iteration` | int | No | Agent-loop iteration (omitted for the first execution) |
| `attempt` | int | No | Retry attempt number (1-based) |
| `input` | JSON | No | Step input data as raw JSON |
| `metadata` | map[string]string | No | The step's static `metadata`, delivered to the worker as `TaskContext.Metadata()` |
| `required_capabilities` | []string | No | Copied from the step's `required_capabilities` |
| `dispatch_nonce` | string | No | Per-dispatch token a worker carries on control-plane requests so the server can verify it received this dispatch |
| `workflow_name` | string | No | Workflow definition name, used for span attributes |
| `traceparent`, `tracestate` | string | No | W3C trace context, so a worker's span parents onto the dispatch span |

### Task Resolution

Workers publish lifecycle events back to the history stream:

| Action | Event Type | Subject | Description |
|--------|-----------|---------|-------------|
| Complete | `step.completed` | `history.{runID}` | Task finished successfully with output payload |
| Fail | `step.failed` | `history.{runID}` | Task failed. The payload carries `error`, `failure_type` and, for `retry_after`, `retry_after_ms` |
| Continue | `step.continue` | `history.{runID}` | Agent loop requesting next iteration |

`failure_type` is `retriable` (the default), `non_retriable` or `retry_after`.

Use `protocol.NewStepEvent()` to construct events with correct subject and dedup ID.

#### Dispatch identity

`step.continue`, `step.completed` and `step.failed` carry the identity of the dispatch they answer: `iteration` (from the task payload), `attempt_number` (the attempt reported on `step.started`) and `dispatch_nonce` (from the task payload). The engine acts on such an event only while the step is still on that dispatch: Running on that iteration and attempt and, when a nonce is present, that nonce; or Queued on that iteration with a matching nonce, which happens in a cluster when the engine handling the event has not yet seen the worker's `step.started` applied (the attempt is not compared then; the engine adopts the event's `attempt_number`). Anything else (a worker whose claim lapsed and was taken over, a duplicate continue, a redelivered event) is dropped and counted in `engine.step_events.rejected`. Message IDs are `{run}.{step}.{type}.{attempt}.{iteration}`.

`step.started` is not fenced but carries the task's `dispatch_nonce` as well. The engine ignores a `step.started` whose nonce differs from the step's current one: the late start of a dispatch the step has already left, such as an attempt whose failure was accepted while the step was Queued and whose retry is already pending under a fresh nonce.

The worker SDK and the HTTP bridge stamp these fields for you. Events without them, from workers built before this change, are accepted while the engine setting `legacy_step_events` is on, which is the default. Upgrade engines before workers. Map instances and `respond` steps are not fenced.

#### Cancel signal

When a run is cancelled, the engine publishes an empty core NATS message on `dagnats.cancel.{runID}`. The worker SDK ends the `Context()` of every in-flight handler of that run; handlers should pass that context to their model and tool calls and return when it ends. The signal is at-most-once, so `Heartbeat()` also checks the run state and returns `worker.ErrRunCancelled` when the run is cancelled. Over HTTP the same condition is a `409 Conflict` on resolve and heartbeat.

#### Agent journal (ADR-026)

A step may keep a journal: one append-only subject `journal.{run_id}.{step_id}` on the `AGENT_JOURNAL` stream (file storage, the cluster's replica count, 30-day max age, a two-minute dedup window, direct get disabled so a tail read is answered by the stream leader, purged with the run by `dagnats clean`). The worker SDK opts a step in when its handler calls `Journal()`; a handler that never calls it runs as before. Opt-in must be rolled out to every worker of a task type together: a journal step's `step.continue` carries no state, so an older handler build on the next iteration would read a stale `TaskPayload.state`. The HTTP bridge actions `journal.append`, `journal.read` and `load_checkpoint` are the follow-up to #804; the bridge's claim must carry the task message's delivery count (`NumDelivered`, which the bridge already derives for the attempt).

**Record envelope.** Each record is one JetStream message. Identity lives in headers; the body is the record's JSON.

| Header | Value |
|--------|-------|
| `Nats-Msg-Id` | `{run_id}.{step_id}.{attempt}.{iteration}.{delivered}.{index}` — stable, so a resent append is deduplicated |
| `Nats-Expected-Last-Subject-Sequence` | the journal sequence the writer last saw (`0` on an empty journal) |
| `Dagnats-Journal-Type` | `claim`, `pause`, `commit`, `entry`, `model.response`, `tool.intent`, `tool.result`, `input.placed` |
| `Dagnats-Journal-Iteration`, `Dagnats-Journal-Attempt` | the writer's claim: the task's iteration and 1-based attempt |
| `Dagnats-Journal-Delivered` | the task message's delivery count. From iteration 1 the engine stamps the attempt, so a stalled worker and its AckWait redelivery share every other field; the delivery count is what tells their claims apart |
| `Dagnats-Journal-Index` | the record's position within its claim; the claim is `0` |
| `Dagnats-Dispatch-Nonce` | `TaskPayload.dispatch_nonce` |

Bodies: `claim` `{"iteration","attempt","dispatch_nonce","delivered"}`; `pause` `{"name","duration_ms"}`; `commit` `{"action":"continue"|"complete"|"fail","output","state","error","failure_type","retry_after_ms"}`; `entry` is the handler's JSON, opaque to dagnats.

**Client read.** `GET /runs/{run}/steps/{step}/journal?from={seq}&follow=1` serves the journal to clients (see "Step journal" in the REST reference). The cursor is the record's JetStream sequence. A page returns up to 1024 records from `from` and `next_from`; `follow=1` streams `event: record` frames whose `id:` is the sequence, and a reconnect resumes through `Last-Event-ID` (one past it), which wins over `from=` and applies to page reads too (a value of 2^64-1 has no successor and is a `400`). Follows are capped at `LogFollowConcurrentMax` in a counter separate from the log follows', share `LogFollowDurationMax` and the keepalive, and ends with `event: eof` once the step is terminal. There is no `agent.loop.iteration` event: the `commit` record with `action: continue` is the per-iteration signal, and `step.continue` already carries `journal_seq`.

**Claims.** Before writing, the worker reads the tail from the leader. A tail from a later iteration means the task is a late redelivery: nothing is written and the message is acknowledged. Otherwise the worker reads the journal back and appends a `claim`. A claim fences every earlier claim for the same or an earlier iteration, in journal order; it never fences a later iteration.

**Appends and rejections.** JetStream rejects an append whose expected sequence is stale (error `10071`, naming the real tail) and, on a clustered stream, one that arrives while another expected-sequence publish on the subject is in process (`10164`). Neither is fencing by itself. The writer re-reads the tail from the leader: the record it just sent (same `Nats-Msg-Id`) means the append landed and its acknowledgement was lost; another record of its own claim, or a record from an earlier iteration, means resync and retry with the real sequence; an unmoved tail means wait briefly and retry. At most 8 resyncs. A record from another claim for the same or a later iteration means the writer is fenced: it stops, publishes no further lifecycle event, and leaves the task message to its successor.

**Commits.** An iteration ends with one `commit` record (one message, not an atomic batch), then the lifecycle event, which carries the commit's journal sequence as `journal_seq`. For a journal step `step.continue` carries no `state`: the commit record is the state of record and the next delivery reads it from the journal, so `loop_state_max_bytes` does not apply. `Pause` appends a `pause` record instead of the `checkpoints` marker; `LoadCheckpoint` returns `{"pause_resume": name}` on the resumed delivery, else the last committed state.

**Recovery.** A `commit` for the task's own attempt and iteration means the previous holder crashed between committing and publishing: the worker republishes that commit's lifecycle event (its msg-id is stable) and acknowledges the task without repeating the turn. Otherwise the iteration starts from the last commit of an earlier iteration, with this iteration's earlier records available for reuse.

**Audit.** `dagnats journal check <run-id>` reads the journal, the run's history and snapshot, and the `submissions` bucket when it exists, and checks: one commit per iteration from the accepted attempt; no record after the claim that fenced its writer; every `tool.intent` paired with a `tool.result`; every placed input once; the engine's iteration count equal to the accepted `continue` commits. It exits 1 on a violation.

### Heartbeat

Workers register in the `workers` KV bucket on startup via `worker.Directory`. The bucket has a **60s TTL**, so workers must re-PUT their registration every **30s** to remain visible. Deregistration happens automatically on TTL expiry or explicit DELETE.

---

## HTTP Transport

The bridge serves four endpoints for HTTP workers, under `/v1`. No env token = open bridge (dev mode); set `DAGNATS_BRIDGE_TOKEN` and every worker needs either the env token (admin/root) or a minted one. The admin token can mint scoped, revocable worker tokens (`dgn_{id}_{secret}`) via [`POST /v1/tokens`](../rest-api#tokens) for individual machines instead of distributing the admin credential itself.

### POST /v1/workers/connect

Registers a worker and maintains a Server-Sent Events (SSE) heartbeat stream. The bridge sends periodic heartbeat events to keep the connection alive and refreshes the worker's KV TTL.

**Request:**

```json
{
  "worker_id": "worker-123",
  "task_types": ["llm", "http"],
  "max_tasks": 2,
  "worker_group": "quarry"
}
```

`worker_group` is optional and names the group this worker drains, so `GET /v1/workers` can report who serves it. A group that is invalid is `400`; a group the token is not scoped to is `403`. When omitted, the registration records the token's own group scope: the group (or groups) of a group-scoped token, and none for an unscoped token.

**Response:** SSE stream with `heartbeat` events every 25 seconds.

**Behavior:**
- Worker is registered in the `workers` KV bucket
- Heartbeat events are sent every 25s to maintain connection
- Worker is deregistered on disconnect

### POST /v1/tasks/poll

Long-polls for available tasks from the TASK_QUEUES stream.

**Request:**

```json
{
  "task_types": ["llm"],
  "worker_group": "repo-alpha",
  "max_tasks": 1,
  "timeout_ms": 30000
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `task_types` | []string | Yes | Task types to poll for |
| `worker_group` | string | No | Worker group to poll. Omit it to poll the ungrouped queue. A token scoped to specific groups can poll only those groups (`403` otherwise) |
| `max_tasks` | int | Yes | Maximum tasks to return |
| `timeout_ms` | int | Yes | Long-poll timeout in milliseconds (max 60000) |

**Response:**

```json
[
  {
    "task_id": "run-1.step-a",
    "run_id": "run-1",
    "step_id": "step-a",
    "iteration": 0,
    "attempt": 1,
    "input": {"prompt": "hello"}
  }
]
```

Each element has the `TaskPayload` fields `task_id`, `run_id`, `step_id`, `iteration`, `attempt`, `input` and `metadata`, plus `traceparent` and `tracestate` for trace propagation. Returns empty array `[]` on timeout. Each fetched message is stored in an in-memory ack map keyed by `task_id`. A poller shares one durable consumer per task type and worker group with every other poller of that pair, native workers included.

### POST /v1/tasks/{id}/resolve

Resolves a polled task. Only the token that claimed the task (or the admin token) may resolve it.

**Request:**

```json
{
  "action": "complete",
  "output": {"result": "ok"},
  "error": "error message",
  "failure_type": "retriable",
  "retry_after_ms": 5000,
  "name": "pause name",
  "duration_ms": 5000,
  "checkpoint": {"state": "..."},
  "data": {"incremental": "..."},
  "run_id": "run-1",
  "timeout_ms": 30000
}
```

**Actions:**

| Action | Fields Used | Behavior |
|--------|------------|----------|
| `complete` | `output` | Publishes `step.completed` event, ACKs message, removes from ack map |
| `fail` | `error`, `failure_type`, `retry_after_ms` | Publishes `step.failed` event with the failure type (`retriable` by default), ACKs message, removes from ack map |
| `continue` | `output` | Publishes `step.continue` event for an agent loop, ACKs message, removes from ack map |
| `pause` | `duration_ms`, `checkpoint` | Writes checkpoint to KV, NAKs with delay (`duration_ms` in 1 to 3600000), removes from ack map |
| `checkpoint` | `data` | Writes incremental checkpoint to KV, extends ack deadline (InProgress) |
| `heartbeat` | none | Extends the ack deadline without resolving the task |
| `stream` | `data` | Publishes `data` on core NATS subject `stream.{runID}.{stepID}` and extends the ack deadline |
| `send_signal` | `run_id`, `name`, `data` | Writes a signal to the `signals` KV bucket under `{run_id}.{name}`, extends the ack deadline |
| `wait_signal` | `name`, `timeout_ms` | Blocks until the named signal arrives and returns its data as the response body; `timeout_ms` in 1 to 3600000, `408` on timeout. Extends the ack deadline while waiting |

**Response:** `200 OK` on success, `404 Not Found` if task ID not in ack map, `403` if the caller did not claim the task, `409` if the run was cancelled.

### POST /v1/tasks/{id}/logs

Publishes captured stdout and stderr for a claimed task to the BUILD_LOGS hot lane. See "Consumer contract" below for the subject and chunk shape.

```json
{"chunks": [{"stream": "out", "data": "aGVsbG8K"}]}
```

`stream` is `out` or `err`; `data` is base64. A request holds at most 256 chunks and 1 MiB. The bridge assigns `seq`, and takes `attempt` and `iteration` from the claimed task, never from the body. A post for a task that already resolved is `409`; for a task that was never claimed, `404`.

---

## WorkerRegistration Schema

Workers register their presence in the `workers` KV bucket:

```json
{
  "worker_id": "worker-123",
  "task_types": ["llm", "http"],
  "language": "python",
  "transport": "bridge",
  "max_tasks": 2,
  "worker_groups": ["gpu"],
  "pid": 4821,
  "hostname": "build-01",
  "version": "1.0.0",
  "metadata": {"team": "ml"},
  "last_seen": "2026-08-28T12:00:00Z"
}
```

`worker_groups`, `pid`, `hostname`, `version` and `metadata` are optional. `last_seen` is stamped on every write.

Canonical Go type: `worker.WorkerRegistration` in `worker/directory.go`.

---

## Task Lifecycle

1. **Connect** (HTTP only): Worker registers via `/v1/workers/connect` and maintains SSE heartbeat
2. **Poll**: Worker polls for tasks via NATS subscription or `/v1/tasks/poll`
3. **Execute**: Worker processes task using input from TaskPayload
4. **Resolve**: Worker completes/fails via event publishing (NATS) or `/v1/tasks/{id}/resolve` (HTTP)

---

## Pause and Checkpoint Semantics

**Pause** suspends task execution for a fixed duration. The worker writes a resume marker to the `checkpoints` KV bucket and NAKs the message with delay. After the delay expires, the task is redelivered to the same or another worker with the marker available through `LoadCheckpoint`.

**Checkpoint** in an agent loop records the state the next iteration starts from. The state is buffered and sent inside the `step.continue` event, so saving and continuing are one write: the engine stores it with the run in the same save that advances the iteration, and hands it to the next iteration and to any retry in the task payload. State is capped per step (`loop_state_max_bytes`, 256 KiB by default); `Continue` fails over the cap. `Checkpoint(nil)` clears the state. Against an engine older than this behaviour, the worker also writes the state to the `checkpoints` KV bucket.

Time spent paused counts toward the step's `timeout`: a resumed task continues the attempt's timeout rather than starting a new one, so a step that pauses repeatedly needs a `timeout` covering its whole run.

The `checkpoints` KV bucket uses keys formatted as `{run_id}.{step_id}`.

---

## Idempotency and Deduplication

| Mechanism | ID Format | Scope |
|-----------|----------|-------|
| NATS message dedup | `Nats-Msg-Id` header | The receiving stream's duplicate window (set per stream) |
| Event dedup | `{run_id}.{step_id}.{event_type}` | Prevents duplicate events on replay |
| Rate retry dedup | `{run_id}.{step_id}.rate_retry` | Prevents duplicate retries |

---

## Authentication

| Transport | Method |
|-----------|--------|
| HTTP bridge | Bearer token via `Authorization: Bearer {token}` header — the `DAGNATS_BRIDGE_TOKEN` admin bearer, or a minted worker token scoped to task-type prefixes ([`/v1/tokens`](../rest-api#tokens)). Missing or invalid tokens return `401 Unauthorized`. A worker token polling a task type outside its `task_type_prefixes`, or a worker group outside its `worker_groups`, returns `403`. |
| NATS native | NATS native authentication (user/password, tokens, NKey, JWT). |

---

## Implementation Limits

| Parameter | Limit |
|-----------|-------|
| Pause duration | 1 hour (3,600,000 ms) |
| Poll timeout | 60 seconds (60,000 ms) |
| Worker KV TTL | 60 seconds |
| Worker heartbeat interval | 30 seconds (NATS), 25 seconds (HTTP SSE) |
| Signal payload size | 1 MiB |

---

## Annotations in TaskResolution.Data

A worker may put a blessed (but optional) shape into `TaskResolution.Data` so a forge integration (GitHub, GitLab, etc.) can pin failure or warning markers onto a diff view. This is a paper contract only: the engine reads `Data` as an opaque `json.RawMessage` and never parses it for engine-level decisions. A worker that emits some other shape into `Data`, or nothing at all, loses nothing -- only a forge-integration consumer that specifically understands this shape benefits from it.

```json
{
  "annotations": [
    {
      "path": "main.go",
      "line": 42,
      "column": 7,
      "severity": "error",
      "message": "undefined variable"
    },
    {
      "path": "util.go",
      "line": 10,
      "severity": "warning",
      "message": "unused import"
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | File path the annotation applies to |
| `line` | int | Yes | 1-based line number |
| `column` | int | No | 1-based column number |
| `severity` | string | Yes | One of `error`, `warning`, `notice` |
| `message` | string | Yes | Human-readable finding text |

Severities are `error`, `warning`, `notice` (`protocol.AnnotationSeverityError`, `protocol.AnnotationSeverityWarning`, `protocol.AnnotationSeverityNotice`). `protocol.AnnotationsMax` (1000) is a documented ceiling that consumers may size their own buffers or API batch calls against -- the engine does not enforce it, since it never parses `Data`.

Canonical Go types: `protocol.Annotation` and `protocol.Annotations` in `protocol/annotation.go`.

---

## Workflow Re-registration and def_hash

`POST /workflows` with a name that already has a registered definition **replaces** the `name -> latest definition` pointer in the `workflow_defs` KV bucket -- the same overwrite-by-name behavior `Service.RegisterWorkflow` has always had for *new* runs of that name.

**In-flight runs are pinned and are not affected.** Every run is stamped with `def_hash` (`WorkflowRun.DefHash`, `dag.NewWorkflowRun`) at the moment it is constructed from a definition -- before any dynamic-planner steps are added. `RegisterWorkflow` additionally persists the definition under an immutable, content-addressed key, `name.v.<hash>`, alongside the mutable `name -> latest` pointer, in the same `workflow_defs` bucket. `Orchestrator.loadRunAndDef` reads a run's def via that pinned `name.v.<hash>` key, not the mutable pointer, whenever `DefHash` is set -- so a `POST /workflows` re-register that lands mid-flight changes what a *new* run of that name sees, but never what an *already-started* run's next advance sees. Dynamic-planner steps recorded on the run (`WorkflowRun.DynamicSteps`) are still layered on top of the pinned base via `dag.EffectiveDef`, exactly as before.

Exactly one fallback exists: a run snapshot written before this feature (`DefHash == ""`) falls back to the mutable `name -> latest` pointer -- today's pre-pinning behavior, preserved for compatibility.

**A run *with* a `DefHash` whose pinned version key is missing does NOT blindly fall back.** Falling back there would reopen the exact hazard this feature exists to close -- a running run silently picking up whatever the mutable pointer currently holds -- just via a different door (retention evicting a version still in use, instead of a raw re-register). Instead `Orchestrator.selfHealMissingVersion` reads the mutable pointer and compares its content hash to `run.DefHash`. **Equal hashes are a proof, not a guess:** they mean the pointer's content is byte-identical to what the run is pinned to, the same guarantee a version-key hit would have given. In that case the missing version key is written (`Create`, tolerant of a racing self-heal via `ErrKeyExists`) and the advance proceeds normally -- this is the migration path for every workflow registered before this feature existed, whose `workflow_defs` entry has only ever had the mutable pointer. Only a **hash mismatch** (the pointer has moved on to different content) falls through to the loud failure: an error naming the run and the missing hash, logged at warn and counted via the `engine.def_pin.missing_version` metric. A stuck-but-correct run is the safe failure mode. Separately, a stored version whose *content* doesn't hash back to the key it was read under is bucket corruption, not an operating condition -- also returned as an error, never silently substituted.

**Retention.** `workflow_defs` retains at most `DefVersionsMax` (32) immutable versions per workflow name. When a register would exceed that cap, the oldest version with no non-terminal run still pinned to it (via `DefHash`) is evicted first -- and the version the mutable pointer itself currently references is always excluded from eviction, even if no run pins it (re-registering byte-identical content for an old version moves the pointer back onto it; without this exclusion a later register could delete the pointer's own target). If every retained version is still referenced, the register is refused rather than evicting one a running run needs: `POST /workflows` returns **409 Conflict** with `{"error": "too many live workflow versions", "live_versions": <n>}`.

The liveness scan behind retention (which runs are pinned to which version) is **best-effort and window-bounded** -- it checks the most-recent `MaxRunsLimitCeiling` runs system-wide, the same bound every other run-population scan in the control plane accepts, not an exhaustive scan of every run that ever existed. A non-terminal run outside that window is invisible to it and its version could be judged evictable when it isn't. This gap is deliberately not closed by widening the scan; what makes a miss survivable is the fail-loud rule above -- a wrongly-evicted version makes that run's next advance error loudly (and increments `engine.def_pin.missing_version`) instead of silently corrupting its behavior. A miss is a retention bug to go fix, never a run-correctness bug.

Concurrent `POST /workflows` calls for the same name are **last-writer-wins** on the mutable pointer, exactly as `RegisterWorkflow` has always been -- version persistence adds a content-addressed record alongside that, not locking. The retention cap itself is enforced per call, not atomically across concurrent registers: two `POST /workflows` calls for the same name with *distinct* content, racing each other, can both observe the same pre-write version count and both proceed, transiently landing one version over `DefVersionsMax` until the next register re-checks and evicts. And a version write can succeed while the immediately-following pointer `Put` fails (a mid-request crash, a transient KV error): the version key is left orphaned -- harmless (nothing pins it yet, and it still counts toward `DefVersionsMax` so it's evicted like any other unreferenced version) and bounded by the same retention cap as every other version.

Both `POST /workflows` and each entry of `GET /workflows` include a `def_hash` field: the hex-encoded SHA-256 of the server's canonical JSON marshal of the definition (`dag.DefHash`, `dag/hash.go`). `GET /runs/{id}` also includes `def_hash` -- the value the run is pinned to. Determinism comes from two `encoding/json` guarantees, not custom canonicalization -- map keys are sorted before marshaling and struct fields are always emitted in declaration order, so two field-for-field-equal `WorkflowDef` values hash identically regardless of how their maps were populated. This does not extend to the `json.RawMessage` fields (`input_schema`, `output_schema`, step `config`): those are hashed verbatim as whatever bytes they hold, so two schemas that are semantically equal but differ in whitespace or key order hash differently.

A caller that re-registers a workflow on every trigger can fetch the current `def_hash` (via `GET /workflows` or by caching the value from its last `POST /workflows` response), compute `dag.DefHash` locally over the definition it is about to send, and skip the `POST /workflows` round-trip entirely when the two hashes match.

## Step State Timestamps

Not part of TaskPayload -- these live on `dag.StepState` in the run snapshot
returned by `GetRun`, so UIs can render per-step durations and a waterfall
without extra queries:

| Field | Type | Description |
|-------|------|--------------|
| `started_at` | RFC3339 timestamp | When the engine decided to dispatch this step (marked it Queued and stamped `dispatch_nonce`) -- not when the worker picked it up. A retry re-stamps this to the new attempt's dispatch time. |
| `completed_at` | RFC3339 timestamp | When the step reached a terminal status (Completed or Failed). |

Both fields are additive: legacy snapshots written before this field existed
deserialize with them absent (nil). Worker-reported `duration_ms` on task
resolution remains the execution-only figure; `started_at`/`completed_at`
capture the engine's end-to-end dispatch-to-terminal window instead.

---

## Consumer contract: run lifecycle events

Two distinct streams carry run/step lifecycle information. They serve
different purposes and neither replaces the other.

### `history.{runID}` -- per-step timeline (WORKFLOW_HISTORY stream)

- **Payload:** `protocol.Event` (see NATS Transport above) -- `step.queued`,
  `step.started`, `step.completed`, `step.failed`, `step.continue`,
  `workflow.completed`, `workflow.failed`, etc.
- **Ordering:** JetStream per-subject order only. Every event for one run
  shares the single `history.{runID}` subject, so events *within a run*
  arrive in publish order. There is no cross-run or cross-subject
  ordering guarantee.
- **Retention:** `historyMaxAge` -- 30 days (`internal/natsutil/conn.go`).
- **Use for:** reconstructing a run's full step-by-step timeline
  (waterfall views, per-step duration, debugging a specific run).

### `event.run.{workflow}.{runID}.{status}` -- reliable run-terminal notification (EVENTS stream)

- **Payload:** `protocol.RunEvent` (`protocol/run_event.go`):

```json
{
  "type": "run.completed",
  "run_id": "run-abc",
  "workflow_id": "my-workflow",
  "status": "completed",
  "created_at": "2026-08-28T12:00:00Z",
  "completed_at": "2026-08-28T12:00:05Z",
  "trace_parent": "00-...-01",
  "trigger_depth": 0
}
```

| Field | Type | Notes |
|-------|------|-------|
| `type` | string | One of `run.completed`, `run.failed`, `run.cancelled`. Compensation outcomes (`compensated`, `compensate_failed`) report as `run.failed` -- compensation only runs after the workflow itself failed. |
| `run_id` | string | The run. |
| `workflow_id` | string | The workflow definition name. |
| `status` | string | The exact `dag.RunStatus` string (`completed`, `failed`, `cancelled`, `compensated`, or `compensate_failed`) -- finer-grained than `type`. |
| `created_at` | RFC3339 timestamp | Run creation time. |
| `completed_at` | RFC3339 timestamp, omitempty | When the run reached its terminal status. |
| `labels` | map[string]string, omitempty | Copied from `dag.WorkflowRun.Labels` at finalization. Absent for a run with no labels. |
| `trace_parent` | string, omitempty | W3C traceparent so a consumer's processing continues the run's trace. |
| `trigger_depth` | int, omitempty | `dag.WorkflowRun.TriggerDepth`. 0 for a manual/HTTP/cron-started run, source run's depth + 1 for a run started by a `run_terminal` trigger. See [Run Terminal Trigger](/docs/triggers/run-terminal). |

- **Subject wildcard recipes:**
  - `event.run.*.*.failed` -- every failure across every workflow.
  - `event.run.myflow.>` -- every terminal event for the `myflow` workflow,
    any run, any status.
  - Filter by run ID with `event.run.*.{runID}.*`, or consume `event.run.>`
    and filter client-side.
- **Ordering:** per-subject JetStream order; since each run publishes at
  most one terminal event, cross-run ordering is not meaningful.
- **Delivery:** at-least-once. Dedup key is `Nats-Msg-Id: run-terminal-{runID}`
  -- a run reaches a terminal status exactly once, so redelivery of the
  same finalize collapses to one message within JetStream's dedup window.
- **Retention:** `eventsMaxAge` -- 14 days (`internal/natsutil/conn.go`).
- **Failure policy:** publishing this event is best-effort. A publish
  failure is logged (WARN) and counted (`engine.run_event.publish_failures`)
  but never fails the run -- `history.{runID}` and the persisted
  `WorkflowRun` snapshot remain the source of truth. The event is
  published only after the terminal snapshot write succeeds.
- **Use for:** the reliable "this run just finished" signal a forge or
  webhook relay needs. **Pollers (`GET /runs/{id}` on a timer) become a
  fallback path, not the primary integration path** -- subscribe to
  `event.run.>` instead of polling; fall back to polling only to recover
  from a missed message during a consumer outage.

### `logs.{runID}.{stepID}.{attempt}.{iteration}` -- captured step stdout/stderr (BUILD_LOGS stream)

dagnats owns the JetStream **hot lane only** -- a bounded, short-TTL buffer
of a step's captured output. There is no S3 offload, no cache, no
long-term index. **Retention past the hot TTL is a consumer's job**, the
same way `history.{runID}` and telemetry already work: a forge that needs
a verdict's logs to stay explainable for years drains
`logs.{runID}.{stepID}.{attempt}.{iteration}` into its own store next to the verdict,
before the TTL elapses.

- **Payload:** `protocol.LogChunk` (`protocol/log_chunk.go`):

  ```json
  {
    "seq": 3,
    "attempt": 1,
    "iteration": 0,
    "ts": "2026-08-28T12:00:00.125Z",
    "stream": "out",
    "data": "aGVsbG8gd29ybGQK"
  }
  ```

  `seq` is monotonic per ATTEMPT, shared across `out`/`err`/`marker` --
  ordering by `seq` reconstructs write order even though stdout and
  stderr buffer independently. `iteration` is the agent-loop iteration (0 for non-loop steps). `attempt` is the 1-based `AttemptNumber`
  this chunk belongs to -- the SAME numbering `step.started`'s
  `AttemptNumber` and `dag.StepState.Attempts` use. `stream` is `"out"`,
  `"err"`, or `"marker"`; for `"marker"`, `data` carries `"completed"`,
  `"failed"`, `"continued"`, `"paused"`, or `"truncated"` instead of
  captured bytes. `data` is base64 on the wire.
- **Subject:** `logs.{runID}.{stepID}.{attempt}.{iteration}` -- `stepID` is
  sanitized with `natsutil.SubjectToken` before it goes into the
  subject; `runID` (a `nuid`) is never sanitized. `attempt` scopes the
  subject so a retry's chunks can never collide with (and silently
  drop as a duplicate of) the prior attempt's within BUILD_LOGS's
  2-minute dedup window -- query `logs.{runID}.{stepID}.>` for every
  attempt of one step.
- **Bounds** (`protocol/log_chunk.go`): `LogChunkBytesMax` 64 KiB per
  chunk, `LogStepBytesMax` 64 MiB per attempt (total across both streams),
  `LogReadChunksMax` 1024 chunks per non-follow read, `LogFollowDurationMax`
  1h per SSE follow, `LogFollowConcurrentMax` 256 concurrent follows per
  API server process.
- **Markers:** every path that ends a task attempt -- worker SDK
  `Complete`/`Fail`/`FailPermanent`/`FailRetryAfter`/`Continue`/`Pause`
  AND the HTTP bridge's `complete`/`fail`/`continue`/`pause` resolve
  actions alike -- emits exactly one of `"completed"`, `"failed"`
  (covers `Fail`/`FailPermanent`/`FailRetryAfter`, or bridge's single
  `fail` action distinguished by `failure_type`), `"continued"`, or
  `"paused"` as the TRUE LAST message on that attempt's subject -- so
  `GET .../logs?from=failure` resolves the failure position in O(1) via
  `GetLastMsgForSubject` instead of a scan. `from=failure` is strict:
  if the last message isn't a `"failed"` marker, the request 404s with
  `{"error":"attempt has no failure marker"}`. `"truncated"` is emitted
  at most once, the moment `LogStepBytesMax` is reached, BEFORE the
  attempt-ending marker (which still lands last).
- **Drain-before-resolve invariant:** the worker SDK's `Complete`, `Fail`,
  `FailPermanent`, `FailRetryAfter`, `Continue`, and `Pause` -- and the
  HTTP bridge's equivalent resolve actions -- all flush buffered log
  bytes and emit their attempt-ending marker BEFORE publishing their
  resolution event (or NAK-ing, for `Pause`). A bridge log POST for an
  already-resolved task is rejected `409` (not `404`). A consumer
  observing a step's terminal event, or the marker itself off a
  `follow=1` connection, is guaranteed every log byte that produced it
  is already on this subject.
- **Buffering:** writes flush at `LogChunkBytesMax` or ~250ms after the
  first unflushed byte, whichever comes first.
- **Dedup key:** `Nats-Msg-Id: log-{runID}-{stepID}-{attempt}-{iteration}-{seq}`.
- **Retention:** `DAGNATS_BUILD_LOGS_TTL` -- default 168h (7d), configurable
  in `[1h, 8760h]`; an invalid value refuses server startup
  (`internal/natsutil/build_logs.go`).
- **Ingest paths:** the worker SDK (`worker.TaskContext.LogOut()` /
  `LogErr()`) for Go workers; `POST /v1/tasks/{id}/logs` for non-Go
  workers via the HTTP bridge, whose `attempt` AND `iteration` are
  both read from the claimed task's own message (never caller-
  supplied). Both resolve `AttemptNumber` the same way and read
  `Iteration` straight off the payload, enforcing the same bounds and
  marker behavior.
- **Read path:** `GET /runs/{id}/logs?step=&attempt=&iteration=&cursor=&follow=&from=`
  (see "Run logs" in the REST API reference) -- non-follow pages through
  stored chunks via an opaque JetStream-stream-sequence cursor,
  `follow=1` upgrades to Server-Sent Events over a single long-lived
  consumer, ending with `event: eof` normally or `event: error` if that
  consumer itself fails.
- **Ordered consumers must be bounded:** if you read this stream with your
  own `jetstream` ordered consumer, set `MaxResetAttempts` explicitly. In
  nats.go v1.53.1 an unset value becomes `-1` (`jetstream/ordered.go`,
  `getConsumerConfig`) and the reset retry loop only stops when
  `attempts > 0` (`retryWithBackoff`), while `Next()` calls `reset()` on
  every invocation. If BUILD_LOGS goes away underneath you -- deleted, or
  aged out of its TTL -- `Next()` never returns: it recreates the consumer
  forever on a 1s/2s/4s/8s/10s backoff, on `context.Background()`, so your
  own context deadline cannot interrupt it. Bound it, and treat the
  resulting `ErrStreamNotFound` as "the hot lane is gone" -- the stream is
  recreatable and the read is retryable, so a 503 fits it better than a
  fatal error. Every dagnats reader applies this bound -- the API's
  paged and follow readers and every `dagnats` CLI command that scans
  BUILD_LOGS or TELEMETRY -- via `natsutil.OpenConsumer`/
  `natsutil.OpenStreamConsumer`, the single place any ordered consumer
  is configured or opened (enforced by a CI lint step).
- **Use for:** a live or historical tail of one attempt's captured output
  within the hot TTL window. Anything longer-lived belongs in a
  consumer's own store, drained before the TTL elapses.

**Breaking change for Go worker SDK consumers:** `worker.TaskContext`
gained `LogOut() io.Writer` and `LogErr() io.Writer`. An out-of-repo
type implementing `TaskContext` directly must add both (return
`io.Discard` from each if log capture isn't needed) -- see the
`TaskContext` doc comment in `worker/worker.go`.

### `event.queue.snapshot` -- periodic task-queue depth (EVENTS stream)

- **Payload:** `protocol.QueueSnapshot` (`protocol/queue.go`), the same
  shape `GET /v1/queue` returns:

```json
{
  "groups": [
    { "task_type": "build", "pending": 3, "oldest_wait_ms": 1523 },
    { "task_type": "test", "pending": 1, "oldest_wait_ms": 402 }
  ],
  "snapshot_at": "2026-08-28T12:00:00Z"
}
```

`groups` is always present (`[]` when nothing is pending, never
`null`), sorted by `task_type`. `truncated` (bool, omitted when false)
appears only when the stream carries more than 256 distinct task-type
subjects -- `groups` is capped to the first 256 in `task_type` order.

- **Cadence:** its own ticker, at `DAGNATS_QUEUE_SNAPSHOT_INTERVAL`
  (default 5s, bounded to [1s, 5m]; invalid or out-of-range refuses
  server startup) -- never per-enqueue. Published only when the queue
  state differs from the last publish (change-suppression, with
  `oldest_wait_ms` rounded to the nearest second for the comparison so
  clock drift on an idle queue doesn't count as a change), plus an
  unconditional heartbeat publish every 60s regardless of change, so a
  consumer with no message for over a minute can tell the publisher is
  down rather than the queue being idle.
- **Dedup key:** `Nats-Msg-Id: queue-snapshot-{snapshot_at RFC3339Nano}`.
- **Ordering:** per-subject JetStream order; every snapshot stands alone.
- **Retention:** `eventsMaxAge` -- 14 days (`internal/natsutil/conn.go`),
  same as `event.run.*`.
- **Source of truth:** the `TASK_QUEUES` stream's own state -- a
  subject-filtered `StreamInfo` plus a direct-get of the oldest pending
  message per subject (`Stream.GetMsg(WithGetMsgSubject(...))`, which
  requires `AllowDirect` on `TASK_QUEUES`), not a KV mirror or an
  in-memory engine counter. A direct-get failure for one subject omits
  that subject's `oldest_wait_ms` rather than failing the whole
  snapshot.
- **Labels are not a grouping dimension.** Pending tasks don't carry
  run labels in their subject or a cheap-to-read header, so grouping by
  label would mean fetching and decoding every pending payload --
  unbounded work. This event groups by `task_type` only (free -- it's
  the subject).
- **Use for:** a live queue-depth feed (dashboards, autoscalers,
  alerting on a growing `oldest_wait_ms`) without polling
  `GET /v1/queue` on a timer.

---

## Coordination leases (`api.leases.*`)

TTL leases that let the replicas of a downstream service elect a leader and
split shard ownership without importing dagnats code. ADR-024 records the
design. Leases are stored in the `leases` KV bucket. The NATS server enforces
each lease's TTL, and the key's KV revision is the **fencing token**.

### Request/reply subjects

These four subjects, and `api.leases.forward` below, belong to
the `dagnats-api` micro service and use queue
group `dagnats-leases`, so exactly one dagnats instance serves each request.
Request and reply bodies are JSON.

| Subject | Request | Success reply |
|---|---|---|
| `api.leases.acquire` | `{"key","holder","ttl_ms"}` | `{"granted","held","key","holder","revision","acquired_at","expires_at","ttl_ms"}` |
| `api.leases.renew` | `{"key","holder","revision","ttl_ms"}` | same shape as acquire, with a new `revision` |
| `api.leases.release` | `{"key","holder","revision"}` | `{"granted":false,"held":false,"key"}` |
| `api.leases.owner` | `{"key"}` | the acquire shape with `"held":true`, or `{"granted":false,"held":false,"key"}` |

```json
// api.leases.acquire request
{"key": "notify-leader", "holder": "notify-7f3a", "ttl_ms": 15000}

// reply: granted
{"granted": true, "held": true, "key": "notify-leader", "holder": "notify-7f3a",
 "revision": 42, "acquired_at": "2026-10-18T12:00:00Z",
 "expires_at": "2026-10-18T12:00:15Z", "ttl_ms": 15000}

// reply: someone else holds it (not an error)
{"granted": false, "held": true, "key": "notify-leader", "holder": "notify-19bc",
 "revision": 40, ...}
```

- **acquire** grants the key if nobody holds it. If another holder has it,
  the reply is `granted:false` and names that holder. If the caller already
  holds it, acquire is **idempotent**: it returns the existing lease with
  `granted:true` and the same revision, and it does **not** extend the TTL.
  To extend a lease, call renew.
- **renew** extends the lease to `ttl_ms` from now. The holder must match,
  and `revision` must be the current fence. The reply's `revision` becomes
  the new fence.
- **release** frees the key. It uses the same fence check as renew.
- **owner** is a read with no side effects.
- Every call reads through the stream leader (a non-direct `STREAM.MSG.GET`), so a lagging replica in an R3 cluster cannot return a stale owner.
- **Requires nats-server 2.11 or newer.**
- `expires_at` is advisory (the server's TTL is authoritative). Times are
  RFC 3339 in UTC.

**Errors** come back as `{"error": "...", "kind": "..."}`:

| `kind` | Meaning |
|---|---|
| `invalid` | Bad input: key, holder, `ttl_ms`, or missing `revision`. |
| `lost` | renew or release only. The caller does not hold the key at that revision, because it expired, was released, or now belongs to another holder. **Stop acting as owner now.** |
| `transport` | The request body is not valid JSON. |
| `internal` | A NATS error, such as a timeout. Retry. |
| `unsupported` | The `leases` bucket does not exist because the NATS server is older than 2.11, which lacks per-key TTL. dagnats skips the bucket and logs a warning. Retrying will not help; upgrade NATS. |

**Input bounds:**

- `key` and `holder` must match `[A-Za-z0-9_-]` and be 1 to 128 bytes. No
  dots, so a key is exactly one subject token.
- `ttl_ms` must be a whole number of seconds from 1000 to 600000. The server
  truncates a message TTL to whole seconds.

### Change events: `dagnats.lease.changed.<key>`

The NATS server republishes every write to the bucket onto
`dagnats.lease.changed.<key>`. Only the stream leader republishes, so each
change arrives once, however many dagnats nodes are running. Each event
carries `Nats-Sequence` (the revision) and `Nats-Subject`
(`$KV.leases.<key>`) headers.

| Event | Identify by | Body |
|---|---|---|
| acquired | `Dagnats-Lease-Event: acquired` | `{"holder","acquired_at","ttl_ms"}` |
| renewed | `Dagnats-Lease-Event: renewed` | same as acquired |
| released | `Dagnats-Lease-Event: released` | empty |
| expired | no `Dagnats-Lease-Event` header; `Nats-Marker-Reason: MaxAge` | empty |

Events are core NATS publishes, delivered at most once. A subscriber that
reconnects should call `api.leases.owner` to resync. If you need durable
delivery, consume the `KV_leases` stream directly. Use `Nats-Sequence` to
order events and to drop duplicates.

### Forwarding to the owner: `api.leases.forward`

`api.leases.forward` sends a request to whichever process holds a key,
without the sender knowing who that is. It is on the `dagnats-api`
service with queue group `dagnats-leases`, like the other lease subjects.

| Subject | Request | Success reply |
|---|---|---|
| `api.leases.forward` | `{"key","payload","timeout_ms"}` | `{"key","holder","revision","payload"}` |

```json
// request: payload is base64 (any bytes)
{"key": "notify-shard-07", "payload": "aGVsbG8=", "timeout_ms": 2000}

// reply: the owner's reply body, and the lease it was delivered under
{"key": "notify-shard-07", "holder": "notify-7f3a", "revision": 42,
 "payload": "b2s="}
```

dagnats reads the lease through the stream leader, then makes a NATS
request to the owner's inbox. The request body is `payload` as raw bytes,
and the request carries the lease revision as the fence. The owner's reply
body comes back as `payload`. Owner reply headers are not relayed.
`timeout_ms` must be 100 to 30000. It bounds the whole call, so set your
own request timeout a little higher than `timeout_ms`.

**Owner inbox convention.** A holder that accepts forwarded requests
subscribes on `dagnats.lease.inbox.<holder>.>`. A request for key `K`
arrives on `dagnats.lease.inbox.<holder>.<K>` with header
`Dagnats-Lease-Fence: <revision>`. Use plain core NATS subscriptions and
not a queue group: holder IDs are unique per process, so the subject
already names exactly one process. Senders that track the owner
themselves (for example from change events) may request this subject
directly with the fence header and skip the dagnats hop. The owner rule
below protects both paths.

**Owner rule: when to reject a fence.** Remember the revision your
**acquire** returned. That is the start of your tenure. Renew raises the
revision, but the tenure stays the same. Reject a request when **any** of
these is true:

- you are not currently acting as owner of `K` (you never acquired it, you
  released it, renew returned `lost`, or you are past `expires_at` without
  a successful renew);
- the `Dagnats-Lease-Fence` header is missing or is not an unsigned
  integer;
- the fence is lower than your tenure's acquire revision. A lower fence
  was read during an earlier tenure, yours or another holder's.

A fence that is equal to or higher than your tenure start is current.
It can be higher than the last revision you have seen while your own
renew reply is in flight. To reject, reply with an empty body and header
`Dagnats-Lease-Reject: stale`. Do not process the request. Also send
the fence on to any downstream writes you make while handling it (see
"Fence downstream writes" below).

**Retry rule.** dagnats re-sends only after an outcome that proves the
owner did **not** process the request: nobody is subscribed on its inbox
(no responders), or it replied with `Dagnats-Lease-Reject`. Then dagnats
reads the lease again. If the holder or revision changed, it tries the new
owner. That is how a handoff in flight succeeds. If nothing changed, it
stops and reports `owner_unavailable`. A call makes at most 3 attempts.
A timeout is never retried, because the owner may have processed the
request.

**Errors.** Forward uses the lease `{"error","kind"}` envelope. On an
owner error, `holder` and `revision` name the owner it last tried.

| `kind` | Meaning | Processed? |
|---|---|---|
| `no_owner` | Nobody holds the key. | No |
| `owner_unavailable` | The holder had no inbox subscriber or refused the fence, and the lease did not move to anyone else. The holder probably died; its lease expires within `ttl_ms`. | No |
| `owner_timeout` | The owner did not reply within `timeout_ms`. | **Maybe.** Retry only if the request is idempotent. |
| `invalid` | Bad `key` or `timeout_ms`. | No |
| `transport` | The request body is not valid JSON. | No |
| `reply_too_large` | The owner processed the request, but its reply does not fit the dagnats hop once base64-encoded (see below). | **Yes** |
| `busy` | This dagnats instance already has 256 forwards in flight. Retry shortly. | No |
| `internal` | A NATS error, a corrupt lease record, or dagnats stopping mid-call. | Maybe |
| `unsupported` | The `leases` bucket does not exist (nats-server older than 2.11). | No |

The payload counts against the NATS `max_payload` on both hops, and base64
adds a third to its size on the dagnats hop. An owner reply that fits its own
hop but not the dagnats reply (about 750 KB at the default 1 MB) comes back
as `reply_too_large`. Keep forwarded replies small, or reply with a
reference to where the result is stored.

**Do not add extra subscribers under `dagnats.lease.inbox.>`.** A
monitoring tap or a second process on an owner's inbox counts as a
responder. A dead owner then looks like a hung one: forward waits out
`timeout_ms` and reports `owner_timeout` instead of failing fast with
`owner_unavailable`.

### Patterns and the fencing contract

- **Leader election:** every replica calls acquire on one well-known key.
  The replica that is granted the lease is the leader. Everyone else
  watches `dagnats.lease.changed.<key>` and retries acquire on `released` or
  `expired`.
- **Shard ownership:** use one lease per shard key, such as `notify-shard-07`.
- **Renew loop:** renew about every `ttl_ms/3`. On `lost`, or if you cannot
  reach dagnats before `expires_at`, stop acting as owner immediately.
- **Fence downstream writes:** send the lease `revision` with every write
  you make as owner, and have the downstream store reject any revision lower
  than the highest it has seen. A paused holder that wakes up after its lease
  expired has no other protection against writing as a stale owner.
- `holder` IDs must be unique per process.

## Control-plane request/reply (`api.*`)

The `dagnats-api` micro service answers these subjects for clients that talk to NATS instead of HTTP. Requests and replies are JSON unless noted. A failure replies with an `error` field.

| Subject | Request | Reply |
|---------|---------|-------|
| `api.workflows.register` | A `WorkflowDef` | `{"status":"registered","name"}` |
| `api.runs.start` | `{"workflow","input"}` | `{"run_id"}` |
| `api.runs.get` | The run ID as plain text | The run snapshot |
| `api.runtimes.register`, `api.runs.spawn`, `api.runtimes.budget` | Gated-write requests for agent runtimes (ADR-021). Used by the worker SDK's control-plane handle | Result, or `{"error","kind"}` |
| `api.leases.*` | See [Coordination leases](#coordination-leases-apileases) | |
| `api.cluster.health` | Empty | The same JSON as `GET /health/cluster` |

The lease endpoints share the queue group `dagnats-leases`, so one instance serves each request. The others have no queue group, so every running instance receives the request.

## Idempotent child-run spawn (`api.runs.spawn`)

A gated handler spawns a child run with `api.runs.spawn`. The request is
`{child_workflow, parent_run_id, parent_step_id, input, nonce, key}`; the
reply is `{run_id}` or `{error, kind}`.

`key` is optional and additive. Without it every request spawns a new child.
With it, the server claims the child run ID with a KV `Create` in the
`idempotency_keys` bucket, under a slot derived from SHA-256 of
`(parent_run_id, parent_step_id, key)`. A repeat request with the same triple
returns the original `run_id` and spawns nothing, even if the child has
already finished or the tree is at its active-run quota.

- The key is scoped to the calling step, so two steps can reuse a key.
- The claim expires with the bucket TTL (24h); a repeat after that spawns a
  new child.
- Maximum key length is 256 bytes; longer keys get `kind: "invalid_def"`.
- The claim is written before the spawn event is published, and the event's
  `Nats-Msg-Id` includes the child run ID. If the server dies between the two,
  the next request with the same key finds the claim, sees that the child run
  does not exist, and republishes, so exactly one child results.

In the Go SDK, pass `worker.WithSpawnKey(key)` to `ControlPlane.StartRun`.

## Reference Implementations

- **Go (NATS):** `worker/` package
- **HTTP SDKs:** Implement against the four HTTP endpoints and JSON schemas above
- All Go types referenced in this document are canonical -- implement JSON serialization matching these types

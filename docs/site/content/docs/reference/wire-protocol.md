---
title: Wire Protocol
weight: 3
---

DagNats supports two transport modes for workers: **NATS** (native) and **HTTP** (via bridge). Both use the same JSON schemas for task payloads and resolutions, ensuring consistent semantics across languages and runtimes.

## NATS Transport

Workers connect directly to NATS JetStream and subscribe to task subjects.

### Task Subjects

Task subjects follow the pattern `task.{type}.{runID}`:

| Subject | Matches |
|---------|---------|
| `task.llm.*` | All LLM tasks |
| `task.http.*` | All HTTP tasks |
| `task.llm.run-abc` | LLM tasks for run-abc only |

Workers create durable pull consumers or ephemeral subscriptions with manual ACK.

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
| `iteration` | int | No | Agent-loop iteration (0 for first execution) |
| `attempt` | int | No | Retry attempt number (1-based) |
| `input` | JSON | No | Step input data as raw JSON |

### Task Resolution

Workers publish lifecycle events back to the history stream:

| Action | Event Type | Subject | Description |
|--------|-----------|---------|-------------|
| Complete | `step.completed` | `history.{runID}` | Task finished successfully with output payload |
| Fail | `step.failed` | `history.{runID}` | Task failed with error payload |
| Continue | `step.continue` | `history.{runID}` | Agent loop requesting next iteration |

Use `protocol.NewStepEvent()` to construct events with correct subject and dedup ID.

### Heartbeat

Workers register in the `workers` KV bucket on startup via `worker.Directory`. The bucket has a **60s TTL**, so workers must re-PUT their registration every **30s** to remain visible. Deregistration happens automatically on TTL expiry or explicit DELETE.

---

## HTTP Transport

The bridge exposes three endpoints for HTTP workers. No env token = open bridge (dev mode); set `DAGNATS_BRIDGE_TOKEN` and every worker needs either the env token (admin/root) or a minted one. The admin token can mint scoped, revocable worker tokens (`dgn_{id}_{secret}`) via [`POST /v1/tokens`](rest-api#tokens) for individual machines instead of distributing the admin credential itself.

### POST /v1/workers/connect

Registers a worker and maintains a Server-Sent Events (SSE) heartbeat stream. The bridge sends periodic heartbeat events to keep the connection alive and refreshes the worker's KV TTL.

**Request:**

```json
{
  "worker_id": "worker-123",
  "task_types": ["llm", "http"],
  "max_tasks": 2
}
```

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
  "max_tasks": 1,
  "timeout_ms": 30000
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `task_types` | []string | Yes | Task types to poll for |
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

Returns empty array `[]` on timeout. Each fetched message is stored in an in-memory ack map keyed by `task_id`.

### POST /v1/tasks/{id}/resolve

Resolves a polled task by completing, failing, pausing, or checkpointing it.

**Request:**

```json
{
  "action": "complete",
  "output": {"result": "ok"},
  "error": "error message",
  "name": "pause name",
  "duration_ms": 5000,
  "checkpoint": {"state": "..."},
  "data": {"incremental": "..."}
}
```

**Actions:**

| Action | Fields Used | Behavior |
|--------|------------|----------|
| `complete` | `output` | Publishes `step.completed` event, ACKs message, removes from ack map |
| `fail` | `error` | Publishes `step.failed` event, ACKs message, removes from ack map |
| `pause` | `duration_ms`, `checkpoint` | Writes checkpoint to KV, NAKs with delay, removes from ack map |
| `checkpoint` | `data` | Writes incremental checkpoint to KV, extends ack deadline (InProgress) |

**Response:** `200 OK` on success, `404 Not Found` if task ID not in ack map.

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
  "metadata": {"version": "1.0.0"}
}
```

Canonical Go type: `worker.WorkerRegistration` in `worker/directory.go`.

---

## Task Lifecycle

1. **Connect** (HTTP only): Worker registers via `/v1/workers/connect` and maintains SSE heartbeat
2. **Poll**: Worker polls for tasks via NATS subscription or `/v1/tasks/poll`
3. **Execute**: Worker processes task using input from TaskPayload
4. **Resolve**: Worker completes/fails via event publishing (NATS) or `/v1/tasks/{id}/resolve` (HTTP)

---

## Pause and Checkpoint Semantics

**Pause** suspends task execution for a fixed duration. The worker writes checkpoint state to KV and NAKs the message with delay. After the delay expires, the task is redelivered to the same or another worker with the checkpoint data available in KV.

**Checkpoint** saves incremental state without suspending execution. The worker writes data to KV and calls `InProgress()` to extend the ack deadline. The task remains in-flight and the worker continues execution.

Both mechanisms use the `checkpoints` KV bucket with keys formatted as `{run_id}.{step_id}`.

---

## Idempotency and Deduplication

| Mechanism | ID Format | Scope |
|-----------|----------|-------|
| NATS message dedup | `Nats-Msg-Id` header | JetStream duplicate window (default 2 min) |
| Event dedup | `{run_id}.{step_id}.{event_type}` | Prevents duplicate events on replay |
| Rate retry dedup | `{run_id}.{step_id}.rate_retry` | Prevents duplicate retries |

---

## Authentication

| Transport | Method |
|-----------|--------|
| HTTP bridge | Bearer token via `Authorization: Bearer {token}` header — the `DAGNATS_BRIDGE_TOKEN` admin bearer, or a minted worker token scoped to task-type prefixes ([`/v1/tokens`](rest-api#tokens)). Missing or invalid tokens return `401 Unauthorized`; an out-of-scope worker token returns `403`. |
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
    "ts": "2026-08-28T12:00:00.125Z",
    "stream": "out",
    "data": "aGVsbG8gd29ybGQK"
  }
  ```

  `seq` is monotonic per ATTEMPT, shared across `out`/`err`/`marker` --
  ordering by `seq` reconstructs write order even though stdout and
  stderr buffer independently. `attempt` is the 1-based `AttemptNumber`
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

## Reference Implementations

- **Go (NATS):** `worker/` package
- **HTTP SDKs:** Implement against the three HTTP endpoints and JSON schemas above
- All Go types referenced in this document are canonical -- implement JSON serialization matching these types

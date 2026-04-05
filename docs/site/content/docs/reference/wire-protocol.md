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

The bridge exposes three endpoints for HTTP workers. All requests require Bearer token authentication via `Authorization: Bearer {token}` header. The token is configured via the `DAGNATS_BRIDGE_TOKEN` environment variable.

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
| HTTP bridge | Bearer token via `Authorization: Bearer {token}` header. Token set via `DAGNATS_BRIDGE_TOKEN`. Missing or invalid tokens return `401 Unauthorized`. |
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

## Reference Implementations

- **Go (NATS):** `worker/` package
- **HTTP SDKs:** Implement against the three HTTP endpoints and JSON schemas above
- All Go types referenced in this document are canonical -- implement JSON serialization matching these types

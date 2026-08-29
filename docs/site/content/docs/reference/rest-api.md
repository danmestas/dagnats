---
title: REST API Reference
weight: 2
---

Complete reference for the DagNats control-plane HTTP API.

All endpoints return `application/json` responses. Error responses use plain text with the appropriate HTTP status code.

## Base URL

The API is served by `dagnats serve` at the configured HTTP address (default `:8080`).

---

## Workflows

### List Workflows

Retrieve all registered workflow definitions.

```
GET /workflows
```

**Response:** `200 OK`

```json
[
  {
    "name": "code-review",
    "version": "1.0.0",
    "steps": [...],
    "timeout": "30m",
    "def_hash": "3f1a...c2"
  }
]
```

Each entry includes `def_hash`: the hex-encoded SHA-256 of the server's canonical JSON marshal of that definition (`dag.DefHash`). See [Wire Protocol: Workflow Re-registration and def_hash](../wire-protocol#workflow-re-registration-and-def_hash) for how a caller uses it to skip re-registering an unchanged definition.

**curl:**
```bash
curl http://localhost:8080/workflows
```

### Register Workflow

Register or update a workflow definition.

```
POST /workflows
```

Re-registering an existing name **replaces** the stored definition; in-flight runs pick up the replacement on their next advance rather than staying pinned to the version they started with (see [Wire Protocol: Workflow Re-registration and def_hash](../wire-protocol#workflow-re-registration-and-def_hash)).

**Request body:** A `WorkflowDef` JSON object (see [Workflow Schema](../workflow-schema)).

```json
{
  "name": "code-review",
  "version": "1.0.0",
  "steps": [
    {
      "id": "fetch-diff",
      "task": "git.fetch-diff",
      "timeout": "2m",
      "type": "normal"
    }
  ]
}
```

**Response:** `201 Created`

```json
{
  "status": "registered",
  "name": "code-review",
  "def_hash": "3f1a...c2"
}
```

`def_hash` is `dag.DefHash(def)` for the definition just registered. A caller that re-registers on every trigger can compare its locally computed hash against the last-seen `def_hash` and skip this call when they match.

| Status | Condition |
|--------|-----------|
| `201` | Workflow registered successfully |
| `400` | Invalid JSON or validation failure |

**curl:**
```bash
curl -X POST http://localhost:8080/workflows \
  -H "Content-Type: application/json" \
  -d @workflow.json
```

---

## Runs

### List Runs

Retrieve all workflow runs, optionally filtered by workflow, status, and/or labels. Returns runs sorted by creation time (newest first).

```
GET /runs[?workflow=NAME][&status=STATUS][&label=KEY=VALUE...]
```

| Query Parameter | Description |
|----------------|-------------|
| `workflow` | Filter by workflow name |
| `status` | Filter by run status: `pending`, `running`, `completed`, `failed`, `cancelled`, `compensated`, or `compensate_failed`. An unrecognized value returns `400` listing the accepted set. |
| `label` | Filter by a run label, `key=value`. Repeatable — every `label` param given must match (AND semantics). A param with no `=` returns `400`. |

Filters compose: `workflow`, `status`, and `label` narrow the same query together, not as alternatives. Filtering is applied within a bounded most-recent-runs window server-side, so a filtered result (here and on Count) may miss matches older than that window until the time-ordered index in #453 lands. Cursor-based pagination is not part of this change and is also tracked by #453.

**Response:** `200 OK`

```json
[
  {
    "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
    "workflow_id": "code-review",
    "status": "running",
    "created_at": "2025-01-15T09:00:00Z",
    "labels": {"pr": "42", "repo": "dagnats"},
    "steps": {
      "fetch-diff": {"status": "completed", "attempts": 1},
      "lint": {"status": "running", "attempts": 1}
    }
  }
]
```

**curl:**
```bash
curl http://localhost:8080/runs?workflow=code-review
curl 'http://localhost:8080/runs?status=failed&label=repo=dagnats&label=pr=42'
```

### Start Run

Start a new workflow run, optionally with input data. If `run_at` is provided and is more than 1 second in the future, the run is scheduled for later execution.

```
POST /runs
```

**Request body:**

```json
{
  "workflow": "code-review",
  "input": {"pr": 42},
  "run_at": "2025-01-16T09:00:00Z",
  "labels": {"pr": "42", "repo": "dagnats"}
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `workflow` | string | Yes | Workflow name |
| `input` | JSON | No | Arbitrary input data |
| `run_at` | string | No | RFC3339 time for scheduled execution |
| `labels` | object | No | Key/value metadata stamped on the run (applies to both immediate and scheduled runs). At most 16 labels; keys match `^[a-z0-9_.-]+$` and are at most 64 chars; values at most 256 chars. Use labels to find or bulk-cancel runs later via `GET /runs?label=` or `POST /runs/cancel` without a separate lookup table. |

**Response (immediate):** `201 Created`

```json
{
  "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"
}
```

**Response (scheduled):** `201 Created`

```json
{
  "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
  "status": "scheduled"
}
```

| Status | Condition |
|--------|-----------|
| `201` | Run started or scheduled |
| `400` | Invalid JSON, workflow not found, or input validation failure |

**curl:**
```bash
curl -X POST http://localhost:8080/runs \
  -H "Content-Type: application/json" \
  -d '{"workflow":"code-review","input":{"pr":42}}'
```

### Get Run

Retrieve the current snapshot of a workflow run.

```
GET /runs/{id}
```

**Response:** `200 OK`

```json
{
  "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
  "workflow_id": "code-review",
  "status": "completed",
  "created_at": "2025-01-15T09:00:00Z",
  "steps": {
    "fetch-diff": {
      "status": "completed",
      "attempts": 1,
      "output": {"files": 3}
    }
  }
}
```

| Status | Condition |
|--------|-----------|
| `200` | Run found |
| `400` | Missing run ID |
| `404` | Run not found |

**curl:**
```bash
curl http://localhost:8080/runs/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6
```

### Cancel Run

Cancel a running workflow by publishing a `workflow.cancelled` event.

```
POST /runs/{id}/cancel
```

**Response:** `200 OK`

```json
{
  "status": "cancelled",
  "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"
}
```

| Status | Condition |
|--------|-----------|
| `200` | Cancel event published |
| `400` | Missing run ID |
| `500` | Publish failure |

**curl:**
```bash
curl -X POST http://localhost:8080/runs/a1b2c3d4.../cancel
```

### Send Signal

Write a named signal with arbitrary data to a running workflow via the `signals` KV bucket.

```
POST /runs/{id}/signal/{name}
```

The request body is the signal payload (raw bytes, max 1 MiB).

**Response:** `200 OK`

```json
{
  "status": "sent",
  "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
  "signal": "approval"
}
```

| Status | Condition |
|--------|-----------|
| `200` | Signal written to KV |
| `400` | Missing run ID or signal name |
| `500` | KV write failure |

**curl:**
```bash
curl -X POST http://localhost:8080/runs/a1b2c3d4.../signal/approval \
  -d '{"approved": true}'
```

### Handle Approval

Process an approval or rejection for a human-in-the-loop step. Uses atomic CAS to guarantee exactly-once token consumption.

```
POST /runs/{id}/approval/{step_id}?action=approve&token=TOKEN
POST /runs/{id}/approval/{step_id}?action=reject&token=TOKEN
```

| Query Parameter | Required | Description |
|----------------|----------|-------------|
| `action` | Yes | `approve` or `reject` |
| `token` | Yes | One-time approval token |

**Request body (optional):**

```json
{
  "comment": "LGTM",
  "approved_by": "alice"
}
```

**Response:** `200 OK`

```json
{
  "status": "approved",
  "run_id": "a1b2c3d4...",
  "step": "review"
}
```

| Status | Condition |
|--------|-----------|
| `200` | Approval processed |
| `400` | Missing step ID, invalid action |
| `401` | Invalid token or token not found/expired |
| `409` | Token already consumed |

**curl:**
```bash
curl -X POST \
  "http://localhost:8080/runs/a1b2.../approval/review?action=approve&token=abc123"
```

---

## Scheduled Runs

### Get Scheduled Run

Retrieve a scheduled (pending) run by ID.

```
GET /runs/{id}/scheduled
```

**Response:** `200 OK` with the scheduled run object.

| Status | Condition |
|--------|-----------|
| `200` | Scheduled run found |
| `404` | Not found |

### Cancel Scheduled Run

Cancel a pending scheduled run before it executes.

```
DELETE /runs/{id}/scheduled
```

**Response:** `200 OK`

```json
{
  "status": "cancelled"
}
```

| Status | Condition |
|--------|-----------|
| `200` | Scheduled run cancelled |
| `400` | Run not found or already executed |

---

## Bulk Operations

### Bulk Run

Start multiple workflow runs in a single request. The workflow definition is loaded once and all inputs are validated atomically before any runs start.

```
POST /runs/bulk
```

**Request body:**

```json
{
  "workflow_id": "deploy",
  "inputs": [
    {"env": "staging"},
    {"env": "prod"}
  ],
  "labels": {"batch": "release-42"}
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `workflow_id` | string | Yes | Workflow name |
| `inputs` | []JSON | Yes | Array of input payloads (max 1000) |
| `labels` | object | No | Applied to every run started in this batch (same bounds as Start Run's `labels`) |

**Response:** `201 Created`

```json
{
  "run_ids": [
    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
    "e5f6a7b8c9d0e1f2a3b4c5d6a1b2c3d4"
  ],
  "total": 2
}
```

| Status | Condition |
|--------|-----------|
| `201` | All runs started |
| `400` | Invalid request, workflow not found, or input validation failure |

**curl:**
```bash
curl -X POST http://localhost:8080/runs/bulk \
  -H "Content-Type: application/json" \
  -d '{"workflow_id":"deploy","inputs":[{"env":"staging"},{"env":"prod"}]}'
```

### Bulk Cancel

Cancel multiple runs matching filter criteria.

```
POST /runs/cancel
```

**Request body:**

```json
{
  "workflow_id": "deploy",
  "status": "running",
  "after": "2025-01-15T00:00:00Z",
  "before": "2025-01-16T00:00:00Z",
  "dry_run": false,
  "labels": {"batch": "release-42"}
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `workflow_id` | string | Yes | Workflow name |
| `status` | string | No | `running`, `pending`, or `all` (default: `all`) |
| `after` | string | No | RFC3339 lower bound on creation time |
| `before` | string | No | RFC3339 upper bound on creation time |
| `dry_run` | bool | No | Preview without cancelling |
| `labels` | object | No | Every key/value must match a run's labels (AND semantics), composing with `workflow_id`/`status`/`after`/`before`. Same bounds as Start Run's `labels`; a filter with more than 16 labels is a `400`. |

**Response:** `200 OK`

```json
{
  "cancelled": ["a1b2c3d4...", "e5f6a7b8..."],
  "skipped": [],
  "total": 2,
  "dry_run": false
}
```

| Status | Condition |
|--------|-----------|
| `200` | Cancel operation completed |
| `400` | Invalid request or too many matching runs (max 1000) |

**curl:**
```bash
curl -X POST http://localhost:8080/runs/cancel \
  -H "Content-Type: application/json" \
  -d '{"workflow_id":"deploy","status":"pending","dry_run":true}'
```

### Bulk Retry

Retry failed runs of a workflow. Supports two modes:

- **rerun**: Start fresh runs with the original input
- **replay**: Re-publish DLQ task messages to resume at the failed step

```
POST /runs/retry
```

**Request body:**

```json
{
  "workflow_id": "deploy",
  "mode": "rerun",
  "after": "2025-01-15T00:00:00Z",
  "dry_run": false
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `workflow_id` | string | Yes | Workflow name |
| `mode` | string | Yes | `rerun` or `replay` |
| `after` | string | No | RFC3339 lower bound |
| `before` | string | No | RFC3339 upper bound |
| `dry_run` | bool | No | Preview without retrying |

**Response:** `200 OK`

```json
{
  "retried": [
    {"original_run_id": "a1b2...", "new_run_id": "c3d4..."}
  ],
  "skipped": [],
  "total": 1,
  "dry_run": false
}
```

For `replay` mode, `new_run_id` is omitted since the original run resumes.

| Status | Condition |
|--------|-----------|
| `200` | Retry operation completed |
| `400` | Invalid request, mode, or too many matching runs (max 1000) |

**curl:**
```bash
curl -X POST http://localhost:8080/runs/retry \
  -H "Content-Type: application/json" \
  -d '{"workflow_id":"deploy","mode":"rerun"}'
```

---

## Workers

### List Workers

Retrieve the live worker directory. A worker is considered live if it has
sent a heartbeat within the staleness TTL (60s); entries older than that
are filtered out before the response is built, so every worker returned
here is currently reachable.

```
GET /v1/workers
```

Note the `/v1` prefix — this route is mounted at the top level alongside
the worker-runtime bridge, not under the unprefixed control-plane paths
above.

**Response:** `200 OK`

```json
{
  "workers": [
    {
      "worker_id": "worker-1",
      "task_types": ["git.fetch-diff", "code-review"],
      "language": "go",
      "transport": "nats",
      "max_tasks": 10,
      "pid": 4821,
      "hostname": "build-01",
      "last_seen": "2026-08-28T12:00:00Z"
    }
  ],
  "count": 1
}
```

`workers` is always an array (`[]` when none are registered or live),
never `null`.

**curl:**
```bash
curl http://localhost:8080/v1/workers
```

---

## Queue

### Queue depth

Retrieve a snapshot of pending task counts per task type on the
`TASK_QUEUES` work queue. The source of truth is the stream's own
per-subject state -- `TASK_QUEUES` is a JetStream work-queue stream, so
an unacked message on `task.{taskType}` IS the pending task; there is
no separate counter to drift from it.

```
GET /v1/queue
```

Note the `/v1` prefix, same as `GET /v1/workers` above.

**Response:** `200 OK`

```json
{
  "groups": [
    { "task_type": "build", "pending": 3, "oldest_wait_ms": 1523 },
    { "task_type": "test", "pending": 1, "oldest_wait_ms": 402 }
  ],
  "snapshot_at": "2026-08-28T12:00:00Z"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `task_type` | string | The task type -- the `task.{taskType}` subject with the `task.` prefix stripped. |
| `pending` | integer | Unacked message count for this subject right now. |
| `oldest_wait_ms` | integer, omitted when unavailable | How long the oldest pending message on this subject has been waiting, in milliseconds. Omitted (not zero) when the server could not read the oldest message for this one subject -- a transient per-subject read failure never fails the whole response. |
| `snapshot_at` | RFC3339 timestamp | When this snapshot was taken. |
| `truncated` | boolean, omitted when false | Present and `true` only when the stream carries more distinct task-type subjects than the server's cap (256) -- `groups` is truncated to the first 256 in `task_type` order. |

`groups` is always an array, sorted by `task_type`, `[]` when nothing is
pending -- never `null`.

**Labels are not a grouping dimension here.** Pending tasks on
`TASK_QUEUES` don't carry run labels in their subject or a
cheap-to-read header; grouping by label would mean fetching and
decoding every pending payload, which is unbounded work on a queue
that can hold an unbounded number of pending tasks. `task_type`
grouping is free (it's the subject); label grouping is not, so it is
out of scope for this endpoint.

For a live feed instead of point-in-time polling, subscribe to
`event.queue.snapshot` on the `EVENTS` stream -- see
[Consumer contract: run lifecycle events](/docs/reference/wire-protocol#consumer-contract-run-lifecycle-events)
for its cadence and change-suppression behavior; the payload shape is
identical to this response.

**curl:**
```bash
curl http://localhost:8080/v1/queue
```

---

## Tokens

Mint, list, and revoke bearer tokens that scope HTTP-bridge worker access
to specific task-type prefixes. Separate from `DAGNATS_BRIDGE_TOKEN`,
which remains the single admin/root credential: it authenticates
unscoped, and it is the only credential these three routes accept.

**All three routes require `Authorization: Bearer <DAGNATS_BRIDGE_TOKEN>`.**
This is a fail-closed contract: if `DAGNATS_BRIDGE_TOKEN` is unset,
token management is unavailable (`503`), never open. A minted worker
token cannot call these routes, no matter its scope.

### Mint Token

```
POST /v1/tokens
```

**Request body:**

```json
{
  "label": "ci-runner-1",
  "task_type_prefixes": ["ci.", "build."]
}
```

An empty (or omitted) `task_type_prefixes` mints a token that is
authorized for **no** task types — fail closed, not "all types." Bounds:
label up to 128 bytes, up to 32 prefixes of up to 64 bytes each, and up
to 1000 non-revoked tokens outstanding at once.

**Response:** `201 Created`

```json
{
  "id": "6f1c...9a2b",
  "token": "dgn_6f1c...9a2b_kQ3z...",
  "label": "ci-runner-1",
  "task_type_prefixes": ["ci.", "build."],
  "created_at": "2026-08-28T12:00:00Z"
}
```

`token` (the bearer a worker presents as
`Authorization: Bearer dgn_{id}_{secret}`) is shown **exactly once**,
here. It is not recoverable from `GET /v1/tokens` or anywhere else — the
server stores only its SHA-256 hash. Save it immediately or mint a new
one.

**curl:**
```bash
curl -X POST http://localhost:8080/v1/tokens \
  -H "Authorization: Bearer $DAGNATS_BRIDGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"label":"ci-runner-1","task_type_prefixes":["ci."]}'
```

### List Tokens

```
GET /v1/tokens
```

**Response:** `200 OK`

```json
{
  "tokens": [
    {
      "id": "6f1c...9a2b",
      "label": "ci-runner-1",
      "task_type_prefixes": ["ci.", "build."],
      "created_at": "2026-08-28T12:00:00Z",
      "created_by": "admin",
      "revoked_at": null
    }
  ]
}
```

Revoked tokens are kept in the listing for audit (`revoked_at` set) —
they are not deleted. The response never includes the secret or its
hash.

**curl:**
```bash
curl http://localhost:8080/v1/tokens \
  -H "Authorization: Bearer $DAGNATS_BRIDGE_TOKEN"
```

### Revoke Token

```
DELETE /v1/tokens/{id}
```

**Response:** `204 No Content` on success, `404 Not Found` if `id` is
unknown.

Revocation latency is bounded by the bridge's KV-watch reconnect window
(capped at 30s), not instant: each bridge process caches minted tokens
in memory and only re-syncs immediately on a live watch; during a NATS
reconnect it keeps serving its last-known cache rather than failing
every poll/resolve outright, so a revoke made during that window takes
effect once the watch reconnects.

**curl:**
```bash
curl -X DELETE http://localhost:8080/v1/tokens/6f1c...9a2b \
  -H "Authorization: Bearer $DAGNATS_BRIDGE_TOKEN"
```

---

## Health

### Telemetry Health

Check service health and telemetry stream status. The health endpoint never returns unhealthy; telemetry information is advisory.

```
GET /health/telemetry
```

**Response:** `200 OK`

```json
{
  "status": "healthy",
  "telemetry": {
    "stream": {
      "messages": 15432,
      "bytes": 2048576,
      "percent": 12.5
    }
  }
}
```

The `percent` field shows telemetry stream storage usage as a percentage of `MaxBytes`.

**curl:**
```bash
curl http://localhost:8080/health/telemetry
```

---

## Error Responses

Errors are returned as plain text with the appropriate HTTP status code:

```
HTTP/1.1 400 Bad Request

invalid workflow: step "x" depends on non-existent step "y"
```

| Status | Meaning |
|--------|---------|
| `400` | Bad request (invalid JSON, validation error, missing fields) |
| `401` | Unauthorized (invalid approval token) |
| `404` | Resource not found |
| `405` | Method not allowed |
| `409` | Conflict (approval token already consumed) |
| `500` | Internal server error |

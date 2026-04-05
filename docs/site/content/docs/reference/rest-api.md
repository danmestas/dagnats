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
    "timeout": "30m"
  }
]
```

**curl:**
```bash
curl http://localhost:8080/workflows
```

### Register Workflow

Register or update a workflow definition.

```
POST /workflows
```

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
  "name": "code-review"
}
```

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

Retrieve all workflow runs, optionally filtered by workflow. Returns runs sorted by creation time (newest first).

```
GET /runs[?workflow=NAME]
```

| Query Parameter | Description |
|----------------|-------------|
| `workflow` | Filter by workflow name |

**Response:** `200 OK`

```json
[
  {
    "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
    "workflow_id": "code-review",
    "status": "running",
    "created_at": "2025-01-15T09:00:00Z",
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
  "run_at": "2025-01-16T09:00:00Z"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `workflow` | string | Yes | Workflow name |
| `input` | JSON | No | Arbitrary input data |
| `run_at` | string | No | RFC3339 time for scheduled execution |

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
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `workflow_id` | string | Yes | Workflow name |
| `inputs` | []JSON | Yes | Array of input payloads (max 1000) |

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
  "dry_run": false
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `workflow_id` | string | Yes | Workflow name |
| `status` | string | No | `running`, `pending`, or `all` (default: `all`) |
| `after` | string | No | RFC3339 lower bound on creation time |
| `before` | string | No | RFC3339 upper bound on creation time |
| `dry_run` | bool | No | Preview without cancelling |

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

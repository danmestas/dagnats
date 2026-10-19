---
title: CLI and API
weight: 1
---

Besides the trigger types, a run can start from an explicit call: the CLI's `run start` command or the control-plane REST API's `POST /runs` endpoint.

## Starting a Run via CLI

The `dagnats run start` command creates a new run of a registered workflow and returns the run ID immediately. The input is an optional positional argument, a JSON string:

```bash
dagnats run start code-review-pipeline '{"repo": "acme/api", "pr": 42}'
```

The input becomes the run's `Input` payload. Steps without dependencies receive it directly. If the workflow declares an `input_schema`, the input is validated against it first.

### Watching a Run

Add `--watch` to stream run events to the terminal until the run reaches a terminal state:

```bash
dagnats run start code-review-pipeline '{"repo": "acme/api", "pr": 42}' --watch
```

`--output` implies `--watch` and prints the run's final output when it finishes. You can also attach later with `dagnats run watch <run-id>`.

### Scheduling a Run

`--at` takes an RFC3339 time and schedules the run for then, instead of starting it now:

```bash
dagnats run start code-review-pipeline '{"pr": 42}' --at=2026-10-20T09:00:00Z
```

### Bulk Runs

Start up to 1000 runs of the same workflow in one call with `dagnats run bulk`. The workflow is a flag:

```bash
dagnats run bulk --workflow code-review-pipeline --from-file inputs.jsonl
```

Each line of the JSONL file is one run's input. You can also pass inputs as positional arguments, or both:

```bash
dagnats run bulk --workflow code-review-pipeline \
  '{"pr": 1}' '{"pr": 2}' '{"pr": 3}'
```

When the workflow has an `input_schema`, every input is validated before any run starts, so one invalid input fails the whole batch. An empty batch, or one over 1000 inputs, is rejected. If publishing fails partway, the runs already published stay started.

## Starting a Run via REST API

`dagnats serve` serves the control-plane REST API on its HTTP listener (`http_addr`, default `127.0.0.1:8080`). The routes have no version prefix. The `/v1/` paths belong to the worker HTTP bridge.

```bash
curl -X POST http://localhost:8080/runs \
  -H "Content-Type: application/json" \
  -d '{
    "workflow": "code-review-pipeline",
    "input": {"repo": "acme/api", "pr": 42}
  }'
```

The response is `201` with the new run ID:

```json
{"run_id": "abc123"}
```

The body also accepts `labels` (a string map copied onto the run) and `run_at` (an RFC3339 time). A `run_at` more than one second in the future schedules the run.

### Bulk API

`POST /runs/bulk` takes the workflow in `workflow_id` and an array of inputs, with the same validation as the CLI:

```json
{
  "workflow_id": "code-review-pipeline",
  "inputs": [
    {"repo": "acme/api", "pr": 1},
    {"repo": "acme/api", "pr": 2}
  ]
}
```

The response lists the started runs: `{"run_ids": [...], "total": 2}`.

## Run Lifecycle

Once started, a run transitions through these states:

| Status | Meaning |
|--------|---------|
| `pending` | Created, not yet claimed by the engine |
| `running` | Engine is actively processing steps |
| `completed` | All steps finished successfully |
| `failed` | One or more steps failed permanently |
| `cancelled` | Cancelled via CLI or API |
| `compensated` | The run failed and saga compensation finished |
| `compensate_failed` | The run failed and compensation also failed |

Check run status at any time with:

```bash
dagnats run status <run-id>
dagnats run status <run-id> --json
```

Run IDs accept any unique prefix of 8 or more characters.

## Related Pages

- [Cron Schedules](/docs/triggers/cron-schedules) -- automated recurring runs
- [Event Triggers](/docs/triggers/event-triggers) -- start runs from NATS events
- [Webhooks](/docs/triggers/webhooks) -- fire-and-forget HTTP triggers
- [HTTP Trigger + Respond Step](/docs/triggers/http) -- synchronous HTTP request/response endpoints
- [Retry Policies](/docs/reliability/retry-policies) -- handling transient failures
- [Cancellation](/docs/reliability/cancellation) -- stopping a running workflow

---
title: CLI and API
weight: 1
---

Every workflow run starts with an explicit trigger -- the CLI's `run start` command or the REST API's `POST /v1/runs` endpoint.

## Starting a Run via CLI

The `dagnats run start` command creates a new run of a registered workflow and returns the run ID immediately.

```bash
dagnats run start code-review-pipeline \
  --input '{"repo": "acme/api", "pr": 42}'
```

The `--input` flag accepts a JSON string that becomes the run's `Input` payload. Steps without dependencies receive this payload directly.

### Watching a Run

Add `--watch` to stream run events to the terminal until the run reaches a terminal state:

```bash
dagnats run start code-review-pipeline \
  --input '{"repo": "acme/api", "pr": 42}' \
  --watch
```

The watch stream prints step status transitions as they occur. Press `Ctrl+C` to detach without cancelling the run.

### Bulk Runs

Start up to 1000 runs of the same workflow in a single call using `dagnats run bulk`:

```bash
dagnats run bulk code-review-pipeline \
  --from-file inputs.jsonl
```

Each line of the JSONL file is one run's input. You can also pass inputs as positional arguments:

```bash
dagnats run bulk code-review-pipeline \
  '{"pr": 1}' '{"pr": 2}' '{"pr": 3}'
```

Validation is **atomic** -- the first invalid input fails the entire batch before any runs are created.

## Starting a Run via REST API

The control plane exposes a REST endpoint for programmatic run creation.

```bash
curl -X POST http://localhost:8080/v1/runs \
  -H "Content-Type: application/json" \
  -d '{
    "workflow": "code-review-pipeline",
    "input": {"repo": "acme/api", "pr": 42}
  }'
```

The response includes the new run ID:

```json
{"run_id": "abc123"}
```

### Bulk API

`POST /v1/runs/bulk` accepts an array of inputs for the same workflow:

```json
{
  "workflow": "code-review-pipeline",
  "inputs": [
    {"repo": "acme/api", "pr": 1},
    {"repo": "acme/api", "pr": 2}
  ]
}
```

Same atomic validation semantics as the CLI -- all or nothing.

## Run Lifecycle

Once started, a run transitions through these states:

| Status | Meaning |
|--------|---------|
| `pending` | Created, not yet claimed by the engine |
| `running` | Engine is actively processing steps |
| `completed` | All steps finished successfully |
| `failed` | One or more steps failed permanently |
| `cancelled` | Cancelled via CLI or API |

Check run status at any time with:

```bash
dagnats run get <run-id>
dagnats run get <run-id> --json
```

## Related Pages

- [Cron Schedules](/docs/triggers/cron-schedules) -- automated recurring runs
- [Event Triggers](/docs/triggers/event-triggers) -- start runs from NATS events
- [Webhooks](/docs/triggers/webhooks) -- fire-and-forget HTTP triggers (202 immediately)
- [HTTP Trigger + Respond Step](/docs/triggers/http) -- synchronous HTTP request/response endpoints
- [Retry Policies](/docs/reliability/retry-policies) -- handling transient failures
- [Cancellation](/docs/reliability/cancellation) -- stopping a running workflow

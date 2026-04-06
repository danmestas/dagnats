# HTTP Bridge: curl Walkthrough

Step-by-step walkthrough of the DagNats HTTP bridge protocol using only curl. Shows the full worker lifecycle: connect, poll, complete, and fail.

All examples assume the bridge is running at `http://localhost:8080` with no auth token set. If auth is enabled, add `-H "Authorization: Bearer <token>"` to every command.

## 1. Connect (SSE heartbeat stream)

Open a persistent SSE connection. This registers the worker and starts receiving heartbeats. Run this in a separate terminal -- it stays open.

```bash
curl -N -X POST http://localhost:8080/v1/workers/connect \
  -H "Content-Type: application/json" \
  -d '{
    "worker_id": "curl-worker-1",
    "task_types": ["uppercase"],
    "max_tasks": 1
  }'
```

Expected output (repeats every 25 seconds):

```
event: heartbeat
data: ok
```

Leave this running. The bridge deregisters the worker when the connection closes.

## 2. Poll for tasks

In another terminal, long-poll for tasks. This blocks until a task is available or the timeout expires.

```bash
curl -s -X POST http://localhost:8080/v1/tasks/poll \
  -H "Content-Type: application/json" \
  -d '{
    "task_types": ["uppercase"],
    "max_tasks": 1,
    "timeout_ms": 30000
  }'
```

Empty response (no tasks available):

```json
[]
```

When a task is available:

```json
[
  {
    "task_id": "abc123.uppercase",
    "run_id": "abc123",
    "step_id": "uppercase",
    "iteration": 0,
    "attempt": 0,
    "input": "Hello, Alice!"
  }
]
```

Note the `task_id` -- you need it for the resolve step.

## 3. Complete a task

After processing the task, resolve it as complete with the output:

```bash
curl -s -X POST http://localhost:8080/v1/tasks/abc123.uppercase/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "action": "complete",
    "output": "HELLO, ALICE!"
  }'
```

Returns HTTP 200 with no body on success.

## 4. Fail a task

To report a permanent failure:

```bash
curl -s -X POST http://localhost:8080/v1/tasks/abc123.uppercase/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "action": "fail",
    "error": "something went wrong",
    "failure_type": "permanent"
  }'
```

To report a retriable failure with a retry delay:

```bash
curl -s -X POST http://localhost:8080/v1/tasks/abc123.uppercase/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "action": "fail",
    "error": "rate limited",
    "failure_type": "retry_after",
    "retry_after_ms": 5000
  }'
```

## 5. Checkpoint (save progress)

For long-running tasks, save intermediate state without completing:

```bash
curl -s -X POST http://localhost:8080/v1/tasks/abc123.uppercase/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "action": "checkpoint",
    "data": {"processed_count": 42, "cursor": "page-3"}
  }'
```

Returns HTTP 200. The task remains in-flight with its ack deadline extended.

## Full lifecycle script

Combine the above into a single script (excluding the SSE connect):

```bash
#!/usr/bin/env bash
set -euo pipefail

BRIDGE="http://localhost:8080"

echo "Polling for tasks..."
TASKS=$(curl -s -X POST "$BRIDGE/v1/tasks/poll" \
  -H "Content-Type: application/json" \
  -d '{"task_types": ["uppercase"], "max_tasks": 1, "timeout_ms": 30000}')

echo "Got: $TASKS"

TASK_ID=$(echo "$TASKS" | python3 -c "import sys,json; t=json.load(sys.stdin); print(t[0]['task_id'] if t else '')")

if [ -z "$TASK_ID" ]; then
  echo "No tasks available"
  exit 0
fi

INPUT=$(echo "$TASKS" | python3 -c "import sys,json; t=json.load(sys.stdin); print(json.dumps(t[0].get('input','')))")
OUTPUT=$(echo "$INPUT" | python3 -c "import sys; print(sys.stdin.read().strip().upper())")

echo "Completing task $TASK_ID with output: $OUTPUT"
curl -s -X POST "$BRIDGE/v1/tasks/$TASK_ID/resolve" \
  -H "Content-Type: application/json" \
  -d "{\"action\": \"complete\", \"output\": $OUTPUT}"

echo "Done"
```

## Resolve actions reference

| Action | Required fields | Description |
|---|---|---|
| `complete` | `output` | Task succeeded |
| `fail` | `error` | Task failed (`failure_type`: `permanent`, `retriable`, `retry_after`) |
| `pause` | `duration_ms` | NAK with delay; optional `checkpoint` |
| `checkpoint` | `data` | Save state, extend ack deadline |
| `send_signal` | `run_id`, `name`, `data` | Write signal to KV |
| `wait_signal` | `name`, `timeout_ms` | Block until signal arrives |

# Python HTTP Worker Example

A Python worker that connects to the DagNats HTTP bridge and processes "uppercase" tasks. Demonstrates the full worker lifecycle using the bridge REST API.

## Prerequisites

- DagNats server running (`dagnats serve`)
- HTTP bridge enabled (default port 8080)
- Python 3.8+

## Run It

Terminal 1 -- start the server:

```bash
dagnats serve
```

Terminal 2 -- start the Python worker:

```bash
cd examples/http-worker-python
pip install -r requirements.txt
python worker.py
```

Terminal 3 -- register and run the workflow:

```bash
dagnats workflow register examples/hello-world/workflow.json
dagnats run start hello-world '"Alice"'
```

The Python worker picks up the `uppercase` step and processes it over HTTP.

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `DAGNATS_BRIDGE_URL` | `http://localhost:8080` | Bridge base URL |
| `DAGNATS_WORKER_ID` | `python-worker-1` | Worker registration ID |
| `DAGNATS_BRIDGE_TOKEN` | (empty) | Bearer token for auth |

## How It Works

1. **Connect** -- POST `/v1/workers/connect` opens an SSE stream. The bridge sends heartbeats every 25s. A background thread drains these to keep the connection alive.
2. **Poll** -- POST `/v1/tasks/poll` long-polls for tasks matching `["uppercase"]`. Returns a JSON array of tasks or an empty array on timeout.
3. **Resolve** -- POST `/v1/tasks/{id}/resolve` completes or fails the task. The `action` field determines behavior (`complete`, `fail`, `pause`, `checkpoint`).
4. **Reconnect** -- On disconnect, the worker retries with exponential backoff (5s, 10s, 20s... up to 60s).

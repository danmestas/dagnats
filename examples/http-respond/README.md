# HTTP Trigger + Respond Step Example

Two primitives from ADR-013 in one minimal workflow:

- An **HTTP trigger** at `POST /api/echo` that turns inbound requests into workflow runs.
- A **respond step** that ships the workflow's output back as the HTTP response.

The DAG: `echo` (normal step, runs the worker) → `respond` (engine-resolved step, publishes the HTTP response).

## Workflow

- `echo` -- worker step. Receives the HTTP request envelope (`method`, `path`, `headers`, `body`), returns a JSON object describing what arrived.
- `respond` -- ships the upstream output as the HTTP response (status 200, `application/json`).

## Run It

Terminal 1 -- start the server:

```bash
dagnats serve
```

Terminal 2 -- start the worker:

```bash
go run ./examples/http-respond/
```

Terminal 3 -- register the workflow + trigger and curl it:

```bash
dagnats workflow register examples/http-respond/workflow.json
# {"status":"registered","name":"http-echo"}

curl -sS -X POST http://localhost:8080/api/echo \
  -H 'Content-Type: application/json' \
  -d '{"name":"alice"}'
# {"you_sent":"the dagnats http trigger","method":"POST","path":"/api/echo","body":{"name":"alice"}}
```

The response header `X-Dagnats-Run-Id` carries the run id; `dagnats run inspect <id>` walks the DAG.

## Idempotency replay

Add `IdempotencyHeader` to the trigger config and supply that header on requests:

```json
"http": {
  "path": "/api/echo",
  "method": "POST",
  "timeout_ms": 5000,
  "max_body_bytes": 1048576,
  "idempotency_header": "Idempotency-Key"
}
```

```bash
curl -sS -X POST http://localhost:8080/api/echo \
  -H 'Idempotency-Key: my-request-1' \
  -d '{"name":"alice"}'
# Re-issuing within 1h with the same Idempotency-Key replays the
# original response without re-running the workflow.
```

## Failure modes

| Condition                                | HTTP outcome                                                |
| ---------------------------------------- | ----------------------------------------------------------- |
| Worker returns error → engine fails run  | `500` with `{"error":"workflow_failed","run_id":"..."}`     |
| Run cancelled via `dagnats run cancel`   | `503` with `{"error":"workflow_cancelled","run_id":"..."}`  |
| Client disconnects before response       | `499` with `{"error":"client_closed","run_id":"..."}`       |
| Per-request timeout elapses              | `504` with `{"error":"workflow_timeout","run_id":"..."}`    |
| Workflow ends without hitting `respond`  | `504` (same as timeout — there's no other signal)           |

The last case is the foot-gun the workflow validator warns about at registration time. If you register a workflow with an HTTP trigger but no reachable `respond` step, `POST /workflows` returns 201 with a `warnings` array including `{"kind":"missing_respond"}`.

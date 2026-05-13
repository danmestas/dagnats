---
title: HTTP Trigger + Respond Step
weight: 5
---

The HTTP trigger turns a DagNats workflow into a synchronous HTTP endpoint. The caller waits for the workflow's response. The **respond step** is the explicit node in the DAG that publishes that response — analogous to a `return` statement, but a node because DAGs have no single return point.

This pair (ADR-013) is distinct from [webhooks](/docs/triggers/webhooks), which are fire-and-forget; webhook callers get a 202 immediately and never see the workflow's output.

## Mental model: `respond` is a side effect, not a return

```
http trigger → [step A] → [step B] → respond → [step C] → [step D]
                                      │
                                      └─ HTTP response dispatched here
                                         (client connection released)
```

`[step C]` and `[step D]` run **after** the HTTP client has already received its response. Their outputs are not visible to the caller. This is desirable for cleanup, audit logging, or fanning out follow-up workflows.

**Anti-pattern:** placing an auth-revocation, billing-charge, or any "must-complete-before-the-user-sees-success" operation *after* `respond`. The user has already seen success; the late step can fail silently. Put such steps **before** `respond`, or split into a separate workflow keyed off the response event.

## Defining an HTTP trigger

Triggers ship inline with the workflow JSON; the dagnats CLI registers both in one call:

```json
{
  "name": "http-echo",
  "version": "1.0",
  "steps": [
    { "id": "echo", "task": "echo", "depends_on": [] },
    {
      "id": "respond",
      "type": "respond",
      "depends_on": ["echo"],
      "config": { "status": 200, "content_type": "application/json" }
    }
  ],
  "triggers": [
    {
      "id": "http-echo-trigger",
      "workflow_id": "http-echo",
      "enabled": true,
      "http": {
        "path": "/api/echo",
        "method": "POST",
        "timeout_ms": 5000,
        "max_body_bytes": 1048576
      }
    }
  ]
}
```

Configuration fields:

| Field                | Required | Default | Notes                                                                 |
| -------------------- | -------- | ------- | --------------------------------------------------------------------- |
| `path`               | yes      | —       | Exact match, must start with `/`. No wildcards in v1.                 |
| `method`             | yes      | —       | One of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`.                       |
| `timeout_ms`         | yes      | —       | Hard cap on the request; 504 if elapsed.                              |
| `max_body_bytes`     | yes      | —       | 413 if exceeded.                                                      |
| `secret`             | no       | —       | HMAC-SHA256 shared secret; signature read from `X-Signature-256`.     |
| `idempotency_header` | no       | —       | If set, header value → run replay (see below).                        |

Routes mount under `/api/` on the same HTTP listener as the control plane. Two HTTP triggers may not share the same `(method, path)` — registration of a colliding trigger returns a `route_conflict` error with the holder trigger's id.

## Defining the respond step

```json
{
  "id": "respond",
  "type": "respond",
  "depends_on": ["upstream-step"],
  "config": {
    "status": 200,
    "content_type": "application/json",
    "headers": { "X-Custom-Header": "value" },
    "body_from": "result.value"
  }
}
```

Configuration fields:

| Field          | Default              | Meaning                                                                   |
| -------------- | -------------------- | ------------------------------------------------------------------------- |
| `status`       | `200`                | HTTP status code.                                                         |
| `content_type` | `application/json`   | `Content-Type` header.                                                    |
| `headers`      | `null`               | Extra response headers.                                                   |
| `body_from`    | `""` (upstream)      | Empty: use the upstream step's output. Dotpath like `result.value`: pluck. |

## Response always carries `X-Dagnats-Run-Id`

Every HTTP response includes `X-Dagnats-Run-Id` with the run id. Use it with `dagnats run inspect <id>` to walk the DAG that produced the response — including any steps that ran *after* `respond` (which the client never sees).

## Failure modes

| Condition                                | HTTP outcome                                                |
| ---------------------------------------- | ----------------------------------------------------------- |
| Worker returns error → engine fails run  | `500` with `{"error":"workflow_failed","run_id":"..."}`     |
| Run cancelled via `dagnats run cancel`   | `503` with `{"error":"workflow_cancelled","run_id":"..."}`  |
| Client disconnects before response       | `499` with `{"error":"client_closed","run_id":"..."}`       |
| Per-request timeout elapses              | `504` with `{"error":"workflow_timeout","run_id":"..."}`    |
| Workflow ends without hitting `respond`  | `504` (same as timeout — there's no other signal)           |

The last case is the foot-gun the workflow validator warns about at registration time. If you register a workflow with an HTTP trigger but no reachable `respond` step, `POST /workflows` returns 201 with a `warnings` array:

```json
{
  "status": "registered",
  "name": "http-echo",
  "warnings": [
    { "kind": "missing_respond", "message": "..." }
  ]
}
```

The other warning is `duplicate_respond` — two respond steps simultaneously reachable on the same run. Mutually-exclusive branches (happy-path + error-path each with their own `respond`) are not warned about.

Warnings are surfaced; they do not block registration.

## Idempotency replay

Setting `idempotency_header` (e.g. `Idempotency-Key`) opts the trigger into replay semantics: when two requests carry the same header value, the second request is bound to the original run's response. The mapping `(trigger_id, header_value) → run_id` is held in a JetStream KV with a 1-hour TTL.

This is true replay — not just NATS dedup. The second request receives the **same response body** as the first, even after the first run has fully completed and the response subject has gone idle.

## Compared to webhooks

| Capability               | HTTP trigger     | [Webhook](/docs/triggers/webhooks) |
| ------------------------ | ---------------- | ---------------------------------- |
| Caller waits for output  | yes              | no — 202 immediately               |
| Response from workflow   | `respond` step   | none                               |
| Path                     | configurable     | `/hooks/{name}`                    |
| HMAC validation          | optional         | optional                           |
| Idempotency replay       | yes (`Idempotency-Key`) | no                          |

Use webhooks for fire-and-forget ingestion (GitHub events, Stripe events, batch kicks). Use HTTP triggers when you need the workflow's result on the wire.

## Example

See [examples/http-respond/](https://github.com/danmestas/dagnats/tree/main/examples/http-respond) for a runnable workflow + worker pair.

## Related Pages

- [Webhooks](/docs/triggers/webhooks) -- fire-and-forget HTTP ingestion
- [CLI and API](/docs/triggers/cli-and-api) -- manual run creation

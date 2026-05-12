# ADR-013: HTTP Trigger + Respond Step (Durable Endpoints)

**Status:** Proposed
**Deciders:** Dan Mestas
**Depends on:** ADR-011 (engine sole retry authority), ADR-012 (engine resolves WorkflowDef)
**Related:** Inngest Durable Endpoints (beta, Nov 2025); iii `http` trigger + middleware skills

## Context

dagnats today exposes two trigger surfaces that are explicitly **fire-and-forget**:

- `internal/trigger/cron.go` — cron schedules
- `internal/trigger/webhook.go` — HTTP webhooks at `POST /hooks/{path}` (control plane)
- `internal/trigger/subject.go` (`SubjectConfig`) — NATS subject subscriptions

All three publish a `TriggerEnvelope` (`internal/trigger/types.go:53-61`) to the
history stream and return immediately. The HTTP caller of `/hooks/{path}` gets a
202-style "accepted" with no path to the workflow's output.

This is correct for the events for which we built it (GitHub webhooks, IoT pings,
batch kicks). It is wrong for the use case users keep asking for:

> *"I want to expose my dagnats workflow as a normal HTTP API endpoint that
> returns the workflow's result."*

Reference frameworks that solve this:

- **iii** (`/Users/dmestas/references/iii/skills/iii-http-endpoints/`) — engine
  owns the listener, registered functions bind to routes via a `http` trigger
  type with `{api_path, http_method}`. The function's return value is the HTTP
  response.
- **Inngest** ([durable-endpoints](https://www.inngest.com/docs/learn/durable-endpoints))
  — wrap a Next.js/Bun handler with `inngest.endpoint(async (req) => { ... })`;
  inside, `step.run` blocks gain durability; the function's `return` becomes
  the HTTP response. **Still in beta** with notable limitations: no POST body
  (query strings only), no flow control. The fact that Inngest punted POST body
  signals the correlation problem is real but not architecturally blocking.

Both treat the endpoint's response as **implicit** (return value of the
function). For a DAG-shaped engine like dagnats, the response point needs to
be **explicit** — a node in the graph — because there is no single "return
statement" in a workflow.

## Decision (proposed)

Add two primitives. Neither requires new infrastructure — both compose with the
existing trigger system, history stream, and JetStream KV.

### 1. `http` trigger kind

Extend `TriggerDef` (`internal/trigger/types.go:11-21`) with a fifth one-of
variant:

```go
type TriggerDef struct {
    ID         string         `json:"id"`
    WorkflowID string         `json:"workflow_id"`
    Enabled    bool           `json:"enabled"`
    Cron       *CronConfig    `json:"cron,omitempty"`
    Subject    *SubjectConfig `json:"subject,omitempty"`
    Webhook    *WebhookConfig `json:"webhook,omitempty"`
    HTTP       *HTTPConfig    `json:"http,omitempty"`  // new
    Debounce   *DebounceConfig `json:"debounce,omitempty"`
}

type HTTPConfig struct {
    Path            string        `json:"path"`             // exact match, e.g. "/api/orders"
    Method          string        `json:"method"`           // GET|POST|PUT|PATCH|DELETE
    TimeoutMs       int           `json:"timeout_ms"`       // hard cap; default 30_000
    MaxBodyBytes    int64         `json:"max_body_bytes"`   // default 1 MiB (matches webhook.go)
}
```

Routes register on the same `cmd/dagnats-api` mux that hosts `/hooks/{path}` and
the control plane (`internal/api/rest.go`). The handler:

1. Reads and bounds the body (programmer-error assertion: `MaxBodyBytes > 0`).
2. Generates a `run_id`.
3. Subscribes (sync, with timeout) to `dagnats.http.response.<run_id>`
   **before** publishing the workflow.started event — eliminates a race where
   a fast workflow could respond before the API server subscribes.
4. Publishes a `TriggerEnvelope` whose `Data` is the HTTP request envelope
   (method, headers, query, body — same shape as the webhook envelope, plus
   `request_id` for tracing).
5. Selects on: the response subscription, `run.failed`/`run.cancelled` events
   filtered by `run_id`, and the per-request timeout.
6. Writes the resulting status/headers/body to the `http.ResponseWriter`.

### 2. `respond` step type

Extend `StepType` (`dag/types.go:14-24`):

```go
const (
    StepTypeNormal StepType = iota
    // ... existing kinds ...
    StepTypePlanner
    StepTypeRespond  // new
)
```

A `respond` step is **terminal** for the response path. It takes the prior
step's output (or a `body_from` step-ref) and publishes a single message to
`dagnats.http.response.<run_id>` with:

```go
type RespondConfig struct {
    Status      int               `json:"status"`        // default 200
    Headers     map[string]string `json:"headers,omitempty"`
    BodyFrom    string            `json:"body_from,omitempty"` // dotpath, default = upstream step output
    ContentType string            `json:"content_type,omitempty"` // default application/json
}
```

The DAG can still continue past a `respond` step — the response is **sent**
when the step executes; subsequent steps (logging, notifications, cleanup)
proceed normally and do not affect the already-sent HTTP response. This is a
deliberate choice: respond is a side effect, not a return statement.

### Correlation — uses NATS, no new state

Single-process (`dagnats serve`) and distributed topologies use the **same**
mechanism: a per-run NATS subject `dagnats.http.response.<run_id>`. The API
server that received the request is the only process subscribed. The `respond`
step publishes once. NATS routes correctly with no shared in-memory map.

This is preferred over an in-memory `runID → ResponseWriter` map because:

- Same code works in distributed leaf-node topologies (Production guide
  `docs/production.md`).
- Survives `dagnats-api` horizontal scaling without sticky sessions.
- Aligns with the project's NATS-native pattern rule (CLAUDE.md → "NATS-Native
  Patterns").

### Failure handling

The API server's select must observe three async sources:

| Event                                     | HTTP outcome                                         |
| ----------------------------------------- | ---------------------------------------------------- |
| `dagnats.http.response.<run_id>` arrives  | Write the status/headers/body verbatim               |
| `run.failed` event for run_id             | 500 with structured error JSON (configurable)        |
| `run.cancelled` event for run_id          | 499 (client-closed) or 503 (engine-cancelled)        |
| Per-request timeout elapses               | 504 with `{ "error": "workflow_timeout", "run_id" }` |
| Workflow reaches end without `respond`    | 504 (same as timeout — there is no other signal)     |

The 504-on-no-respond case is the foot-gun the validation step below mitigates.

### Workflow validation at registration

When `POST /workflows` accepts a definition that includes an `http` trigger,
the API service walks the DAG and warns (non-fatal) if any reachable terminal
step path does not include a `respond` step. Fatal rejection is too strict
(branches can legitimately complete without responding once one branch already
responded), but silent acceptance breeds production hangs.

Validation lives in `dag/` (pure logic, unit-testable, no NATS).

## Why a step, not a return value

iii and Inngest both bind the response to a function's return. Our DAG model
has no single return point. Three options were considered:

1. **Return value of the last step** — fragile; "last" is graph-dependent and
   parallel branches break it.
2. **A `response_step` field on the trigger** — couples trigger and step;
   forces unique step ids per trigger; can't compose with map/dynamic steps.
3. **A first-class `respond` step type** *(chosen)* — workflow author marks
   the response point explicitly; the engine treats it as any other step
   (retries, history, observability all free); multiple branches can each
   have their own `respond` with first-write-wins semantics enforced at the
   subject (NATS delivers to one subscriber; second publish silently drops or
   logs).

## Why not...

### ...build full routing/middleware/content-negotiation (iii's surface)

Anti-goal. dagnats stays a workflow engine. Middleware composes as upstream
DAG steps. Path params/wildcards out of scope for v1 (exact match only).
Content negotiation: the step before `respond` produces bytes; the workflow
author chooses the representation.

### ...reuse `/hooks/{path}`

The webhook path is fire-and-forget by contract — callers don't wait. Mixing
sync and async response semantics on the same endpoint family is more
confusing than adding a new trigger kind.

### ...skip POST body

This is Inngest's headline limitation. We have no architectural reason to
skip it — the body becomes `TriggerEnvelope.Data`, same as webhook bodies
already do today. Bounded by `MaxBodyBytes` (default 1 MiB, matches
`internal/trigger/webhook.go`). Ship v1 **with** POST body.

## Implementation plan (estimate)

| Component                                              | LOC est. | Risk    |
| ------------------------------------------------------ | -------- | ------- |
| `internal/trigger`: `HTTPConfig`, validation, types    | ~120     | low     |
| `internal/trigger/http.go`: HTTPHandler (mirrors webhook.go) | ~180     | medium  |
| `internal/api`: route registration from trigger KV     | ~100     | medium  |
| `dag`: `StepTypeRespond`, `RespondConfig`, validation  | ~80      | low     |
| `engine`: respond step execution + publish path        | ~100     | low     |
| `dag`: reachability check for `respond` from `http` trigger | ~80   | low     |
| Tests: trigger fire→workflow→respond happy path        | ~200     | low     |
| Tests: timeout, run-failure, run-cancellation, no-respond | ~150  | medium  |
| Tests: distributed correlation (API server ≠ engine)   | ~120     | medium  |
| Example workflow + docs/site page                      | ~100     | trivial |

**Total ~1,230 LOC** (incl. tests). Single contributor estimate: **1 week**.

Should split into ≥3 PRs:

1. `HTTPConfig` + trigger plumbing + `dag.StepTypeRespond` (no end-to-end yet)
2. API handler + correlation subscription + happy path e2e
3. Failure-mode coverage (timeout/fail/cancel/no-respond) + validation

## Open questions for the planning phase

1. **HTTP method semantics for input**: should `GET` triggers carry query
   string only (Inngest's beta limitation) or query+headers? Recommend
   query+headers; no body for GET.
2. **First-write-wins enforcement**: rely on NATS subject having one
   subscriber, or have engine track "responded" state per run and refuse
   second `respond` step? Recommend the latter — fail-loud in workflow
   logs even if NATS would silently drop.
3. **Streaming responses**: out of scope for v1 (Inngest supports, we
   defer). Mark as future ADR.
4. **TLS termination**: same answer as today (operator's responsibility,
   front with caddy/nginx/CF). No change.
5. **Authentication**: out of scope at the trigger layer — workflow author
   adds an auth step before `respond`. Optional `secret`/HMAC field in
   `HTTPConfig` mirroring `WebhookConfig.Secret` is a maybe.
6. **Idempotency**: should the trigger generate a `Nats-Msg-Id` from a
   request hash so repeated calls don't fan out duplicate runs? Recommend
   yes, **opt-in** via `HTTPConfig.IdempotencyHeader` (e.g.
   `Idempotency-Key`).
7. **Observability**: the run_id should be returned in a response header
   (`X-Dagnats-Run-Id`) so callers can inspect via `dagnats run inspect`.
8. **Per-route concurrency limits**: defer to JetStream consumer config on
   the trigger? Or surface as `HTTPConfig.MaxConcurrent`? Recommend defer.
9. **Distributed: subject collision**: two `dagnats-api` replicas both
   subscribed to `dagnats.http.response.<run_id>`. Use queue group? With a
   unique `run_id` per request, only one replica generated it and only one
   has the open ResponseWriter — no collision in practice, but worth
   asserting explicitly in the design doc.

## Alignment with project rules

| CLAUDE.md rule                              | How this proposal complies                                     |
| ------------------------------------------- | -------------------------------------------------------------- |
| Ousterhout — deep modules, small interfaces | One new struct (`HTTPConfig`), one new step type. No new public packages. |
| TigerStyle — bounded everything             | `MaxBodyBytes`, `TimeoutMs`, response subject TTL (auto-clean) |
| NATS-Native — no custom infra               | Correlation via NATS subjects, not in-memory maps              |
| 70-line function limit                      | API handler split: read+validate, subscribe, publish, select   |
| Min 2 assertions per function               | Programmer errors panic on nil config, zero bounds, empty path |
| Provider-agnostic observability             | run_id in response header; trace context flows through history |
| All loops bounded                           | Select with timeout; no infinite waits                         |

## Consequences

### Positive

- dagnats workflows become addressable as normal HTTP endpoints. This is the
  single most-requested capability gap vs Inngest/iii.
- The DAG of an endpoint is inspectable via existing tooling
  (`dagnats run events <id>`) — an actual differentiator.
- POST body works from v1, beating Inngest's current beta.
- Zero new infrastructure dependencies.

### Negative

- The control plane no longer "only" hosts management routes — operators
  must understand that `dagnats-api` now also serves application traffic.
  Mitigation: clear documentation; consider a separate `dagnats-api` flag
  to disable HTTP triggers in deployments that want a pure control plane.
- New foot-gun: workflows with `http` trigger but no reachable `respond`.
  Mitigation: validation warning + 504 with clear error.
- Subject-per-run cardinality: NATS handles this fine (subjects are
  ephemeral) but should be acknowledged.

### Neutral

- ELv2-free design: nothing in this proposal copies code from iii. The
  `HTTPConfig` shape converges on iii's by virtue of HTTP semantics, not
  by lifting code.

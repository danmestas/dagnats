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
    Path              string `json:"path"`                          // exact match, e.g. "/api/orders"
    Method            string `json:"method"`                        // GET|POST|PUT|PATCH|DELETE
    TimeoutMs         int    `json:"timeout_ms"`                    // hard cap; default 30_000
    MaxBodyBytes      int64  `json:"max_body_bytes"`                // default 1 MiB
    Secret            string `json:"secret,omitempty"`              // optional HMAC, mirrors WebhookConfig.Secret
    IdempotencyHeader string `json:"idempotency_header,omitempty"`  // e.g. "Idempotency-Key"; becomes Nats-Msg-Id
}
```

Routes register on the same `cmd/dagnats-api` mux that hosts `/hooks/{path}` and
the control plane (`internal/api/rest.go`). The handler:

1. Reads and bounds the body via the shared `internal/httpenvelope` helper
   (programmer-error assertion: `MaxBodyBytes > 0`). The helper is used by
   both webhook and http triggers so envelope shape and body limits stay in
   one place — preventing the two from drifting under independent maintenance.
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
proceed normally and do not affect the already-sent HTTP response.

#### Mental model: `respond` is a side effect, not a return

This is the workflow-author's #1 cognitive trap, so spell it out:

    http trigger → [step A] → [step B] → respond → [step C] → [step D]
                                          │
                                          └─ HTTP response dispatched here
                                             (client connection released)

`[step C]` and `[step D]` run **after** the HTTP client has already received
its response. Their outputs are not visible to the caller. This is desirable
for cleanup, audit logging, or fanning out follow-up workflows.

**Anti-pattern:** placing an auth-revocation, billing-charge, or any
"must-complete-before-the-user-sees-success" operation *after* `respond`. The
user has already seen success; the late step can fail silently. Put such
steps **before** `respond`, or split into a separate workflow keyed off the
response event.

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

**Subject ownership.** The subject `dagnats.http.response.<run_id>` is
**engine-private** — not a public surface. The string is produced in exactly
one place: a helper `internal/trigger/http.ResponseSubject(runID string) string`.
The API handler (subscriber) and the engine's respond step (publisher) both
import it. Hardcoding the subject in two packages would be change
amplification waiting to happen; one helper makes it a single-site rename if
the subject ever needs versioning.

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
two layers of validation run before the workflow is persisted:

**Layer 1 — graph validation (`dag/` package, pure, no NATS):**

- Warn if any reachable terminal step path does not include a `respond` step
  (the 504-hang foot-gun).
- Warn if more than one `respond` step is *simultaneously reachable* from the
  trigger (the duplicate-respond case — see Q2 resolution). Multiple respond
  nodes on mutually-exclusive branches (e.g. happy vs error) are legitimate;
  the validator distinguishes "both reachable on the same execution" from
  "one of two branches will run."

Fatal rejection is too strict (legitimate branch-per-outcome patterns exist),
so both checks emit warnings, not errors. Silent acceptance breeds production
hangs and silent runtime drops.

**Layer 2 — field validation (`internal/trigger` package):**

- `Path` syntax (must start with `/`, no wildcards in v1).
- `Method` enum (GET|POST|PUT|PATCH|DELETE).
- `MaxBodyBytes > 0`, `TimeoutMs > 0`.
- If `Secret` is set, minimum length check.
- If `IdempotencyHeader` is set, syntactically valid HTTP header name.

Field validation is fatal; graph validation surfaces warnings in the
`POST /workflows` response body so the workflow author sees them at
registration time, not first production hang.

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

### ...add engine-tracked "responded" state for runtime first-write-wins

An earlier draft proposed a JetStream KV gate (`KV.Create("responded:<run_id>")`)
to fail loudly on a second runtime `respond`. Rejected: it introduces a new
engine state primitive the codebase does not have today — per-run "side
effect performed" tracking is absent (see `internal/engine/task_publisher.go:36`,
which only tracks per-`(runID, stepID, attempt)` task dedup). Two sources of
truth (KV and subject) can disagree under partition. The chosen design
instead relies on:

1. **Graph validator** catches duplicate `respond` at registration (most cases).
2. **Subject semantics** — the originating API replica unsubscribes after the
   first publish; a second publish has no subscriber and is silently dropped
   at NATS. This is benign because the HTTP response is already on the wire.
3. **Per-task dedup** — the existing `Nats-Msg-Id` keyed on `(runID, stepID,
   attempt)` prevents the *same* respond step from re-firing on retry.

This pulls the loudness *up* to validation (where the cost is a warning in
the registration response) rather than *sideways* into runtime state (where
the cost is a new KV bucket and a partition-risk dependency).

## Implementation plan (estimate)

| Component                                                              | LOC est. | Risk    |
| ---------------------------------------------------------------------- | -------- | ------- |
| `internal/httpenvelope`: shared body-bounding + envelope build (webhook+http) | ~60      | low     |
| `internal/trigger`: `HTTPConfig` types + field validation              | ~80      | low     |
| `internal/trigger/http.go`: HTTPHandler (uses `httpenvelope` + `ResponseSubject`) | ~150 | medium  |
| `internal/trigger/http.go`: `ResponseSubject(runID)` helper            | ~10      | low     |
| `internal/api`: route registration from trigger KV                     | ~100     | medium  |
| `dag`: `StepTypeRespond`, `RespondConfig`                              | ~50      | low     |
| `dag`: graph validation (reachability + multiplicity for `respond`)    | ~100     | low     |
| `engine`: respond step execution (publishes via `ResponseSubject`)     | ~80      | low     |
| Tests: trigger fire→workflow→respond happy path                        | ~200     | low     |
| Tests: timeout, run-failure, run-cancellation, no-respond              | ~150     | medium  |
| Tests: distributed correlation (API server ≠ engine)                   | ~120     | medium  |
| Tests: graph validator (missing respond, duplicate respond warnings)   | ~60      | low     |
| Tests: idempotency-header dedup window                                 | ~50      | low     |
| Example workflow + docs/site page                                      | ~100     | trivial |

**Total ~1,310 LOC** (incl. tests). Single contributor estimate: **1 week**.

Should split into ≥3 PRs:

1. `HTTPConfig` + trigger plumbing + `dag.StepTypeRespond` (no end-to-end yet)
2. API handler + correlation subscription + happy path e2e
3. Failure-mode coverage (timeout/fail/cancel/no-respond) + validation

## Resolved questions

Resolutions captured during the planning phase. Each is one-liner; see
referenced sections above for design impact.

1. **GET trigger inputs:** query + headers, no body. Same envelope shape as
   POST. Stripping headers on GET would create an unknown unknown ("why did
   my `Authorization` header vanish?"). See `HTTPConfig`.
2. **First-write-wins enforcement:** pure subject semantics + graph validator
   warns on duplicate `respond`. No engine KV state. See "Why not... add
   engine-tracked responded state" and "Workflow validation at registration".
3. **Streaming responses:** deferred to a future ADR. v1 = single-shot
   `respond` (one publish, one HTTP response). Adding streaming later is a
   strict superset; adding it now would force a stream-vs-single-shot
   interface choice on every workflow author from day one.
4. **TLS termination:** operator's responsibility (caddy/nginx/CF), no
   change from status quo. Reuses the existing `dagnats-api` mux conventions.
5. **Authentication:** out of scope at the trigger layer — workflow author
   adds an auth step before `respond`. `HTTPConfig.Secret` (HMAC) is offered
   as a near-zero-cost shared-secret guard, mirroring `WebhookConfig.Secret`.
6. **Idempotency:** opt-in via `HTTPConfig.IdempotencyHeader` (e.g.
   `Idempotency-Key`). The header value becomes the `Nats-Msg-Id`, leveraging
   JetStream's existing dedup window. **Caveat:** the default JetStream dedup
   window is 30s. A workflow author expecting Stripe-style 24h semantics will
   be surprised; the first PR must either surface the window in `HTTPConfig`
   or bump the trigger stream's default dedup window to a more realistic
   value (decision deferred to implementation, documented in PR 1).
7. **Observability:** `X-Dagnats-Run-Id` always present in the response, not
   configurable. The value is tiny, the upside (debug via `dagnats run
   inspect`) is universal, and there's no scenario where a caller is *harmed*
   by an extra header. Pull complexity down by not making it a knob.
8. **Per-route concurrency limits:** deferred. Already expressible via
   `MaxAckPending` on the trigger's JetStream consumer; surfacing a second
   `HTTPConfig.MaxConcurrent` knob would be change amplification (two ways to
   spell the same constraint, divergent under load). Add the field only after
   a real user reads the docs and still asks for it.
9. **HA correlation (subject collision under replicas):** single-replica
   correlation is the v1 contract. The originating `dagnats-api` replica is
   the only subscriber to `dagnats.http.response.<run_id>`; if that replica
   dies between subscribe and publish, the client's TCP socket closes and
   the front-end proxy returns 502 — which is correct, since the client
   connection is gone. Durable in-flight HTTP request survival across API
   restarts is a future ADR if anyone asks.

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

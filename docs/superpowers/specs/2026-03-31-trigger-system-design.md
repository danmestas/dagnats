# DagNats Trigger System Design

## Context

DagNats has no trigger system. Workflows can only be started manually via REST API,
NATS request/reply, or direct event publishing. This design adds automatic workflow
triggering via cron schedules, NATS subject subscriptions, and HTTP webhooks.

## Design Decisions

1. **Package location: `trigger/` in dagnats core** — Triggers are infrastructure,
   not agent-specific. They start any workflow type.
2. **Missed schedule recovery: Configurable per-schedule** — Each cron trigger has a
   `backfill` flag. Idempotent pipelines want backfill; notifications don't.
3. **NATS subject input: Envelope with metadata** — All trigger types wrap the payload
   in a standard envelope with trigger type, source, and timestamp. Workflows always
   know how they were triggered.
4. **Webhook implementation: HTTP→NATS adapter** — Webhook handler publishes to an
   internal NATS subject; a subject trigger picks it up. Thin layer on top of NATS
   triggers.

---

## Data Model

```go
// TriggerDef defines a single trigger. Exactly one of Cron, Subject,
// or Webhook is non-nil.
type TriggerDef struct {
    ID         string         `json:"id"`
    WorkflowID string        `json:"workflow_id"`
    Enabled    bool           `json:"enabled"`
    Cron       *CronConfig    `json:"cron,omitempty"`
    Subject    *SubjectConfig `json:"subject,omitempty"`
    Webhook    *WebhookConfig `json:"webhook,omitempty"`
}

type CronConfig struct {
    Expression string `json:"expression"`  // 5-field cron
    Timezone   string `json:"timezone"`    // IANA, default "UTC"
    Backfill   bool   `json:"backfill"`    // recover missed runs on startup
}

type SubjectConfig struct {
    Subject string `json:"subject"` // NATS subject (wildcards allowed)
}

type WebhookConfig struct {
    Path   string `json:"path"`             // URL path, e.g. "/hooks/deploy"
    Secret string `json:"secret,omitempty"` // HMAC-SHA256 key
}
```

### TriggerEnvelope

All trigger types produce the same workflow input:

```go
type TriggerEnvelope struct {
    Trigger   string          `json:"trigger"`    // "cron", "nats", "webhook"
    Source    string          `json:"source"`     // expression, subject, or path
    Timestamp time.Time       `json:"timestamp"`
    Data      json.RawMessage `json:"data,omitempty"`
}
```

### Validation Rules

- Exactly one of Cron/Subject/Webhook must be non-nil
- WorkflowID must reference an existing def in `workflow_defs` KV
- Cron expression must parse as valid 5-field cron
- Webhook path must start with `/`
- Subject must not be empty
- ID must not be empty

### NATS Resources

| Resource | Name | Purpose |
|----------|------|---------|
| KV Bucket | `triggers` | Trigger definitions (JSON) |
| KV Bucket | `trigger_state` | Last-run timestamps for cron schedules |

---

## Architecture

### File Structure

```
trigger/
  types.go        — TriggerDef, CronConfig, SubjectConfig, WebhookConfig, Envelope
  validate.go     — TriggerDef validation
  scheduler.go    — Cron ticker, backfill logic
  subject.go      — NATS subject subscriber
  webhook.go      — HTTP→NATS gateway with HMAC validation
  service.go      — TriggerService (lifecycle, KV watcher, coordination)
```

### TriggerService

Single entry point that owns all trigger types:

1. Loads trigger definitions from `triggers` KV bucket on startup
2. Starts cron scheduler, NATS subscribers, and webhook HTTP server
3. When any trigger fires, publishes `workflow.started` event to history stream
4. Watches KV for live trigger changes (add/remove without restart)

---

## Component Behavior

### Cron Scheduler (`scheduler.go`)

- Ticks every 30 seconds, checks all enabled cron triggers against current time
- Deduplicates via `Nats-Msg-Id`: `trigger.{triggerID}.{minute_timestamp}`
  Same minute never fires twice even with multiple service instances
- Updates `trigger_state` KV with `last_run_at` after each fire
- Timezone-aware: parses `CronConfig.Timezone` (defaults to UTC)

**Backfill on startup:**
- For each cron trigger with `backfill: true`:
  - Load `last_run_at` from `trigger_state` KV
  - Calculate all missed cron ticks between `last_run_at` and now
  - Fire missed runs oldest-first
  - Cap at 100 backfills per trigger (prevent flood after long outage)

**Cron parsing:**
- 5-field standard: minute hour day-of-month month day-of-week
- Supports `*`, `*/N`, `N-M`, comma-separated values
- Implemented in-house (~100 LOC, no external dependency per CLAUDE.md rules)

### NATS Subject Trigger (`subject.go`)

- One JetStream push-subscribe per subject trigger
- On message receipt:
  1. Build TriggerEnvelope with trigger="nats", source=subject, data=msg.Data
  2. Publish `workflow.started` event to `history.{newRunID}`
  3. Ack the message
- On publish failure: nak with 5s delay (NATS redelivers)
- Supports wildcard subjects (`events.>`, `deploy.*.completed`)

### Webhook Handler (`webhook.go`)

- Single `http.ServeMux` serving all webhook paths
- Request flow:
  1. Match path to webhook trigger
  2. If secret configured: validate HMAC-SHA256 (header `X-Signature-256`)
  3. Read body (capped at 1MB)
  4. Publish to internal NATS subject `webhook.{triggerID}` with body as data
  5. Return 202 Accepted immediately
- Error responses: 401 bad signature, 404 unknown path, 413 body too large
- Internally, the webhook publish is picked up by a subject trigger on
  `webhook.{triggerID}` — webhooks are HTTP→NATS adapters

### Live Reload (KV Watcher)

- TriggerService watches `triggers` KV bucket for changes
- On create/update: validate, add or update trigger
  - Cron: rebuild schedule entry
  - Subject: unsubscribe old consumer, subscribe new
  - Webhook: update route table (no HTTP server restart)
- On delete: stop trigger, remove from active set
- Bounded: max 500 active triggers (prevent runaway)

---

## CLI

```
dagnats trigger create <workflow> --cron "0 9 * * 1-5" [--tz "America/Denver"] [--backfill]
dagnats trigger create <workflow> --subject "events.deploy.>"
dagnats trigger create <workflow> --webhook "/hooks/deploy" [--secret "s3cret"]
dagnats trigger list
dagnats trigger delete <trigger-id>
dagnats trigger history <trigger-id>
```

---

## Event Flow

### Cron Trigger
```
[30s tick] → scheduler checks cron expressions
  → match found → build TriggerEnvelope
  → generate run ID (nuid)
  → publish workflow.started event to history.{runID}
  → update trigger_state KV with last_run_at
  → orchestrator picks up event, runs workflow
```

### NATS Subject Trigger
```
[NATS message on subject] → subject trigger receives
  → build TriggerEnvelope with msg.Data
  → generate run ID
  → publish workflow.started event
  → ack original message
  → orchestrator picks up event, runs workflow
```

### Webhook Trigger
```
[HTTP POST /hooks/deploy] → webhook handler
  → validate HMAC if secret set
  → publish body to NATS subject webhook.{triggerID}
  → return 202
  → internal subject trigger receives
  → build TriggerEnvelope
  → publish workflow.started event
  → orchestrator picks up event, runs workflow
```

---

## Error Handling

- **Cron parse failure at registration:** Rejected with error message
- **Workflow def not found at fire time:** Log error, skip this fire, don't crash service
- **NATS publish failure:** Retry via nak (subject triggers) or log + skip (cron)
- **Webhook HMAC failure:** Return 401, no retry
- **KV watch error:** Log, reconnect with backoff
- **Backfill overflow (>100 missed):** Fire 100, log warning, update last_run_at to now

---

## Testing Strategy

**Unit tests:**
- Cron expression parsing and next-fire-time calculation
- TriggerDef validation (positive + negative for all trigger types)
- TriggerEnvelope construction for each trigger type
- Backfill calculation (last_run_at vs expected ticks, cap at 100)
- HMAC-SHA256 signature validation and rejection

**Integration tests (real embedded NATS, per test):**
- Cron trigger fires, `workflow.started` event appears on history stream
- NATS subject trigger receives message, workflow starts
- Webhook trigger receives HTTP POST, workflow starts
- Dedup: same cron minute doesn't fire twice
- Live reload: add trigger via KV, verify it starts firing
- Backfill: set `last_run_at` to 3 ticks ago, verify 3 runs created on startup
- Disabled trigger doesn't fire

No shared NATS servers between tests.

---

## What Changes in Existing Code

### `natsutil/conn.go`
Add `triggers` and `trigger_state` KV buckets to default setup, or provision
via `WithKVBuckets` option (already supported).

### `cli/`
Add `trigger` subcommand with create/list/delete/history.

### No changes to `engine/`, `dag/`, `worker/`, `protocol/`
Triggers produce standard `workflow.started` events — the orchestrator doesn't
know or care that a trigger started the workflow.

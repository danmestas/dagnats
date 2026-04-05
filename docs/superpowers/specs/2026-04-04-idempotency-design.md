# Idempotency by Expression

**Status:** Design
**Date:** 2026-04-04
**Depends on:** Nothing (independent)

## Problem

When the same logical event triggers a workflow multiple times (webhook retries,
duplicate messages, user double-clicks), each trigger creates a separate workflow
run. There's no way to say "if a run with this key already exists, return the
existing run instead of creating a new one."

Inngest provides `Idempotency: "event.data.request_id"` — an expression
evaluated against the event data to produce a dedup key.

## Design

### 1. Concept

Idempotency prevents duplicate workflow runs by keying on a value extracted from
the input. When starting a run with an idempotency key:

1. Hash the key to a deterministic string.
2. Check if a run with that key already exists (and is not terminal).
3. If yes: return the existing run ID (no new run created).
4. If no: create the run and store the key mapping.

The key is a **dot-path expression** evaluated against the workflow input, reusing
`dag.ExtractDotPath`. No new expression language needed.

### 2. Type Changes

**`dag/types.go`** — add field on `WorkflowDef`:

```go
type WorkflowDef struct {
    // ... existing fields ...
    IdempotencyKey string `json:"idempotency_key,omitempty"`
}
```

The value is a dot-path like `"request_id"` or `"data.order_id"`. Evaluated
against the workflow input at `StartRun` time.

**`dag/builder.go`** — builder method:

```go
func (b *WorkflowBuilder) WithIdempotencyKey(
    dotPath string,
) *WorkflowBuilder {
    b.idempotencyKey = dotPath
    return b
}
```

### 3. How It Works

**KV bucket: `idempotency_keys`** — maps `{workflowName}.{keyHash}` to `{runID}`.

**TTL:** Configurable per workflow, default 24 hours. After expiry, the same key
can trigger a new run. This prevents unbounded growth while covering retry
windows.

**Flow (in `api/service.go` — `startRunInner`):**

1. Load workflow def from KV.
2. If `IdempotencyKey` is empty: start run normally (no change).
3. If `IdempotencyKey` is set:
   a. Extract the key value from input via `dag.ExtractDotPath`.
   b. Hash it: `sha256(workflowName + "." + keyValue)` (full 64-char hex).
      No truncation — KV keys have no meaningful length limit, and full SHA-256
      eliminates collision discussion entirely.
   c. Check `idempotency_keys.{workflowName}.{hash}` in KV.
   d. If entry exists: return the existing run ID. Don't check whether the
      run is terminal — let KV TTL handle expiry. If someone re-triggers
      within the TTL of a completed run, they get the existing run ID back,
      which is correct (the operation was already done).
   e. If no entry: create the run, then store the mapping.

**Race condition:** Two identical requests arrive simultaneously. Both check KV,
both find no entry, both try to create. Solution: use KV `Create` (not `Put`) —
it fails if the key already exists. The loser retries the check and returns the
winner's run ID.

```go
_, err := idempotencyKV.Create(kvKey, []byte(runID))
if err != nil {
    // Key already exists — another request won the race.
    // Load the existing run ID and return it.
    entry, _ := idempotencyKV.Get(kvKey)
    return string(entry.Value()), nil
}
```

This defines the race condition out of existence — KV `Create` is atomic.

### 4. Trigger Integration

Triggers can also use idempotency. When a trigger fires a workflow that has
`IdempotencyKey`, the trigger passes the event data as input, and the normal
`StartRun` path handles dedup. No special trigger-side logic needed.

### 5. Validation

**`dag/validate.go`:**

- `IdempotencyKey` must be a valid dot-path if non-empty (no empty segments,
  no leading/trailing dots).
- `IdempotencyKey` is validated syntactically at `Build()` time. Runtime
  extraction failure (key not found in input) is not an error — the run starts
  without idempotency protection, and a warning is logged.

### 6. API Response

`StartRun` continues to return `(string, error)` — the Go API doesn't change.
The idempotency mechanism is invisible to callers (information hiding).

The REST layer signals idempotent returns via HTTP status code only:
`201 Created` for new runs, `200 OK` for idempotent returns. The response
body is the same either way (just the run ID). Callers that don't care about
the distinction ignore the status code difference.

### 7. NATS Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `idempotency_keys` | KV (TTL: 24h default) | Key-to-runID mapping |

### 8. Bounds

- Maximum key length: 256 characters (before hashing).
- KV TTL: 24 hours default, configurable via `IdempotencyTTL` on WorkflowDef.
- Maximum concurrent idempotency checks: bounded by API request concurrency.
- Hash: full SHA-256 (64 hex chars). No truncation, no collision concern.

### 9. Builder API

```go
wb := dag.NewWorkflow("process-payment").
    WithIdempotencyKey("data.payment_id")
wb.Task("charge", "charge-card")
wb.Task("receipt", "send-receipt").DependsOn("charge")
def, _ := wb.Build()
```

```bash
dagnats run start process-payment \
    --input '{"data":{"payment_id":"pay_123","amount":100}}'
# Returns: run_abc (new)

dagnats run start process-payment \
    --input '{"data":{"payment_id":"pay_123","amount":100}}'
# Returns: run_abc (existing — idempotent)
```

### 10. CLI

```
dagnats run start process-payment --input '...'
```

Output when idempotent:
```
run_id: run_abc (idempotent — existing run returned)
```

### 11. Edge Cases

- **Input missing the key path:** Run starts without idempotency. Warning logged.
  This is a soft failure — better to run than to reject.
- **Run completes, same key sent again:** KV entry still exists within TTL.
  Returns the existing (completed) run ID — correct behavior, the operation
  was already done. After TTL expiry, a new run can be created.
- **Run cancelled, same key sent:** Same as completed — returns existing run ID
  within TTL. Caller can inspect run status if they need to decide whether to
  retry with a different key.
- **KV unavailable:** Start run normally without idempotency. Log error. Don't
  fail the run because the dedup layer is down.

### 12. Observability

- Metric: `api.idempotency.hits` — returned existing run.
- Metric: `api.idempotency.misses` — created new run.
- Metric: `api.idempotency.races` — lost Create race, returned winner's run.
- Log: warn when key extraction fails (missing path in input).

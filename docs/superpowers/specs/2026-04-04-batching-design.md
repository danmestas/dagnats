# Event Batching

**Status:** Design
**Date:** 2026-04-04
**Depends on:** Nothing (independent)

## Problem

When a trigger fires rapidly (e.g., hundreds of webhook events per second for a
data pipeline), each event spawns a separate workflow run. This is wasteful when
the workflow could process many events at once — for example, batch-inserting
rows, sending a single API call with multiple items, or summarizing a burst of
notifications into one digest.

There is no way to say "collect events for up to 5 seconds or until you have 50,
then start one run with all of them."

## Design

### 1. Concept

Batching collects multiple triggering events into a single workflow run. The run
receives a JSON array of event payloads instead of a single event. Two conditions
control when the batch fires:

- **MaxSize:** Fire when N events have accumulated.
- **Timeout:** Fire after T duration even if MaxSize is not reached.

Whichever condition is met first triggers the batch. This ensures latency is
bounded (timeout) while allowing full batches when traffic is high (maxSize).

An optional **Key** groups events into separate batches by entity (e.g., per
tenant, per repo) using dot-path extraction from the event data.

### 2. Type Changes

**`trigger/types.go`** -- add `BatchConfig` and a field on `TriggerDef`:

```go
// BatchConfig collects multiple events into a single workflow run.
// The batch fires when MaxSize is reached or Timeout elapses,
// whichever comes first.
type BatchConfig struct {
    MaxSize int           `json:"max_size"`
    Timeout time.Duration `json:"timeout"`
    Key     string        `json:"key,omitempty"` // dot-path for per-entity batching
}
```

```go
type TriggerDef struct {
    // ... existing fields ...
    Batch *BatchConfig `json:"batch,omitempty"`
}
```

### 3. How It Works

**KV bucket: `batch_state`** -- stores the accumulating batch per window.

Key format: `{triggerID}` (global) or `{triggerID}.{keyValue}` (per-entity).

Value:

```go
type batchEntry struct {
    Events    []json.RawMessage `json:"events"`
    FirstSeenAt int64           `json:"first_seen_ns"`
    TimerSeq    uint64          `json:"timer_seq"`
}
```

**Flow:**

1. Trigger receives event.
2. If `Batch` is nil, dispatch immediately (existing behavior).
3. If `Batch` is set:
   a. Compute batch key: `{triggerID}` or `{triggerID}.{extractDotPath(key, data)}`.
   b. CAS-load the `batch_state` entry for this key.
   c. Append event to `Events` array.
   d. If `len(Events) >= MaxSize`: fire immediately -- publish `workflow.started`
      with the full events array as input, delete the entry.
   e. If this is the first event (`FirstSeenAt` was not set): schedule a
      `SLEEP_TIMERS` message with action `batch_fire` for `Timeout` duration.
      Store the timer's seq in `TimerSeq`.
   f. Save the updated entry via CAS.
4. When the `batch_fire` timer fires:
   a. Load the batch entry.
   b. Verify `TimerSeq` matches (guards against stale timers from a
      batch that already fired via MaxSize).
   c. If match: publish `workflow.started` with the events array, delete entry.
   d. If no match: discard (batch already fired).

**Input format to workflow:** The workflow receives a `TriggerEnvelope` where
`Data` is a JSON array of the original event payloads:

```json
{
    "trigger": "subject",
    "source": "batch",
    "timestamp": "2026-04-04T12:00:00Z",
    "data": [
        {"item": "a"},
        {"item": "b"},
        {"item": "c"}
    ]
}
```

Workers distinguish batched from non-batched runs by checking if `data` is an
array. No schema change to `TriggerEnvelope` -- the `Data` field is already
`json.RawMessage`.

### 4. Timer Integration

**`engine/sleeptimer.go`** -- one new action:

```go
const TimerActionBatchFire TimerAction = "batch_fire"
```

`TimerMessage` gains a `TriggerID` field (shared with debounce):

```go
TriggerID string `json:"trigger_id,omitempty"`
```

When `batch_fire` fires, the consumer:
1. Loads `batch_state.{key}`.
2. Checks `TimerSeq` matches.
3. If match: wraps events in `TriggerEnvelope`, publishes `workflow.started`,
   deletes entry.
4. If no match: discard (already fired via MaxSize).

### 5. Validation

**`trigger/validate.go`**:

- `Batch.MaxSize` must be in [2, 1000]. (1 is pointless, >1000 risks payload size.)
- `Batch.Timeout` must be in [100ms, 5 minutes].
- `Batch.Key` must be a valid dot-path if non-empty.
- Batch is incompatible with debounce (mutual exclusion -- debounce collapses to
  last event, batch collects all events).
- Cron triggers cannot have batching (no events to batch).
- Total batch payload must not exceed 10 MiB (enforced at fire time; if exceeded,
  split into multiple runs of MaxSize/2 each).

### 6. Builder / CLI API

**Trigger creation:**

```bash
dagnats trigger create my-pipeline \
    --subject="data.ingested" \
    --batch-max=50 \
    --batch-timeout=5s \
    --batch-key="data.tenant_id"
```

**Go API:**

```go
trigger := TriggerDef{
    WorkflowID: "ingest-pipeline",
    Subject:    &SubjectConfig{Subject: "data.ingested"},
    Batch: &BatchConfig{
        MaxSize: 50,
        Timeout: 5 * time.Second,
        Key:     "data.tenant_id",
    },
}
```

### 7. NATS Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `batch_state` | KV (TTL: 2x max timeout or 10 min) | Accumulating batch entries |

`SLEEP_TIMERS` stream reused with `batch_fire` action discriminator.

### 8. Bounds

- Max batch size: 1000 events.
- Max batch timeout: 5 minutes.
- Max batch payload: 10 MiB.
- Max concurrent batch windows per trigger: 10,000 (per-key).
- CAS retry limit: 10.

### 9. Observability

- Metric: `trigger.batch.events_collected` -- histogram of events per fired batch.
- Metric: `trigger.batch.fires` -- counter, with label `reason: maxsize|timeout`.
- Metric: `trigger.batch.payload_bytes` -- histogram of fired batch payload sizes.
- Log: warn when batch payload exceeds 5 MiB (approaching 10 MiB limit).

### 10. Edge Cases

- **MaxSize reached exactly:** Fire immediately, timer becomes stale and
  self-discards on next delivery.
- **Rapid events exceed MaxSize multiple times before timer:** Each time MaxSize
  is reached, fire and start a new accumulation window. Timer for the first
  window is stale; subsequent windows get their own timers.
- **Trigger disabled during accumulation:** Timer fires, checks trigger enabled
  state, discards if disabled. Entry cleaned up.
- **Trigger deleted during accumulation:** KV watch detects deletion, cleans up
  batch entries.
- **Engine restart:** Batch state is in KV, timers are in `SLEEP_TIMERS`. Both
  survive restarts.
- **Payload size exceeded:** Split into ceil(N / (MaxSize/2)) sub-batches, each
  published as a separate `workflow.started` event. Log warning.
- **Single event arrives, timeout fires:** Run starts with a 1-element array.
  This is valid -- the workflow always receives an array.

### 11. Interaction Matrix

| Feature | Compatible | Notes |
|---------|-----------|-------|
| Debounce | **No** | Mutual exclusion at validation |
| Throttling | Yes | Throttle applies to fired batches, not individual events |
| Rate limiting | Yes | Rate limit applies per fired batch |
| Concurrency | Yes | Each fired batch = one run for concurrency purposes |
| Priority | Yes | Priority resolved from first event in batch |
| Singleton | **No** | Singleton assumes 1:1 event:run; batching breaks this |
| CancelOn | Yes | Cancels the run once started |
| Idempotency | **No** | Event-level dedup IDs don't map to batch-level |

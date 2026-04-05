# Orbit: JetStream Extensions (jetstreamext)

**Status:** Design
**Date:** 2026-04-04 (revised 2026-04-05)
**Depends on:** JetStream API migration (Phase 1 minimum)
**Source:** github.com/synadia-io/orbit.go/jetstreamext v0.2.1
**Requires:** nats-server v2.12.0+

## Problem

When a DAG step completes and multiple downstream steps become ready, the engine
publishes each task individually. If the engine crashes mid-fan-out, some tasks
land and others don't. The existing idempotent replay handles this correctly, but
atomic fan-out reduces the failure window and simplifies the code path.

Separately, the CLI and engine read batches of messages from streams using
throwaway consumers with ad-hoc sequence tracking.

## Actual API (verified)

```go
// Atomic batch publish — all messages land or none do
func PublishMsgBatch(
    ctx context.Context,
    js jetstream.JetStream,       // NEW API required
    messages []*nats.Msg,
    opts ...PublishMsgBatchOpt,
) (*BatchAck, error)

// Batch read by sequence
func GetBatch(
    ctx context.Context,
    js jetstream.JetStream,
    stream string, batch int,
    opts ...GetBatchOpt,
) (iter.Seq2[*jetstream.RawStreamMsg, error], error)

// Batch read last messages for subjects
func GetLastMsgsFor(
    ctx context.Context,
    js jetstream.JetStream,
    stream string, subjects []string,
    opts ...GetLastForOpt,
) (iter.Seq2[*jetstream.RawStreamMsg, error], error)
```

All functions require `jetstream.JetStream` (new API), `context.Context`,
and return Go 1.23+ `iter.Seq2` iterators.

## Design

### 1. Atomic Task Fan-Out

After the engine migrates `task_publish.go` to the new API (migration Phase 1),
replace the publish loop in `enqueueReadySteps` with `PublishMsgBatch`.

The existing `collectReadyMessages` extraction (already implemented in the
worktree) builds `[]*nats.Msg` without publishing. The batch call replaces
the for-loop:

```go
_, err = jetstreamext.PublishMsgBatch(ctx, jsNew, msgs)
```

**Mixed-stream handling:** `taskSubject()` routes agent steps to `agent_task.>`
(AGENT_TASKS stream) and normal steps to `task.>` (TASK_QUEUES). Atomic publish
works within a single stream. Split messages by stream prefix before batching:

```go
var taskMsgs, agentMsgs []*nats.Msg
for _, msg := range msgs {
    if strings.HasPrefix(msg.Subject, "agent_task.") {
        agentMsgs = append(agentMsgs, msg)
    } else {
        taskMsgs = append(taskMsgs, msg)
    }
}
```

Two atomic batches: one per stream. Each is independently all-or-nothing.

**Cross-stream gap:** Task fan-out is atomic per stream. The workflow lifecycle
event (to WORKFLOW_HISTORY) remains a separate publish with `Nats-Msg-Id` dedup.
Same correctness model as today.

### 2. Stream Configuration

`AllowAtomicPublish` must be enabled on TASK_QUEUES. Already implemented via
`natsutil.EnableAtomicPublish(nc, "TASK_QUEUES")` using the new jetstream API.
Also enable on AGENT_TASKS if agent steps exist.

### 3. Batch History Retrieval

Replace throwaway consumers in the API service layer with `GetLastMsgsFor`:

```go
iter, err := jetstreamext.GetLastMsgsFor(
    ctx, jsNew, "WORKFLOW_HISTORY",
    []string{"workflow." + workflowID + ".>"},
)
for msg, err := range iter {
    // process msg
}
```

### 4. Recovery Replay

On engine startup, replay from a sequence watermark:

```go
iter, err := jetstreamext.GetBatch(
    ctx, jsNew, "WORKFLOW_HISTORY", 10000,
    jetstreamext.GetBatchSeq(lastProcessedSeq),
)
```

### 5. Bounds

- Max messages per atomic batch: 100 (server default)
- DAG fan-out rarely exceeds 20 steps — well within limit
- GetBatch max per call: 10,000

### 6. Observability

- Metric: `engine.publish.batch_size` — histogram of messages per atomic batch
- Metric: `cli.history.batch_fetch_ms` — histogram of batch retrieval latency

### 7. Risks

- **Batch size limits:** If a DAG has > 100 parallel steps, chunk into
  sub-batches. Each sub-batch is independently atomic.
- **Cross-stream gap:** Already handled by idempotent replay. No new failure mode.
- **`publishTask` vs `Orchestrator.publishTask`:** The package-level function in
  `task_publish.go` is replaced. The method on `Orchestrator` in
  `orchestrator.go` is a different function and must not be removed.

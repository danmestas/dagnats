# Bulk Cancellation

**Status:** Design
**Date:** 2026-04-04
**Depends on:** Nothing (builds on existing CancelRun)

## Problem

Cancelling runs is currently one-at-a-time: `POST /runs/{id}/cancel` or
`dagnats run cancel <run-id>`. When an incident requires cancelling hundreds of
runs (bad deploy triggered a flood, faulty trigger created junk runs, need to
drain a workflow before maintenance), operators must script a loop or cancel
each run manually.

## Design

### 1. Concept

Bulk cancellation cancels multiple runs in a single operation, filtered by:

- **Workflow ID:** Cancel all running/pending runs of a specific workflow.
- **Status:** Cancel only `running`, only `pending`, or both (default: both).
- **Time range:** Cancel runs created between `after` and `before` timestamps.
- **Dry run:** Preview which runs would be cancelled without acting.

The operation is **bounded** (max 1000 runs per call) and **idempotent**
(cancelling an already-cancelled run is a no-op).

### 2. API

**`POST /runs/cancel`** -- new endpoint for bulk cancellation.

Request body:

```go
type BulkCancelRequest struct {
    WorkflowID string    `json:"workflow_id"`
    Status     string    `json:"status,omitempty"`     // "running", "pending", "all" (default: "all")
    After      time.Time `json:"after,omitempty"`       // only runs created after this
    Before     time.Time `json:"before,omitempty"`      // only runs created before this
    DryRun     bool      `json:"dry_run,omitempty"`
}
```

Response:

```go
type BulkCancelResponse struct {
    Cancelled []string `json:"cancelled"`          // run IDs that were cancelled
    Skipped   []string `json:"skipped,omitempty"`  // already terminal
    Total     int      `json:"total"`
    DryRun    bool     `json:"dry_run"`
}
```

### 3. How It Works

**`api/service.go`** -- add `BulkCancelRuns`:

```go
func (s *Service) BulkCancelRuns(
    ctx context.Context, req BulkCancelRequest,
) (BulkCancelResponse, error) {
```

**Flow:**

1. Validate request: `WorkflowID` must not be empty. `Status` defaults to
   `"all"`. `Before` must be after `After` if both set.
2. Load all runs via `store.ListAll(maxBulkCancelLimit)`.
3. Filter by: WorkflowID, status (running/pending/all), time range.
4. Sort by CreatedAt ascending (cancel oldest first).
5. Cap at `maxBulkCancelLimit` (1000). If more runs match, return an error
   indicating the filter is too broad (force the caller to narrow with time
   range or status).
6. If `DryRun`: return the matching run IDs without cancelling.
7. For each matching run: publish `workflow.cancelled` event via `cancelRunInner`.
   Collect results into `Cancelled` (newly cancelled) and `Skipped` (already
   terminal).
8. Return response.

**Concurrency:** Cancellation events are published sequentially, not in parallel.
Each event goes to the history stream and the orchestrator processes them under
the per-run lock. Sequential publishing avoids thundering herd on the
orchestrator. For 1000 runs, this takes ~1-2 seconds (1-2ms per publish).

### 4. REST Endpoint

**`api/rest.go`** -- add route `POST /runs/cancel`:

```go
case r.Method == "POST" && r.URL.Path == "/runs/cancel":
    handleBulkCancel(s, w, r)
```

```go
func handleBulkCancel(
    s *Service, w http.ResponseWriter, r *http.Request,
) {
    // Parse BulkCancelRequest from body
    // Call s.BulkCancelRuns(ctx, req)
    // Return BulkCancelResponse as JSON
}
```

### 5. CLI

```bash
# Cancel all running/pending runs of a workflow
dagnats run cancel-all --workflow=deploy-pipeline

# Cancel only running runs (leave pending queued)
dagnats run cancel-all --workflow=deploy-pipeline --status=running

# Cancel runs in a time window
dagnats run cancel-all --workflow=deploy-pipeline \
    --after="2026-04-04T00:00:00Z" \
    --before="2026-04-04T12:00:00Z"

# Preview without acting
dagnats run cancel-all --workflow=deploy-pipeline --dry-run

# Output (always):
# Cancelled 47 runs (3 skipped, already terminal)
# With --json flag:
# {"cancelled":["run-1","run-2",...],"skipped":["run-x"],"total":50}
```

### 6. Validation

- `WorkflowID` is required (prevent accidental "cancel everything").
- If more than `maxBulkCancelLimit` (1000) runs match, return error
  `"too many matching runs (N > 1000); narrow with --after/--before or --status"`.
- `After` must be before `Before` when both are set.
- `Status` must be one of: `"running"`, `"pending"`, `"all"`.

### 7. Bounds

- Max runs per bulk cancel: 1000.
- Sequential publish: ~1-2ms per run, ~1-2s for 1000 runs.
- No timeout on the bulk operation itself (the HTTP request may take up to 5s
  for 1000 runs; the CLI shows a progress indicator).

### 8. Observability

- Metric: `api.bulk_cancel.runs` -- histogram of runs cancelled per call.
- Metric: `api.bulk_cancel.duration_ms` -- histogram of operation duration.
- Log: info-level with workflow_id, count cancelled, count skipped.
- Span: `api.bulkCancelRuns` with attributes for workflow_id, filter params,
  result counts.

### 9. Edge Cases

- **Run transitions while iterating:** A run may complete between the list and
  the cancel. `cancelRunInner` checks `run.Status != RunStatusRunning` (existing
  guard in orchestrator's `handleWorkflowCancelled`). The run is added to
  `Skipped` if the cancel event is a no-op.
- **Concurrent bulk cancels:** Two bulk cancel requests for the same workflow
  race. Each publishes cancel events; the orchestrator's per-run lock ensures
  each run is cancelled exactly once. Dedup via `Nats-Msg-Id` on the cancel
  event prevents duplicate processing.
- **Pending runs:** Pending runs (queued behind concurrency limits) are
  cancelled by updating their KV snapshot status. They never start.
  `cancelRunInner` already publishes `workflow.cancelled` which the orchestrator
  handles for both running and pending statuses.
- **Empty result:** If no runs match, return `{"cancelled":[],"total":0}`.
  Not an error.
- **Dry run with stale data:** Between dry-run and actual cancel, runs may
  complete. This is expected; the actual cancel will skip them.

### 10. Why Not Async

Bulk cancel could be implemented as an async job (publish a "bulk cancel" command,
process in background, notify when done). This adds complexity (job tracking, status
polling, notification) for a bounded operation that completes in 1-2 seconds. The
synchronous approach is simpler and sufficient for the 1000-run cap.

If the cap needs to increase beyond 1000, revisit with an async pattern:
publish a `bulk_cancel.requested` event, process in a consumer, publish
`bulk_cancel.completed` when done.

### 11. NATS-Native Alternative Considered

An alternative: subscribe to `workflow_runs` KV and cancel matching runs via
KV watch. Rejected because:
1. KV watch is push-based -- harder to control rate of cancel event publishing.
2. No natural "done" signal (when has the watch seen all matching runs?).
3. The list-then-cancel approach is simpler and bounded.

### 12. Interaction Matrix

| Feature | Interaction |
|---------|------------|
| Concurrency | Cancelled runs release their concurrency slot; pending runs in queue auto-start |
| Singleton | Cancelled singleton releases the lock; new runs can claim it |
| CancelOn (event) | Bulk cancel and event cancel are independent; either can cancel a run |
| Compensation | Cancelled runs do NOT trigger compensation (cancellation is intentional, not failure) |
| Sub-workflows | Cancelling a parent cascades to non-detached children (existing behavior) |

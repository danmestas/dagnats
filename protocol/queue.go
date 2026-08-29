// protocol/queue.go
// QueueSnapshot is the shared payload for two surfaces (#632): the
// synchronous GET /v1/queue response and the periodic
// event.queue.snapshot notification on the EVENTS stream. Both surfaces
// answer the same question -- "how deep is each task-type queue right
// now" -- so they share one wire schema instead of drifting apart.
// See docs/wire-protocol.md "Consumer contract: run lifecycle events"
// for the event.queue.snapshot cadence/dedup contract.
package protocol

import "time"

// QueueGroup is the pending-task count for one task type on the
// TASK_QUEUES stream (task.{taskType} subject). OldestWaitMs is
// omitted when the server could not read the oldest pending message
// for this subject (a best-effort direct-get failure never fails the
// whole response/snapshot -- see internal/api/queue.go).
type QueueGroup struct {
	TaskType     string `json:"task_type"`
	Pending      int64  `json:"pending"`
	OldestWaitMs *int64 `json:"oldest_wait_ms,omitempty"`
}

// QueueSnapshot is the wire payload for GET /v1/queue and
// event.queue.snapshot. Groups is always a non-nil slice, sorted by
// TaskType, so JSON serializes it as [] rather than null when there
// are no pending tasks. Truncated is set when the TASK_QUEUES stream
// carries more distinct task-type subjects than
// internal/api.QueueGroupsMax -- Groups is capped at that bound rather
// than growing unbounded.
type QueueSnapshot struct {
	Groups     []QueueGroup `json:"groups"`
	SnapshotAt time.Time    `json:"snapshot_at"`
	Truncated  bool         `json:"truncated,omitempty"`
}

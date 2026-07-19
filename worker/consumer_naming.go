// worker/consumer_naming.go
// Worker-side bindings for the shared consumer-naming convention.
//
// The scheme itself lives in internal/consumername because the bridge's
// poll path must produce byte-identical durables (issue #532), and
// TASK_QUEUES is a work-queue stream that rejects a second consumer on an
// overlapping filter. Sharing via internal/ keeps that coupling out of
// the worker package's public SDK surface.
package worker

import "github.com/danmestas/dagnats/internal/consumername"

// defaultAckWait bounds the longest expected task duration plus a margin.
// See consumername.DefaultAckWait for the rationale and the deferred
// heartbeat work.
const defaultAckWait = consumername.DefaultAckWait

// sanitizeConsumerName maps a task-type or group string to a NATS-legal
// consumer-name fragment.
func sanitizeConsumerName(s string) string {
	return consumername.Sanitize(s)
}

// consumerNameFor produces the durable consumer name for a (taskType,
// group) pair. group=="" means the default branch.
func consumerNameFor(taskType, group string) string {
	return consumername.NameFor(taskType, group)
}

// consumerFilterFor produces the filter subject for a (taskType, group)
// pair. Inputs are NOT sanitized — they must round-trip exactly.
func consumerFilterFor(taskType, group string) string {
	return consumername.FilterFor(taskType, group)
}

// Package consumername owns the naming convention for dagnats-managed
// JetStream consumers on TASK_QUEUES. All durable names live under the
// "workers-" prefix; sanitization maps task-type/group strings to
// NATS-legal name fragments.
//
// Internal, not exported from worker: TASK_QUEUES is a work-queue stream,
// so the bridge's poll path must land on the byte-identical durable a
// native worker would create or JetStream rejects the second consumer for
// overlapping filters (issue #532). Both packages therefore share this
// one definition rather than the bridge reaching into worker's public
// SDK surface — the scheme is an internal invariant, not an API promise.
package consumername

import "time"

// DefaultAckWait bounds the longest expected task duration plus a margin.
// Workers running tasks longer than this should call msg.InProgress()
// periodically (planned: ADR-008 heartbeats, tracked as follow-up to
// issue #136) or override at handler registration via WithAckWait.
// See ADR-006 §"Out of scope (deferred)" for the full deferred-list.
const DefaultAckWait = 5 * time.Minute

// Sanitize maps a task-type or group string to a NATS-legal consumer-name
// fragment. Dots collapse to hyphens for the common dotted-namespace case;
// other disallowed characters fall back to underscore. Empty input or
// empty output is a programmer error.
//
// The mapping is lossy: "send.email" and "send-email" both yield
// "send-email". Callers that adopt a consumer by name must verify the
// adopted filter subject rather than trusting the name alone — see
// NameFor's contract.
func Sanitize(s string) string {
	if s == "" {
		panic("consumername.Sanitize: input must not be empty")
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-',
			c == '_':
			out = append(out, c)
		case c == '.':
			out = append(out, '-')
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		panic("consumername.Sanitize: result must not be empty")
	}
	return string(out)
}

// NameFor produces the durable consumer name for a (taskType, group)
// pair. group=="" means the default branch. Both inputs are sanitized via
// Sanitize before being concatenated under the "workers-" prefix.
// The "workers-" prefix is reserved for dagnats-managed consumers.
//
// Not injective: distinct (taskType, group) pairs can collide on one name
// because Sanitize is lossy. A name alone therefore does not identify the
// subjects a consumer serves; pair it with FilterFor when adopting.
func NameFor(taskType, group string) string {
	if taskType == "" {
		panic("consumername.NameFor: taskType must not be empty")
	}
	if group == "" {
		out := "workers-" + Sanitize(taskType)
		if out == "" {
			panic("consumername.NameFor: result must not be empty")
		}
		return out
	}
	out := "workers-" + Sanitize(taskType) + "-" + Sanitize(group)
	if out == "" {
		panic("consumername.NameFor: result must not be empty")
	}
	return out
}

// FilterFor produces the filter subject for a (taskType, group) pair.
// Inputs are NOT sanitized — they appear in the message-subject hierarchy
// and must round-trip exactly. Subject validity is the publisher's
// contract; sanitization is a consumer-naming concern.
func FilterFor(taskType, group string) string {
	if taskType == "" {
		panic("consumername.FilterFor: taskType must not be empty")
	}
	if group == "" {
		out := "task." + taskType + ".>"
		if out == "" {
			panic("consumername.FilterFor: result must not be empty")
		}
		return out
	}
	out := "task." + taskType + "." + group + ".>"
	if out == "" {
		panic("consumername.FilterFor: result must not be empty")
	}
	return out
}

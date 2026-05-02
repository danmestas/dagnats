// worker/consumer_naming.go
// Naming convention for dagnats-managed JetStream consumers on TASK_QUEUES.
// All durable names live under the "workers-" prefix; sanitization maps
// task-type/group strings to NATS-legal name fragments.
package worker

import "time"

// defaultAckWait bounds the longest expected task duration plus a margin.
// Workers running tasks longer than this should call msg.InProgress()
// periodically (see ADR-008) or override at handler registration via
// WithAckWait (deferred follow-up, see ADR-006 §1).
const defaultAckWait = 5 * time.Minute

// sanitizeConsumerName maps a task-type or group string to a NATS-legal
// consumer-name fragment. Dots collapse to hyphens for the common
// dotted-namespace case; other disallowed characters fall back to
// underscore. Empty input or empty output is a programmer error.
func sanitizeConsumerName(s string) string {
	if s == "" {
		panic("sanitizeConsumerName: input must not be empty")
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
		panic("sanitizeConsumerName: result must not be empty")
	}
	return string(out)
}

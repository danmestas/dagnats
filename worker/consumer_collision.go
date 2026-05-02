// worker/consumer_collision.go
// Registration-time precheck: refuse to start if any (taskType, group) pair
// in the worker's configured handlers collides on the durable consumer name
// after sanitization. Catches cases like "render.gpu" + "render-gpu" before
// they corrupt NATS state via CreateOrUpdateConsumer.
package worker

import "fmt"

// origin records the (taskType, group) pair that produced a given durable.
// Carrying both lets the panic message name the originals — actionable for
// the operator, who needs to rename one.
type origin struct {
	taskType string
	group    string
}

// assertNoConsumerNameCollisions enumerates every durable consumer name this
// worker would create and panics if any two distinct origins collide on the
// same name. Pure: takes the in-memory handler map and groups slice, no NATS.
//
// groups==nil or empty groups means the default branch (one durable per
// taskType). Otherwise the cross-product of (taskType x group) is enumerated.
func assertNoConsumerNameCollisions(
	handlers map[string]HandlerFunc, groups []string,
) {
	if handlers == nil {
		panic("assertNoConsumerNameCollisions: handlers must not be nil")
	}

	seen := make(map[string]origin, len(handlers))

	if len(groups) == 0 {
		for taskType := range handlers {
			name := consumerNameFor(taskType, "")
			if prior, exists := seen[name]; exists {
				panic(fmt.Sprintf(
					"dagnats: task types %q and %q both produce durable %q — rename one",
					prior.taskType, taskType, name,
				))
			}
			seen[name] = origin{taskType: taskType}
		}
		return
	}

	for taskType := range handlers {
		for _, group := range groups {
			name := consumerNameFor(taskType, group)
			if prior, exists := seen[name]; exists {
				panic(fmt.Sprintf(
					"dagnats: (task=%q,group=%q) and (task=%q,group=%q) both produce durable %q — rename one",
					prior.taskType, prior.group, taskType, group, name,
				))
			}
			seen[name] = origin{taskType: taskType, group: group}
		}
	}
}

// worker/consumer_collision_xprocess.go
// Cross-process precheck (ADR-010): catches the case where two workers in
// different processes registered different task types whose sanitized durable
// names collide. Worker A registered first; this helper runs in Worker B's
// subscribePullConsumer before CreateOrUpdateConsumer, sees the existing
// durable with our name but a different FilterSubject, and panics with both
// filters named so the operator knows which task types to rename.
//
// Companion to assertNoConsumerNameCollisions (consumer_collision.go), which
// catches the same collision class in the same Worker. This file is the
// NATS-aware twin that catches the cross-process variant.
package worker

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// assertNoCrossProcessCollision panics if TASK_QUEUES already holds a
// consumer with the given durable name but a different filter subject.
// Idempotency: same name + same filter is fine — that's the steady-state
// case where another worker (or our prior incarnation) already created the
// shared durable. Different filter means another process registered a
// different task type that sanitized to the same durable — silent routing
// corruption follows if we proceed.
//
// Cost: one ListConsumers iteration per call. Bounded at typical N (≤50
// consumers); revisit if N grows. See ADR-010 "Consolidation path."
func assertNoCrossProcessCollision(
	ctx context.Context, js jetstream.JetStream, filter, durable string,
) {
	if filter == "" {
		panic("assertNoCrossProcessCollision: filter must not be empty")
	}
	if durable == "" {
		panic("assertNoCrossProcessCollision: durable must not be empty")
	}

	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		panic("assertNoCrossProcessCollision: Stream: " + err.Error())
	}

	iter := stream.ListConsumers(ctx)
	for info := range iter.Info() {
		if info.Config.Durable != durable {
			continue
		}
		if info.Config.FilterSubject == filter {
			// Same durable, same filter: idempotent reuse. Nothing to do.
			continue
		}
		panic(fmt.Sprintf(
			"dagnats: cross-process consumer collision on TASK_QUEUES — "+
				"durable %q already owned with FilterSubject %q, "+
				"this worker would claim FilterSubject %q. Rename one task "+
				"type so their sanitized durable names differ.",
			durable, info.Config.FilterSubject, filter,
		))
	}
	if err := iter.Err(); err != nil {
		panic("assertNoCrossProcessCollision: iterator: " + err.Error())
	}
}

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
	"errors"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/nats-io/nats.go/jetstream"
)

// assertNoCrossProcessCollision panics if TASK_QUEUES already holds a
// consumer with the given durable name but a different filter subject —
// UNLESS the existing filter is exactly the pre-#674 ">"-anchored twin of
// filter (consumername.FilterIsLegacyUpgrade), in which case it is a
// durable a not-yet-upgraded sibling process created for the SAME task
// type/group, not a genuine collision. That case is an in-place upgrade:
// FilterSubject is immutable on an existing JetStream consumer, so
// CreateOrUpdateConsumer cannot rewrite it — this deletes the stale
// durable so the caller's subsequent CreateOrUpdateConsumer recreates it
// with the new anchor, and logs once at warn so the upgrade is visible.
//
// Idempotency: same name + same filter is fine — that's the steady-state
// case where another worker (or our prior incarnation) already created the
// shared durable. A different filter that is NOT the legacy twin means
// another process registered a different task type that sanitized to the
// same durable — silent routing corruption follows if we proceed.
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
		if consumername.FilterIsLegacyUpgrade(filter, info.Config.FilterSubject) {
			upgradeLegacyDurable(ctx, stream, durable, filter)
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

// upgradeLegacyDurable deletes durable so the caller's subsequent
// CreateOrUpdateConsumer recreates it with the new "*"-anchored filter.
//
// Deliberately does NOT trust the ListConsumers snapshot that led here:
// two sibling processes can both observe the same stale legacy durable
// and both start upgrading it concurrently. If B blindly deleted on the
// strength of its snapshot, it could delete the durable AFTER A already
// recreated it — killing A's live, freshly upgraded consumer instead of
// the stale one either of them meant to replace. Re-fetching the
// durable's CURRENT FilterSubject immediately before deleting closes
// that window down to the single DeleteConsumer call: if another
// process already finished the upgrade (current == filter) or the
// durable is simply gone (a sibling deleted it and hasn't recreated it
// yet), this is a no-op; only a durable STILL carrying the legacy
// filter at the moment of the re-check gets deleted.
func upgradeLegacyDurable(
	ctx context.Context, stream jetstream.Stream, durable, filter string,
) {
	if durable == "" {
		panic("upgradeLegacyDurable: durable must not be empty")
	}
	if filter == "" {
		panic("upgradeLegacyDurable: filter must not be empty")
	}

	cons, err := stream.Consumer(ctx, durable)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			// A sibling already deleted it; nothing left for us to do.
			return
		}
		panic(
			"upgradeLegacyDurable: Consumer lookup for " + durable +
				": " + err.Error(),
		)
	}
	info := cons.CachedInfo()
	if info == nil {
		panic(
			"upgradeLegacyDurable: Consumer " + durable +
				" returned no cached config",
		)
	}
	current := info.Config.FilterSubject
	if current == filter {
		// A sibling already completed the upgrade. Idempotent no-op.
		return
	}
	if !consumername.FilterIsLegacyUpgrade(filter, current) {
		// The durable no longer matches the legacy shape we
		// snapshotted — something else changed it between our scan and
		// now. Don't delete state we no longer understand; leave it for
		// the caller's CreateOrUpdateConsumer (or the next
		// assertNoCrossProcessCollision pass) to react to.
		return
	}

	slog.Warn(
		"auto-upgrading legacy consumer filter (issue #674)",
		"durable", durable,
		"old_filter", current,
		"new_filter", filter,
	)
	if err := stream.DeleteConsumer(ctx, durable); err != nil &&
		!errors.Is(err, jetstream.ErrConsumerNotFound) {
		panic(
			"upgradeLegacyDurable: DeleteConsumer for legacy upgrade " +
				"of " + durable + ": " + err.Error(),
		)
	}
}

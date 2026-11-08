package trigger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// claimAttemptsMax bounds the Create/Update retry loop in claimCluster. A
// revision-mismatch means another node won, so one retry (Create lost to
// a concurrent Create, then Update) is all a correct run can need.
const claimAttemptsMax = 3

// claimKey is the per-trigger cluster claim. It sits beside
// <id>.last_run_at in trigger_state and holds the last fired minute
// bucket (unix/60) as decimal text.
func claimKey(triggerID string) string {
	return triggerID + ".cron_claim"
}

// claimCluster atomically claims `minute` for this node across the
// whole cluster. Every node runs a Scheduler, and the only other
// cross-node guard is the workflow.started Nats-Msg-Id, which the
// WORKFLOW_HISTORY stream remembers for just 5 s; ticks 18 s apart in
// one minute therefore both started a run. KV Create/Update is
// compare-and-set, so exactly one node wins a given minute no matter
// how far apart the ticks land.
//
// Returns won=true with the claim's revision when this node should
// fire. won=false, err=nil means the minute (or a newer one) is already
// claimed. A non-nil err means the claim state is unknown.
func (s *Scheduler) claimCluster(
	ctx context.Context, triggerID string, minute time.Time,
) (bool, uint64, error) {
	if triggerID == "" {
		panic("claimCluster: triggerID must not be empty")
	}
	if minute.IsZero() {
		panic("claimCluster: minute must not be zero")
	}
	bucket := minute.Unix() / 60
	value := []byte(strconv.FormatInt(bucket, 10))
	key := claimKey(triggerID)

	for attempt := 0; attempt < claimAttemptsMax; attempt++ {
		entry, err := s.stateKV.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			rev, createErr := s.stateKV.Create(ctx, key, value)
			if createErr == nil {
				return true, rev, nil
			}
			if isRevisionConflict(createErr) {
				continue
			}
			return false, 0, fmt.Errorf("claim create: %w", createErr)
		}
		if err != nil {
			return false, 0, fmt.Errorf("claim get: %w", err)
		}
		held, err := strconv.ParseInt(string(entry.Value()), 10, 64)
		if err == nil && held >= bucket {
			return false, 0, nil
		}
		// An unparseable value is overwritten: a corrupt claim must not
		// silence the trigger forever.
		rev, err := s.stateKV.Update(ctx, key, value, entry.Revision())
		if err == nil {
			return true, rev, nil
		}
		if !isRevisionConflict(err) {
			return false, 0, fmt.Errorf("claim update: %w", err)
		}
	}
	// Still conflicting after repeated tries: others are winning.
	return false, 0, nil
}

// isRevisionConflict reports that a compare-and-set lost to another
// writer (key already exists on Create, wrong last sequence on Update).
func isRevisionConflict(err error) bool {
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) &&
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}

// tryClaim decides whether this node fires `minute` for the trigger.
//
// The in-process check runs first; it only saves a KV round trip on
// repeat ticks and is recorded only after the cluster outcome is known.
// Losing the cluster claim also records it (the cluster already holds
// this minute or newer, so re-asking is wasted).
//
// On a KV error the tick is skipped, not fired: firing blind could
// double a run another node already claimed, and nothing is recorded
// locally, so the next tick in the same minute (the scheduler ticks
// every 30 s) retries the claim. The error is logged and returned.
func (s *Scheduler) tryClaim(
	ctx context.Context, triggerID string, minute time.Time,
) (bool, uint64, error) {
	if triggerID == "" {
		panic("tryClaim: triggerID must not be empty")
	}
	if minute.IsZero() {
		panic("tryClaim: minute must not be zero")
	}
	minute = minute.Truncate(time.Minute)
	if s.claimedLocally(triggerID, minute) {
		return false, 0, nil
	}
	won, rev, err := s.claimCluster(ctx, triggerID, minute)
	if err != nil {
		slog.Warn("cron: cluster claim failed, skipping tick",
			"trigger_id", triggerID, "minute", minute, "error", err)
		return false, 0, fmt.Errorf("cluster claim %q: %w", triggerID, err)
	}
	s.markLocally(triggerID, minute)
	return won, rev, nil
}

// claimedLocally reports whether this process already settled `minute`
// (or a later one) for the trigger.
func (s *Scheduler) claimedLocally(triggerID string, minute time.Time) bool {
	s.lastFiredMu.Lock()
	defer s.lastFiredMu.Unlock()
	return !minute.After(s.lastFired[triggerID])
}

func (s *Scheduler) markLocally(triggerID string, minute time.Time) {
	s.lastFiredMu.Lock()
	defer s.lastFiredMu.Unlock()
	if minute.After(s.lastFired[triggerID]) {
		s.lastFired[triggerID] = minute
	}
}

// releaseClaim undoes a won claim whose fire failed, so another node
// (or this one on its next tick) can still start the run instead of the
// minute being lost. Best effort: a failed release is logged; the
// revision guard stops it deleting a claim another node has since taken.
func (s *Scheduler) releaseClaim(
	ctx context.Context, triggerID string, rev uint64,
) {
	if triggerID == "" {
		panic("releaseClaim: triggerID must not be empty")
	}
	if rev == 0 {
		panic("releaseClaim: rev must be non-zero")
	}
	err := s.stateKV.Delete(ctx, claimKey(triggerID), jetstream.LastRevision(rev))
	if err != nil {
		slog.Warn("cron: release claim failed",
			"trigger_id", triggerID, "error", err)
	}
	s.lastFiredMu.Lock()
	delete(s.lastFired, triggerID)
	s.lastFiredMu.Unlock()
}

// claimAndFire is the single path both Tick and Backfill use to start a
// cron run for `minute`: cluster claim, then fire, releasing the claim
// if the fire fails.
func (s *Scheduler) claimAndFire(
	ctx context.Context, def TriggerDef, minute time.Time,
) error {
	won, rev, err := s.tryClaim(ctx, def.ID, minute)
	if err != nil || !won {
		return err
	}
	if err := s.fireWorkflow(ctx, def, minute); err != nil {
		s.releaseClaim(ctx, def.ID, rev)
		return err
	}
	return nil
}

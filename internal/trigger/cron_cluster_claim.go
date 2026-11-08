package trigger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
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

// fireFunc is the Fire signature, so tests can inject publish failures.
type fireFunc func(
	context.Context, *natsutil.TracingPublisher, TriggerDef, string,
	time.Time,
) (string, error)

// Fire retries stay inside WORKFLOW_HISTORY's 5 s duplicate window,
// measured from the first attempt.
const (
	fireAttemptsMax  = 3
	fireRetryBackoff = 200 * time.Millisecond
	fireRetryWindow  = 4 * time.Second
)

// claimAndFire is the single path both Tick and Backfill use to start a
// cron run for `minute`: cluster claim, then fire with bounded retry.
//
// The claim is never released. A failed Fire is ambiguous: the publish
// may have landed with only its ack lost, and releasing would let
// another node fire ~30 s later, past the 5 s duplicate window, which
// is the double fire this claim exists to stop. A visible lost fire
// (ERROR log plus returned error) beats a silent double fire. Retries
// reuse `minute`, hence the same Nats-Msg-Id trigger.<id>.<unix/60>
// (startedDedupID takes only id, source and minute), so a retry after a
// publish that did land is dropped by JetStream as a duplicate.
func (s *Scheduler) claimAndFire(
	ctx context.Context, def TriggerDef, minute time.Time,
) error {
	won, _, err := s.tryClaim(ctx, def.ID, minute)
	if err != nil || !won {
		return err
	}
	started := time.Now()
	for attempt := 1; ; attempt++ {
		err = s.fireWorkflow(ctx, def, minute)
		if err == nil {
			return nil
		}
		if attempt >= fireAttemptsMax ||
			time.Since(started)+fireRetryBackoff > fireRetryWindow {
			break
		}
		time.Sleep(fireRetryBackoff * time.Duration(attempt))
	}
	slog.Error("cron fire lost for minute",
		"trigger_id", def.ID, "minute", minute.Truncate(time.Minute),
		"error", err)
	return fmt.Errorf("cron fire lost for minute %s: %w",
		minute.Truncate(time.Minute).Format(time.RFC3339), err)
}

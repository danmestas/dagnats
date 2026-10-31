// bridge/restart_limit.go
// The crash-loop guard of ADR-028 for bridge workers. The Go SDK
// fails a journal step whose deliveries keep claiming one iteration
// without progress (worker/restart_limit.go); a non-Go worker gets
// the same rule here, at its journal claim: the claim that finds
// restart-limit claims already sitting at the journal tail with no
// progress between them fails the step with reason restart_limit and
// the worker sees 410 {"code":"restart_limit"}. The limit is the
// bridge's (default worker.RestartLimitDefault), set with
// SetRestartLimit or DAGNATS_BRIDGE_RESTART_LIMIT, because a bridge
// worker has no registration to carry a per-worker option.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/jetstream"
)

// restartLimitEnv names the environment variable NewBridge reads the
// restart limit from.
const restartLimitEnv = "DAGNATS_BRIDGE_RESTART_LIMIT"

// SetRestartLimit sets how many deliveries may claim one iteration of
// a journal step without progress before the step fails with reason
// restart_limit, as worker.WithRestartLimit does for Go workers. It
// is safe to call while the bridge serves; a claim in flight uses the
// old value. Panics unless
// 1 <= n <= worker.RestartLimitMax.
func (b *Bridge) SetRestartLimit(n int) {
	if n < 1 {
		panic("SetRestartLimit: n must be >= 1")
	}
	if n > worker.RestartLimitMax {
		panic("SetRestartLimit: n must be <= worker.RestartLimitMax")
	}
	b.restartLimit.Store(int32(n))
}

// restartLimitFromEnv reads restartLimitEnv; an unset value yields the
// default, and an invalid one the default with a warning (unlike
// SetRestartLimit, which panics: the environment is operator input).
func restartLimitFromEnv() int {
	raw := os.Getenv(restartLimitEnv)
	if raw == "" {
		return worker.RestartLimitDefault
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > worker.RestartLimitMax {
		slog.Warn("bridge restart limit ignored: want an integer in [1, 1000]",
			"env", restartLimitEnv, "value", raw,
			"default", worker.RestartLimitDefault)
		return worker.RestartLimitDefault
	}
	return n
}

// failRestartLimit fails the step when the journal j just claimed
// shows the limit's worth of claims without progress before it. It
// returns journal.ErrRestartLimit once step.failed is published and
// the task is settled, and nil when the worker may proceed. The
// caller holds the task's journal lock, so the commit goes straight
// to j rather than through commitJournal.
func (b *Bridge) failRestartLimit(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg, j *journal.Journal,
) error {
	if j == nil {
		panic("failRestartLimit: journal must be open")
	}
	limit := int(b.restartLimit.Load())
	if limit < 1 {
		panic("failRestartLimit: restart limit must be positive")
	}
	if j.Restarts() < limit {
		return nil
	}
	reason := fmt.Sprintf("%s: %d deliveries claimed iteration %d without progress",
		worker.ReasonRestartLimit, j.Restarts(), j.Task().Iteration)
	slog.WarnContext(ctx, "journal step hit its restart limit; failing it",
		"task_id", taskID, "iteration", j.Task().Iteration,
		"restarts", j.Restarts(), "limit", limit)
	failureType := protocol.FailureTypeRetriable
	seq, err := j.Commit(ctx, journal.Commit{
		Action: journal.ActionFail, Error: reason, FailureType: string(failureType),
	})
	switch {
	case errors.Is(err, journal.ErrFenced):
		return b.settleJournalStop(ctx, taskID, tokenID, msg, err)
	case errors.Is(err, journal.ErrCommitted):
		return b.finishCommitted(ctx, taskID, tokenID, msg, j)
	case err != nil:
		return fmt.Errorf("journal commit: %w", err)
	}
	return b.publishRestartLimit(ctx, taskID, tokenID, msg, j, seq, reason)
}

// publishRestartLimit announces the committed failure, acknowledges the
// task and forgets it, so the worker's later calls answer "committed".
func (b *Bridge) publishRestartLimit(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg,
	j *journal.Journal, seq uint64, reason string,
) error {
	payload, err := json.Marshal(protocol.StepFailedPayload{
		Error: reason, FailureType: protocol.FailureTypeRetriable,
	})
	if err != nil {
		return fmt.Errorf("marshal fail payload: %w", err)
	}
	task := j.Task()
	runID, stepID := splitTaskID(taskID)
	evt := protocol.NewStepEvent(protocol.EventStepFailed, runID, stepID, payload)
	evt.SetIdentity(task.Iteration, task.Attempt, task.DispatchNonce)
	evt.JournalSeq = seq
	b.emitLogMarker(ctx, taskID, msg, protocol.LogMarkerFailed)
	if err := b.publishEvent(ctx, evt); err != nil {
		return fmt.Errorf("publish fail event: %w", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	b.ackMap.Delete(taskID)
	b.ackMap.MarkResolved(taskID, tokenID)
	return journal.ErrRestartLimit
}

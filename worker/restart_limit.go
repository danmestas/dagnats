// worker/restart_limit.go
// The crash-loop guard of ADR-028: a journal step whose deliveries
// keep claiming one iteration without progress is failed with reason
// restart_limit instead of being redelivered forever. The journal
// counts the claims (journal.Restarts); this file applies the limit
// and fails the step through the ordinary Fail path, so the failure
// is committed to the journal, published to the engine and retried by
// the step's retry policy, and the retry resumes from the journal.
package worker

import (
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/journal"
)

// RestartLimitDefault is the number of consecutive claims on one
// iteration without progress after which a journal step fails.
const RestartLimitDefault = 5

// restartLimitMax bounds the option: a limit past it is a
// configuration error, not a policy.
const restartLimitMax = 1000

// ReasonRestartLimit prefixes the error of a step the guard failed.
const ReasonRestartLimit = "restart_limit"

// WithRestartLimit sets how many deliveries may claim one iteration of
// a journal step without progress before the step fails with reason
// restart_limit (ADR-028). A pause counts as progress. Default 5.
func WithRestartLimit(n int) WorkerOption {
	if n < 1 {
		panic("WithRestartLimit: n must be >= 1")
	}
	if n > restartLimitMax {
		panic("WithRestartLimit: n must be <= 1000")
	}
	return func(w *Worker) { w.restartLimit = n }
}

// failRestartLimit fails the step when the journal j just claimed
// shows the limit's worth of claims without progress before it. It
// returns journal.ErrRestartLimit once step.failed is published, and
// nil when the handler may run.
func (c *taskContext) failRestartLimit(j *journal.Journal) error {
	if j == nil {
		panic("failRestartLimit: journal must be open")
	}
	if c.journalStop != journalLive {
		panic("failRestartLimit: task already stopped")
	}
	limit := c.restartLimit
	if limit <= 0 {
		limit = RestartLimitDefault
	}
	if j.Restarts() < limit {
		return nil
	}
	reason := fmt.Errorf("%s: %d deliveries claimed iteration %d without progress",
		ReasonRestartLimit, j.Restarts(), j.Task().Iteration)
	slog.WarnContext(c.ctx, "journal step hit its restart limit; failing it",
		"run_id", c.runID, "step_id", c.stepID, "iteration", j.Task().Iteration,
		"attempt", j.Task().Attempt, "restarts", j.Restarts(), "limit", limit)
	if err := c.Fail(reason); err != nil {
		return err
	}
	c.journalStop = journalRestartLimit
	return journal.ErrRestartLimit
}

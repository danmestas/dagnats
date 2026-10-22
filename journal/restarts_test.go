// journal/restarts_test.go
// The crash-loop guard's count (ADR-028, #807): Restarts is the number
// of claims on the iteration at the tail with nothing between them.
// Redeliveries that claim and die raise it; a pause, an entry, a tool
// record, a commit, or an iteration boundary resets it.
//
// Methodology: one embedded server, the public Open surface, modelled
// as the engine delivers: the attempt is stamped on every dispatch and
// only the delivery count moves between deliveries. Every wait is
// bounded.
package journal

import (
	"context"
	"testing"
	"time"
)

func TestRestarts_CountsClaimsWithoutProgress(t *testing.T) {
	forEachDispatchSingle(t, func(t *testing.T, c client, runID string, d dispatch) {
		ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
		defer cancel()
		// The attempt is the stamped one on every delivery.
		attempt := func(int) int { return d.first }
		var j *Journal
		for delivered := 1; delivered <= 4; delivered++ {
			j = open(t, c, task(runID, d.iteration, attempt(delivered), delivered))
			if j.Restarts() != delivered-1 {
				t.Errorf("delivery %d: Restarts = %d, want %d", delivered, j.Restarts(), delivered-1)
			}
		}
		if _, err := j.Pause(ctx, "cooldown", time.Second); err != nil {
			t.Fatalf("Pause: %v", err)
		}
		j = open(t, c, task(runID, d.iteration, attempt(5), 5))
		if j.Restarts() != 0 {
			t.Errorf("after a pause: Restarts = %d, want 0: a pause is progress", j.Restarts())
		}
		if _, err := j.Append(ctx, TypeEntry, []byte(`"work"`)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		j = open(t, c, task(runID, d.iteration, attempt(6), 6))
		if j.Restarts() != 0 {
			t.Errorf("after an entry: Restarts = %d, want 0", j.Restarts())
		}
		if _, err := j.Commit(ctx, Commit{Action: ActionFail, Error: "restart_limit"}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		retry := open(t, c, task(runID, d.iteration, attempt(6)+1, 7))
		if retry.Restarts() != 0 {
			t.Errorf("retry after a commit: Restarts = %d, want 0", retry.Restarts())
		}
	})
}

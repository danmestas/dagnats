// worker/chaos_cost_test.go
// The crash seam costs production nothing: with no hook installed a
// taskContext boundary must not build a Site (which reads the
// message's metadata and allocates) or allocate at all.
//
// Methodology: a taskContext built from a payload the way the Worker
// builds one, against an embedded NATS server; AllocsPerRun over the
// boundary with no hook, then one call with a hook to prove the same
// boundary does reach it.
package worker

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/internal/chaos"
)

func TestCrashAt_NoHookDoesNotAllocate(t *testing.T) {
	chaos.Install(nil)
	nc, _ := journalServer(t)
	tc := delivery(t, nc, journalPayload("run-chaos-cost", 1), 1)
	allocs := testing.AllocsPerRun(1000, func() {
		if err := tc.crashAt(chaos.WorkerAfterClaim); err != nil {
			t.Fatalf("crashAt with no hook: %v", err)
		}
	})
	if allocs != 0 {
		t.Errorf("crashAt with no hook allocates %.1f times per call, want 0", allocs)
	}
	reached := false
	chaos.Install(func(_ context.Context, p chaos.Point, site chaos.Site) error {
		reached = p == chaos.WorkerAfterClaim && site.RunID == "run-chaos-cost" &&
			site.Iteration == 1 && site.Delivered == 1
		return chaos.ErrKilled
	})
	t.Cleanup(func() { chaos.Install(nil) })
	if err := tc.crashAt(chaos.WorkerAfterClaim); !chaos.Killed(err) {
		t.Fatalf("crashAt with a killing hook: err = %v, want ErrKilled", err)
	}
	if !reached || tc.journalStop != journalKilled {
		t.Errorf("hook reached = %v, stop = %d; want the site and journalKilled", reached, tc.journalStop)
	}
}

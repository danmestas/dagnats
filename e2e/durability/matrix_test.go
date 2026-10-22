// e2e/durability/matrix_test.go
// The crash-point matrix (#809): the scripted agent runs once per
// crash point, killed or stalled past AckWait there on the first
// delivery of its unsafe iteration (engine points: on the first
// dispatch that reaches them). After recovery the run must complete
// with the crash-free answer, the journal audit must pass and the
// unsafe tool must have started at most once. A stall is the zombie
// resuming after its successor took over, so it also proves the
// fence. Engine stalls are not in the matrix: the history consumer's
// AckWait is 30s and not configurable, so a stall there is a delay,
// not a takeover.
//
// Methodology: the shared fixture of harness_test.go; one baseline
// run, then one subtest per (point, mode) running in parallel against
// the same engines and workers, each routed its own crash by run ID.
// Every wait is bounded.
package durability

import (
	"testing"

	"github.com/danmestas/dagnats/internal/chaos"
)

func TestCrashPointMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("crash-point matrix skipped in -short mode")
	}
	f := newFixture(t, false)
	r := &router{}
	r.install(t)
	baselineID := f.startRun(t)
	baseline := f.waitCompleted(t, baselineID)
	f.audit(t, baselineID)
	if len(baseline.Calls) != len(script)-1 || baseline.Final != "done" {
		t.Fatalf("baseline answer = %+v, want %d calls and final done", baseline, len(script)-1)
	}
	for _, point := range chaos.Points() {
		for _, kill := range []bool{true, false} {
			if !kill && point >= chaos.EngineEnqueueSaved {
				continue
			}
			name := point.String() + "/" + mode(kill)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				runCrashPoint(t, f, r, baseline, point, kill)
			})
		}
	}
}

// mode names a hook's action.
func mode(kill bool) string {
	if kill {
		return "kill"
	}
	return "stall"
}

// runCrashPoint runs one agent run crashed at point and audits it.
func runCrashPoint(
	t *testing.T, f *fixture, r *router, baseline answer, point chaos.Point, kill bool,
) {
	t.Helper()
	if baseline.Final == "" {
		panic("runCrashPoint: baseline must be complete")
	}
	hook := &crashHook{point: point, kill: kill}
	runID := f.startRunRouted(t, r, hook)
	got := f.waitCompleted(t, runID)
	if hook.count != 1 {
		t.Errorf("hook at %s fired %d times, want exactly once", point, hook.count)
	}
	if !sameAnswer(baseline, got) {
		t.Errorf("answer after %s/%s = %+v, want %+v", point, mode(kill), got, baseline)
	}
	f.audit(t, runID)
}

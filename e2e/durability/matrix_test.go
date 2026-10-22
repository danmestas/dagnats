// e2e/durability/matrix_test.go
// The crash-point matrix (#809): the scripted agent runs once per
// crash point, killed or stalled past AckWait there on the first
// handling of its unsafe iteration (engine points: on the first
// dispatch that reaches them). After recovery the run must complete
// with the crash-free answer, the journal audit must pass and the
// unsafe tool must have started at most once. A stall is the zombie
// resuming after its successor took over, so it also proves the
// fence. Engine stalls are not in the matrix: the history consumer's
// AckWait is 30s and not configurable, so a stall there is a delay,
// not a takeover.
//
// Methodology: the shared fixture of harness_test.go with two engines
// sharing the history durable (DAGNATS_DURABILITY_ENGINES overrides,
// 1..4); one baseline run, then one subtest per cell running in
// parallel against it, each on its own agent (task type and workers)
// so a stall in one cell cannot age another cell's prefetched
// deliveries, and each routed its own crash by run ID. Every wait is
// bounded.
package durability

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/danmestas/dagnats/internal/chaos"
)

func TestCrashPointMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("crash-point matrix skipped in -short mode")
	}
	f := newFixture(t, fixtureOptions{engines: matrixEngines(t)})
	r := &router{}
	r.install(t)
	baselineID := f.newAgent(t).startRun(t)
	baseline := f.waitCompleted(t, baselineID)
	f.audit(t, baselineID)
	if len(baseline.Calls) != len(script)-1 || baseline.Final != "done" {
		t.Fatalf("baseline answer = %+v, want %d calls and final done", baseline, len(script)-1)
	}
	for _, c := range matrixCases() {
		t.Run(c.String(), func(t *testing.T) {
			t.Parallel()
			runCrashPoint(t, f, r, baseline, c)
		})
	}
}

// matrixCase is one cell: a point, kill or stall, and, for worker
// points, the iteration crashed.
type matrixCase struct {
	point     chaos.Point
	kill      bool
	iteration int
}

// String names the cell.
func (c matrixCase) String() string {
	if c.point <= 0 {
		panic("matrixCase: point must be set")
	}
	if c.point >= chaos.EngineEnqueueSaved {
		return c.point.String() + "/" + mode(c.kill)
	}
	return c.point.String() + "/" + mode(c.kill) + "/iteration" + fmt.Sprint(c.iteration)
}

// matrixCases lists every cell: worker points killed and stalled on
// iteration 0 (the first dispatch, whose stamped attempt ADR-026's
// finding 1 added), on the unsafe iteration and on the safe iteration
// after it; engine points killed on their first reach. The safe
// iteration proves a safe call interrupted by a crash is rerun, never
// answered as interrupted.
func matrixCases() []matrixCase {
	iterations := []int{0, unsafeIteration, safeIteration}
	cases := make([]matrixCase, 0, len(chaos.Points())*2*len(iterations))
	for _, point := range chaos.Points() {
		if point >= chaos.EngineEnqueueSaved {
			cases = append(cases, matrixCase{point: point, kill: true})
			continue
		}
		for _, kill := range []bool{true, false} {
			for _, iteration := range iterations {
				cases = append(cases, matrixCase{point: point, kill: kill, iteration: iteration})
			}
		}
	}
	if len(cases) == 0 {
		panic("matrixCases: no cases")
	}
	return cases
}

// matrixEngines is how many orchestrators the matrix runs: two by
// default, as a cluster shares the history durable;
// DAGNATS_DURABILITY_ENGINES=1..4 overrides it when bisecting.
func matrixEngines(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("DAGNATS_DURABILITY_ENGINES")
	if raw == "" {
		return 2
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 4 {
		t.Fatalf("DAGNATS_DURABILITY_ENGINES=%q: want 1..4", raw)
	}
	return n
}

// mode names a hook's action.
func mode(kill bool) string {
	if kill {
		return "kill"
	}
	return "stall"
}

// runCrashPoint runs one agent run crashed at point and audits it.
func runCrashPoint(t *testing.T, f *fixture, r *router, baseline answer, c matrixCase) {
	t.Helper()
	if baseline.Final == "" {
		panic("runCrashPoint: baseline must be complete")
	}
	hook := &crashHook{point: c.point, kill: c.kill, iteration: c.iteration}
	runID := f.newAgent(t).startRunRouted(t, r, hook.fire)
	got, ok := f.waitCompletedOK(runID)
	if !ok {
		f.dumpRun(t, runID)
		t.Fatalf("run %s did not complete within %s after %s", runID, runTimeout, c)
	}
	if n := hook.count.Load(); n != 1 {
		t.Errorf("hook at %s fired %d times, want exactly once", c, n)
	}
	crashed := map[chaos.Point]int{}
	if c.point < chaos.EngineEnqueueSaved {
		crashed[c.point] = c.iteration
	}
	if want := expectedAnswer(baseline, crashed); !sameAnswer(want, got) {
		t.Errorf("answer after %s = %+v, want %+v", c, got, want)
	}
	if !f.auditOK(t, runID, c.String()) {
		f.dumpRun(t, runID)
	}
}

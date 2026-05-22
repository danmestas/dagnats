// cli/demo_test.go
// Integration tests for the demo-seed noop worker.
// Methodology: spin a real embedded NATS server + orchestrator + API
// via dagnatstest.NewHarness (worker is intentionally not started by
// the harness — runDemoSeed brings up its own), then call runDemoSeed
// directly so we exercise the same code path the CLI hits, just
// without the os.Args dispatch. Distribution assertions use the
// deterministic RNG seed exposed via demoSeedOptions.seed. The full
// terminal-state wait is bounded at 10s in tests (more generous than
// the CLI default's 5s because CI runners can be slow) so a hang
// surfaces as a t.Fatal rather than a goroutine leak.
package cli

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
)

// TestDemoSeedProducesTerminalDistribution exercises the canonical
// 70 / 20 / 10 distribution and proves no run gets stuck in running
// past the wait timeout. Deterministic seed keeps tolerance tight
// and removes flake risk.
func TestDemoSeedProducesTerminalDistribution(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)

	const seedRuns = 100
	res, err := runDemoSeed(h.Svc, h.NC, demoSeedOptions{
		count:       seedRuns,
		seed:        42, // deterministic
		waitTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("runDemoSeed: %v", err)
	}

	if res.Total() != seedRuns {
		t.Fatalf("Total = %d, want %d (counts: %+v)",
			res.Total(), seedRuns, res)
	}
	if res.Stuck != 0 {
		t.Fatalf("Stuck = %d, want 0 (counts: %+v)",
			res.Stuck, res)
	}

	// The deterministic plan generated with seed=42 is fixed; assert
	// the shape we know it produces by recomputing the plan here.
	// This guards against an accidental change in distribution
	// proportions or RNG semantics — if the seed-to-plan mapping
	// changes the test author must update the expectations
	// deliberately.
	plan := planDemoOutcomes(seedRuns, false, 42)
	wantC, wantF, wantX := countPlan(plan)
	if res.Completed != wantC {
		t.Errorf("Completed = %d, want %d (plan: %+v)",
			res.Completed, wantC, res)
	}
	if res.Failed != wantF {
		t.Errorf("Failed = %d, want %d (plan: %+v)",
			res.Failed, wantF, res)
	}
	if res.Cancelled != wantX {
		t.Errorf("Cancelled = %d, want %d (plan: %+v)",
			res.Cancelled, wantX, res)
	}

	// Sanity-check that the seed actually exercises all three
	// branches at this sample size — if any bucket is 0 the test is
	// not proving what it claims to prove. With seed=42 / N=100 the
	// distribution should land near 70 / 20 / 10.
	if wantC == 0 || wantF == 0 || wantX == 0 {
		t.Fatalf("planned distribution has empty bucket: %+v"+
			" — seed/count combination doesn't exercise all paths",
			res)
	}

	// Loose proportion check (±15%) — protects against a future
	// drawOutcome rewrite that breaks the 70/20/10 shape. The exact
	// counts check above gives precision; this gives shape.
	if !inRange(res.Completed, 70, 15) {
		t.Errorf("Completed %d not within ±15 of 70 (%+v)",
			res.Completed, res)
	}
	if !inRange(res.Failed, 20, 15) {
		t.Errorf("Failed %d not within ±15 of 20 (%+v)",
			res.Failed, res)
	}
	if !inRange(res.Cancelled, 10, 15) {
		t.Errorf("Cancelled %d not within ±15 of 10 (%+v)",
			res.Cancelled, res)
	}
}

// TestDemoSeedIncludeFailedFlag verifies that --include-failed
// guarantees at least one failed and one cancelled outcome even
// with a tiny sample size where natural draws might miss them.
func TestDemoSeedIncludeFailedFlag(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)

	const seedRuns = 5
	res, err := runDemoSeed(h.Svc, h.NC, demoSeedOptions{
		count:         seedRuns,
		includeFailed: true,
		seed:          1, // deterministic
		waitTimeout:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("runDemoSeed: %v", err)
	}

	if res.Total() != seedRuns {
		t.Fatalf("Total = %d, want %d (counts: %+v)",
			res.Total(), seedRuns, res)
	}
	if res.Stuck != 0 {
		t.Fatalf("Stuck = %d, want 0 (counts: %+v)",
			res.Stuck, res)
	}
	if res.Failed < 1 {
		t.Errorf("Failed = %d, want >= 1 with --include-failed"+
			" (counts: %+v)", res.Failed, res)
	}
	if res.Cancelled < 1 {
		t.Errorf("Cancelled = %d, want >= 1 with --include-failed"+
			" (counts: %+v)", res.Cancelled, res)
	}
}

// TestPlanDemoOutcomes_Distribution exercises the planner alone
// (no NATS) to lock in the 70/20/10 proportions at a sample size
// large enough to wash out small-sample noise. Pure unit — fast.
func TestPlanDemoOutcomes_Distribution(t *testing.T) {
	t.Parallel()
	plan := planDemoOutcomes(1000, false, 12345)
	c, f, x := countPlan(plan)
	if c+f+x != 1000 {
		t.Fatalf("plan sum = %d, want 1000", c+f+x)
	}
	if !inRange(c, 700, 50) {
		t.Errorf("completed %d not within ±50 of 700", c)
	}
	if !inRange(f, 200, 50) {
		t.Errorf("failed %d not within ±50 of 200", f)
	}
	if !inRange(x, 100, 50) {
		t.Errorf("cancelled %d not within ±50 of 100", x)
	}
}

// TestPlanDemoOutcomes_IncludeFailedForcesFirstTwo guards the
// includeFailed contract: slot 0 is failed, slot 1 is cancelled,
// regardless of how the RNG draws otherwise.
func TestPlanDemoOutcomes_IncludeFailedForcesFirstTwo(t *testing.T) {
	t.Parallel()
	plan := planDemoOutcomes(10, true, 999)
	if plan[0] != outcomeFailed {
		t.Errorf("plan[0] = %v, want %v", plan[0], outcomeFailed)
	}
	if plan[1] != outcomeCancelled {
		t.Errorf("plan[1] = %v, want %v", plan[1], outcomeCancelled)
	}
}

// TestParseDemoSeedFlags_Defaults locks the default knobs.
func TestParseDemoSeedFlags_Defaults(t *testing.T) {
	t.Parallel()
	f, err := parseDemoSeedFlags([]string{})
	if err != nil {
		t.Fatalf("parseDemoSeedFlags([]): %v", err)
	}
	if f.count != 10 {
		t.Errorf("count = %d, want 10", f.count)
	}
	if f.timeout != 5*time.Second {
		t.Errorf("timeout = %v, want 5s", f.timeout)
	}
	if f.includeFailed {
		t.Errorf("includeFailed = true, want false")
	}
}

// TestParseDemoSeedFlags_IncludeFailed verifies the flag flips.
func TestParseDemoSeedFlags_IncludeFailed(t *testing.T) {
	t.Parallel()
	f, err := parseDemoSeedFlags(
		[]string{"--include-failed", "--count=25"},
	)
	if err != nil {
		t.Fatalf("parseDemoSeedFlags: %v", err)
	}
	if !f.includeFailed {
		t.Errorf("includeFailed = false, want true")
	}
	if f.count != 25 {
		t.Errorf("count = %d, want 25", f.count)
	}
}

// TestParseDemoSeedFlags_RejectsBadCount confirms input validation.
func TestParseDemoSeedFlags_RejectsBadCount(t *testing.T) {
	t.Parallel()
	cases := []string{"--count=0", "--count=-1",
		"--count=abc", "--count=10000"}
	for _, arg := range cases {
		if _, err := parseDemoSeedFlags([]string{arg}); err == nil {
			t.Errorf("parseDemoSeedFlags(%q) error = nil,"+
				" want non-nil", arg)
		}
	}
}

// countPlan tallies a plan slice into completed/failed/cancelled
// counts. Test-only helper kept here (not in the production file)
// because the production code never needs to count its own plan.
func countPlan(plan []demoOutcome) (int, int, int) {
	var c, f, x int
	for _, o := range plan {
		switch o {
		case outcomeCompleted:
			c++
		case outcomeFailed:
			f++
		case outcomeCancelled:
			x++
		}
	}
	return c, f, x
}

// inRange returns true when |got - want| <= tolerance.
func inRange(got, want, tolerance int) bool {
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

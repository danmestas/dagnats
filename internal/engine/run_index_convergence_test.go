// engine/run_index_convergence_test.go
// Tests for repairRunIndexToConvergenceWith: the pure, store-agnostic
// retry-with-backoff-then-fail loop Orchestrator.Start uses to repair
// the run index before serving (#659 review round 2). Pure unit tests
// against a fake runIndexRepairer -- no real NATS -- so retry/backoff
// and non-convergence paths are fast and deterministic to exercise.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
)

// fakeRunIndexRepairer is a scriptable runIndexRepairer: failUntilCall
// makes every call up to and including that call number fail (0 means
// never fail); results supplies the RepairStats for each call that
// does NOT fail, consumed in order and held at the last entry once
// exhausted; persistentErr, if set, makes every call fail forever
// (overrides failUntilCall).
type fakeRunIndexRepairer struct {
	calls         int
	failUntilCall int
	persistentErr error
	results       []RepairStats
}

func (f *fakeRunIndexRepairer) RepairRunIndex(
	_ context.Context, pageMax int,
) (RepairStats, error) {
	if pageMax <= 0 {
		panic("fakeRunIndexRepairer.RepairRunIndex: pageMax must be positive")
	}
	f.calls++
	if f.persistentErr != nil {
		return RepairStats{}, f.persistentErr
	}
	if f.calls <= f.failUntilCall {
		return RepairStats{}, errors.New("injected transient failure")
	}
	if len(f.results) == 0 {
		return RepairStats{}, nil
	}
	idx := len(f.results) - 1
	if resultIdx := f.calls - f.failUntilCall - 1; resultIdx < len(f.results) {
		idx = resultIdx
	}
	return f.results[idx], nil
}

// testBackoff is a short schedule so retry tests run in milliseconds,
// not the real 200ms-3.2s production schedule.
var testBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

// TestRepairRunIndexToConvergenceRecoversFromTransientError is failing
// test (a): a transient error on the very first RepairRunIndex call
// must not fail the whole convergence loop -- the retry-with-backoff
// inside a single pass absorbs it, and convergence still completes.
func TestRepairRunIndexToConvergenceRecoversFromTransientError(t *testing.T) {
	repairer := &fakeRunIndexRepairer{
		failUntilCall: 1, // call #1 fails, call #2+ succeed
		results: []RepairStats{
			{Repaired: 1200},                 // pass 1 (after retry): still work to do
			{Repaired: 0, OrphansRemoved: 0}, // pass 2: converged
		},
	}
	repaired, orphans, passes, err := repairRunIndexToConvergenceWith(
		context.Background(), repairer, repairPageMax,
		repairRunIndexMaxIterations, testBackoff,
	)
	// Positive: convergence succeeded despite the transient failure.
	if err != nil {
		t.Fatalf("repairRunIndexToConvergenceWith: %v", err)
	}
	if repaired != 1200 {
		t.Fatalf("repaired = %d, want 1200", repaired)
	}
	if orphans != 0 {
		t.Fatalf("orphans = %d, want 0", orphans)
	}
	if passes != 2 {
		t.Fatalf("passes = %d, want 2", passes)
	}
	// Negative: the retry must have actually happened (3 total calls:
	// 1 failure + 2 successful passes), not silently skipped.
	if repairer.calls != 3 {
		t.Fatalf("repairer.calls = %d, want 3 (1 failed + 2 ok)", repairer.calls)
	}
}

// TestRepairRunIndexToConvergencePersistentErrorReturnsError is
// failing test (b): a persistent RepairRunIndex error must exhaust
// the retry budget and return an error -- not panic, not hang.
func TestRepairRunIndexToConvergencePersistentErrorReturnsError(t *testing.T) {
	repairer := &fakeRunIndexRepairer{
		persistentErr: errors.New("kv unavailable"),
	}
	_, _, _, err := repairRunIndexToConvergenceWith(
		context.Background(), repairer, repairPageMax,
		repairRunIndexMaxIterations, testBackoff,
	)
	// Positive: an error comes back.
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Negative: the retry budget was exhausted (1 initial + len(backoff)
	// retries), not given up early or retried forever.
	wantCalls := len(testBackoff) + 1
	if repairer.calls != wantCalls {
		t.Fatalf("repairer.calls = %d, want %d (retry budget exhausted)",
			repairer.calls, wantCalls)
	}
}

// TestRepairRunIndexToConvergenceExceedingBoundReturnsError is failing
// test (c): a store that never converges (every pass reports more
// work) must return an error once the iteration bound is hit --
// NEVER panic. Non-convergence is an operating condition (a huge
// store, or writes outpacing repair), not a programmer error.
func TestRepairRunIndexToConvergenceExceedingBoundReturnsError(t *testing.T) {
	repairer := &fakeRunIndexRepairer{
		results: []RepairStats{{Repaired: 1}}, // never reports zero
	}
	const smallBound = 3
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("must return an error, not panic: %v", r)
		}
	}()
	repaired, _, passes, err := repairRunIndexToConvergenceWith(
		context.Background(), repairer, repairPageMax, smallBound, testBackoff,
	)
	// Positive: an error naming the bound comes back.
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if passes != smallBound {
		t.Fatalf("passes = %d, want %d", passes, smallBound)
	}
	// Negative: progress made before giving up is still reported, not
	// silently dropped -- an operator debugging this wants to know it
	// was making progress, just not converging.
	if repaired != smallBound {
		t.Fatalf("repaired = %d, want %d", repaired, smallBound)
	}
}

// TestOrchestratorStartFailsLoudlyOnPersistentRepairError is failing
// test (b) at the real Orchestrator.Start level (not the pure
// repairRunIndexToConvergenceWith unit above): a persistent repair
// failure must surface as an error from Start, and the history
// consumer must NEVER have been started -- the process must not begin
// serving reads over a possibly-misordered index.
//
// The persistent failure is genuine, not mocked: a run.<id> value
// written with corrupt (non-JSON) bytes and no index entry makes
// every RepairRunIndex call's backfill unmarshal fail, forever --
// there is no self-healing path, so the retry budget always exhausts.
// repairRunIndexRetryBackoff is temporarily shortened so the test
// doesn't pay the real ~6s production backoff.
func TestOrchestratorStartFailsLoudlyOnPersistentRepairError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc)

	if _, err := orch.store.kv.Put(
		context.Background(), "run.corrupt-run", []byte("not json"),
	); err != nil {
		t.Fatalf("seed corrupt run: %v", err)
	}

	saved := repairRunIndexRetryBackoff
	repairRunIndexRetryBackoff = testBackoff
	t.Cleanup(func() { repairRunIndexRetryBackoff = saved })

	err := orch.Start()
	// Positive: Start returns an error.
	if err == nil {
		t.Fatal("expected Start to return an error, got nil")
	}
	// Negative: the history consumer was NEVER started -- Start must
	// not have gotten past the repair step.
	if orch.cc != nil {
		t.Fatal("history consumer was started despite a repair failure")
	}
	// A failed Start leaves nothing running to Stop; nothing to clean up.
}

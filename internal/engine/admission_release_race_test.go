// internal/engine/admission_release_race_test.go
// Regression test for the PR #661 review round-2 finding on #648:
// ReleaseSingletonLock's Get-then-Delete was not revision-guarded, so
// a reclaim of the SAME key by a new run landing between the Get and
// the Delete (the reconciler can replay a release arbitrarily late,
// long after the original owner's lock was already superseded) would
// have the late Delete wipe out the NEW holder's fresh lock instead
// of no-op'ing.
//
// This race is only reachable under genuine concurrent execution (the
// ownership check inside ReleaseSingletonLock already guards any
// purely-sequential replay -- a second call sees the new owner's
// RunID and skips). releaseSingletonLockRaceHook is a test-only seam
// (default no-op in production) that lets this test force the
// Get/Delete interleaving deterministically instead of relying on a
// timing-dependent goroutine race.
//
// Methodology: real embedded NATS/JetStream KV.
package engine

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestReleaseSingletonLock_RevisionGuardsAgainstReclaim(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	singletonKV, err := js.KeyValue("singleton_locks")
	if err != nil {
		t.Fatalf("singleton_locks KV: %v", err)
	}

	orch := NewOrchestrator(nc)
	ctx := context.Background()

	const key = "race-wf"
	// Run A claims the lock.
	if _, err := singletonKV.Create(key, []byte(`{"run_id":"run-A"}`)); err != nil {
		t.Fatalf("create lock for run-A: %v", err)
	}

	// Force the interleaving: after ReleaseSingletonLock's internal
	// Get (which sees run-A and decides to delete) but before its
	// Delete executes, run-B reclaims the SAME key -- simulating
	// singletonCheck's stale-lock reclaim racing with a very late
	// reconciler replay of run-A's release.
	restore := releaseSingletonLockRaceHook
	defer func() { releaseSingletonLockRaceHook = restore }()
	releaseSingletonLockRaceHook = func() {
		entry, getErr := singletonKV.Get(key)
		if getErr != nil {
			t.Fatalf("get lock before reclaim: %v", getErr)
		}
		if _, updErr := singletonKV.Update(
			key, []byte(`{"run_id":"run-B"}`), entry.Revision(),
		); updErr != nil {
			t.Fatalf("simulate reclaim by run-B: %v", updErr)
		}
	}

	orch.admission.ReleaseSingletonLock(ctx, dag.WorkflowRun{
		RunID: "run-A", SingletonKey: key,
	})

	// Positive: run-B's fresh lock must survive run-A's late release.
	entry, getErr := singletonKV.Get(key)
	if getErr != nil {
		t.Fatalf("lock for run-B missing after run-A's release: %v", getErr)
	}
	if string(entry.Value()) != `{"run_id":"run-B"}` {
		t.Fatalf("lock value = %s, want run-B's untouched lock", entry.Value())
	}
}

// internal/engine/reconciler_release_pending_test.go
// Tests for #648: the reconciler's terminal-run sweep recovers the
// admission release (singleton lock / concurrency slot) for a run
// whose finalizeRun left WorkflowRun.ReleasePending set because
// afterPersist failed after the terminal snapshot was already saved.
// Also proves releaseAdmission's idempotence -- a second call after a
// successful release must not double-free a lock/slot.
//
// Methodology: real embedded NATS, real KV (singleton_locks,
// concurrency_runs); bypass orchestrator.Start's reconcile ticker and
// call reconcileRunningRuns/releaseAdmission directly, exactly like
// reconciler_test.go's existing wedged-run tests. The debt state
// (terminal + ReleasePending) is written straight to the snapshot
// store rather than by injecting a failure into a live event handler
// -- this is the same "controllable afterPersist" shape
// run_event_release_pending_test.go already uses for finalizeRun
// itself, applied here one layer up.
//
// A note on singleton recovery specifically: singletonCheck
// (admission.go) already self-heals a leaked lock the next time a
// NEW run for that same singleton key is admitted -- it reloads the
// lock-holding run and reclaims the key if that run is terminal. That
// pre-existing mechanism means "start a competing run and see if it's
// admitted" cannot isolate the reconciler's OWN release from that
// unrelated self-heal, so the singleton test below asserts directly
// against the singleton_locks KV entry instead. The concurrency slot
// has no such self-heal (AcquireRun only ever inspects the shared
// counter, never a specific run's status), so it is the resource
// where the reconciler's recovery is actually load-bearing, and its
// tests use the "second/third run admitted" shape end to end.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// markTerminalWithDebt mutates run in place to look exactly like a
// finalizeRun call that hit finalizeWithReleaseDebt: terminal status,
// CompletedAt stamped, ReleasePending set. Mirrors markTerminal
// (run_event.go) plus the one extra field #648 adds.
func markTerminalWithDebt(run dag.WorkflowRun, status dag.RunStatus) dag.WorkflowRun {
	run.Status = status
	now := time.Now().UTC()
	run.CompletedAt = &now
	run.ReleasePending = true
	return run
}

func TestReconciler_RecoversReleasePending_SingletonLock(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "release-pending-singleton-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
		Singleton: &dag.SingletonConfig{Mode: dag.SingletonModeSkip},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// run-1 acquires the singleton lock through the real admission
	// path and starts Running.
	startAdmissionRun(t, js, wfDef, "rp-singleton-1", nil)
	waitForRunStatus(t, orch.store, "rp-singleton-1",
		dag.RunStatusRunning, 5*time.Second)

	ctx := context.Background()
	run1, err := orch.store.Load(ctx, "rp-singleton-1")
	if err != nil {
		t.Fatalf("load rp-singleton-1: %v", err)
	}
	if run1.SingletonKey == "" {
		t.Fatal("rp-singleton-1: SingletonKey must be set once admitted")
	}

	// Simulate finalizeRun hitting finalizeWithReleaseDebt: the run
	// completed, but afterPersist (the release) failed, so the lock
	// is STILL held and the flag is now set. Written directly (not by
	// injecting a failure into the live completion handler) so the
	// test is deterministic.
	run1 = markTerminalWithDebt(run1, dag.RunStatusCompleted)
	if err := orch.store.Save(ctx, run1); err != nil {
		t.Fatalf("save release-pending run-1: %v", err)
	}

	singletonKV, err := js.KeyValue("singleton_locks")
	if err != nil {
		t.Fatalf("singleton_locks KV: %v", err)
	}

	// Positive: the lock is still present before recovery.
	if _, getErr := singletonKV.Get(run1.SingletonKey); getErr != nil {
		t.Fatalf("lock %q missing before recovery: %v", run1.SingletonKey, getErr)
	}

	// Run one reconciler pass directly -- this is the #648 recovery
	// sweep, not the wedged-Running path (run-1 is terminal).
	orch.reconcileRunningRuns(ctx)

	// Positive: the flag is cleared once the recovery sweep runs.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reloaded, loadErr := orch.store.Load(ctx, "rp-singleton-1")
		if loadErr == nil && !reloaded.ReleasePending {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	recovered, err := orch.store.Load(ctx, "rp-singleton-1")
	if err != nil {
		t.Fatalf("load rp-singleton-1 after sweep: %v", err)
	}
	if recovered.ReleasePending {
		t.Fatal("rp-singleton-1: ReleasePending still true after reconciler pass")
	}

	// Positive: the lock itself is gone -- releaseAdmission actually
	// deleted it via the same ReleaseSingletonLock the normal
	// completion path uses, not merely a flag flip.
	if _, getErr := singletonKV.Get(run1.SingletonKey); getErr == nil {
		t.Fatal("lock still present after reconciler recovery -- releaseAdmission did not delete it")
	}
}

func TestReconciler_NormalTerminalRun_NeverTouchedByReleasePendingSweep(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "release-pending-normal-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
	}
	seedWorkflowDef(t, nc, wfDef)

	orch := NewOrchestrator(nc)
	ctx := context.Background()

	// A run that finalized cleanly (ReleasePending never set) --
	// released admission normally, nothing owed.
	run := dag.WorkflowRun{
		RunID:      "rp-normal-1",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusCompleted,
		Steps:      map[string]dag.StepState{"a": {Status: dag.StepStatusCompleted}},
		CreatedAt:  time.Now().UTC(),
	}
	now := time.Now().UTC()
	run.CompletedAt = &now
	if err := orch.store.Save(ctx, run); err != nil {
		t.Fatalf("save rp-normal-1: %v", err)
	}

	// Must not panic or error touching a terminal run with no debt.
	orch.reconcileRunningRuns(ctx)

	reloaded, err := orch.store.Load(ctx, "rp-normal-1")
	if err != nil {
		t.Fatalf("load rp-normal-1: %v", err)
	}
	if reloaded.ReleasePending {
		t.Fatal("rp-normal-1: ReleasePending became true, want unchanged false")
	}
	if reloaded.Status != dag.RunStatusCompleted {
		t.Fatalf("rp-normal-1: status = %v, want unchanged Completed", reloaded.Status)
	}
}

// TestReconciler_RecoversReleasePending_ConcurrencySlot proves the
// concurrency-slot half of #648's leak end to end: unlike the
// singleton lock (which self-heals via singletonCheck's staleness
// reclaim, see the file header), the shared concurrency counter has
// no such safety net -- a run whose slot never got released blocks
// every later run for that workflow forever, with no recovery path
// other than this sweep.
func TestReconciler_RecoversReleasePending_ConcurrencySlot(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "release-pending-concurrency-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startAdmissionRun(t, js, wfDef, "rp-conc-1", nil)
	waitForRunStatus(t, orch.store, "rp-conc-1",
		dag.RunStatusRunning, 5*time.Second)

	ctx := context.Background()
	run1, err := orch.store.Load(ctx, "rp-conc-1")
	if err != nil {
		t.Fatalf("load rp-conc-1: %v", err)
	}
	// Simulate the release failure -- the slot stays held (MaxRuns=1,
	// counter still at 1) even though the run is terminal.
	run1 = markTerminalWithDebt(run1, dag.RunStatusCompleted)
	if err := orch.store.Save(ctx, run1); err != nil {
		t.Fatalf("save release-pending run-1: %v", err)
	}

	// Positive: with the slot still held, a second run for the same
	// workflow queues instead of running -- the leak is real and
	// nothing about a redelivery/new run fixes it on its own.
	startAdmissionRun(t, js, wfDef, "rp-conc-2", nil)
	waitForRunStatus(t, orch.store, "rp-conc-2",
		dag.RunStatusPending, 5*time.Second)

	// Run one reconciler pass -- releases the slot AND (since
	// releaseAdmission also calls startNextPendingRun) advances the
	// queue, promoting rp-conc-2 to Running in the same pass.
	orch.reconcileRunningRuns(ctx)

	waitForRunStatus(t, orch.store, "rp-conc-2",
		dag.RunStatusRunning, 5*time.Second)

	recovered, err := orch.store.Load(ctx, "rp-conc-1")
	if err != nil {
		t.Fatalf("load rp-conc-1 after sweep: %v", err)
	}
	if recovered.ReleasePending {
		t.Fatal("rp-conc-1: ReleasePending still true after reconciler pass")
	}
	if recovered.Status != dag.RunStatusCompleted {
		t.Fatalf("rp-conc-1: status = %v, want unchanged Completed", recovered.Status)
	}
}

// TestReleaseAdmission_ReplayedConcurrencyReleaseDoesNotStealNewHolder
// is the exact scenario from the PR #661 review round-3 BLOCKER,
// driven through the public releaseAdmission path a reconciler replay
// actually uses (not just the underlying ConcurrencyManager unit --
// see concurrency_test.go's TestConcurrencyReleaseDoesNotStealADifferentRunsSlot
// for that level). MaxRuns=1: run1 admitted; releaseAdmission(run1)
// (the first, real release); run2 admitted into the freed slot;
// releaseAdmission(run1) AGAIN (the replay -- simulating the
// reconciler retrying a ReleasePending debt whose flag-clear save
// failed after the first release already succeeded); run3 must NOT be
// admitted while run2 is Running.
func TestReleaseAdmission_ReplayedConcurrencyReleaseDoesNotStealNewHolder(
	t *testing.T,
) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "release-pending-replay-conc-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startAdmissionRun(t, js, wfDef, "rp-replay-conc-1", nil)
	waitForRunStatus(t, orch.store, "rp-replay-conc-1",
		dag.RunStatusRunning, 5*time.Second)

	ctx := context.Background()
	run1, err := orch.store.Load(ctx, "rp-replay-conc-1")
	if err != nil {
		t.Fatalf("load rp-replay-conc-1: %v", err)
	}
	run1 = markTerminalWithDebt(run1, dag.RunStatusCompleted)
	if err := orch.store.Save(ctx, run1); err != nil {
		t.Fatalf("save rp-replay-conc-1: %v", err)
	}

	// The first, real release.
	if err := orch.releaseAdmission(ctx, run1); err != nil {
		t.Fatalf("releaseAdmission (first, real release): %v", err)
	}

	// run2 acquires the slot run1 freed.
	startAdmissionRun(t, js, wfDef, "rp-replay-conc-2", nil)
	waitForRunStatus(t, orch.store, "rp-replay-conc-2",
		dag.RunStatusRunning, 5*time.Second)

	// The replay: run1's release fires AGAIN, long after run2 took
	// the slot run1 vacated.
	if err := orch.releaseAdmission(ctx, run1); err != nil {
		t.Fatalf("releaseAdmission (replay): %v", err)
	}

	// Negative: run3 must NOT be admitted -- the replay must not have
	// stolen run2's slot. A bare-counter implementation would have
	// decremented the shared counter to 0 here and wrongly admitted
	// run3 alongside the still-Running run2.
	startAdmissionRun(t, js, wfDef, "rp-replay-conc-3", nil)
	waitForRunStatus(t, orch.store, "rp-replay-conc-3",
		dag.RunStatusPending, 5*time.Second)
	run2, err := orch.store.Load(ctx, "rp-replay-conc-2")
	if err != nil {
		t.Fatalf("load rp-replay-conc-2: %v", err)
	}
	if run2.Status != dag.RunStatusRunning {
		t.Fatalf(
			"rp-replay-conc-2: status = %v, want Running -- the "+
				"replay must not have disturbed its slot",
			run2.Status,
		)
	}
}

// TestReleaseAdmission_ReplayedSingletonReleaseDoesNotStealNewHolder
// is the singleton-lock analog of the concurrency-slot scenario above,
// driven through releaseAdmission end to end (complementing
// admission_release_race_test.go's internal-hook TOCTOU test, which
// forces the Get/Delete interleaving directly -- this one exercises
// the ordinary sequential replay a reconciler retry produces: by the
// time the replay's Get runs, the lock's owner has already changed,
// so the existing ownership check alone is enough to protect it).
func TestReleaseAdmission_ReplayedSingletonReleaseDoesNotStealNewHolder(
	t *testing.T,
) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "release-pending-replay-singleton-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
		Singleton: &dag.SingletonConfig{Mode: dag.SingletonModeSkip},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startAdmissionRun(t, js, wfDef, "rp-replay-sing-1", nil)
	waitForRunStatus(t, orch.store, "rp-replay-sing-1",
		dag.RunStatusRunning, 5*time.Second)

	ctx := context.Background()
	run1, err := orch.store.Load(ctx, "rp-replay-sing-1")
	if err != nil {
		t.Fatalf("load rp-replay-sing-1: %v", err)
	}
	if run1.SingletonKey == "" {
		t.Fatal("rp-replay-sing-1: SingletonKey must be set once admitted")
	}
	singletonKey := run1.SingletonKey
	run1 = markTerminalWithDebt(run1, dag.RunStatusCompleted)
	if err := orch.store.Save(ctx, run1); err != nil {
		t.Fatalf("save rp-replay-sing-1: %v", err)
	}

	// The first, real release.
	if err := orch.releaseAdmission(ctx, run1); err != nil {
		t.Fatalf("releaseAdmission (first, real release): %v", err)
	}

	// run2 acquires the lock run1 freed.
	startAdmissionRun(t, js, wfDef, "rp-replay-sing-2", nil)
	waitForRunStatus(t, orch.store, "rp-replay-sing-2",
		dag.RunStatusRunning, 5*time.Second)

	// The replay: run1's release fires AGAIN, long after run2 took
	// the lock run1 vacated.
	if err := orch.releaseAdmission(ctx, run1); err != nil {
		t.Fatalf("releaseAdmission (replay): %v", err)
	}

	// Positive: the lock is still present and still run2's -- the
	// replay must not have deleted it.
	singletonKV, err := js.KeyValue("singleton_locks")
	if err != nil {
		t.Fatalf("singleton_locks KV: %v", err)
	}
	entry, getErr := singletonKV.Get(singletonKey)
	if getErr != nil {
		t.Fatalf("lock missing after replay: %v", getErr)
	}
	if string(entry.Value()) != `{"run_id":"rp-replay-sing-2"}` {
		t.Fatalf(
			"lock value = %s, want run2's untouched lock", entry.Value(),
		)
	}

	// Negative: run3 must NOT be admitted while run2 holds the lock.
	startAdmissionRun(t, js, wfDef, "rp-replay-sing-3", nil)
	waitForRunStatus(t, orch.store, "rp-replay-sing-3",
		dag.RunStatusCancelled, 5*time.Second)
}

// TestReconciler_ReleasePendingAbandonedAfterMaxAttempts is the PR
// #661 review round-3 NIT: a ReleasePending run whose release keeps
// failing for a structural (not transient) reason must not retry --
// and WARN-log -- every reconciler pass forever. After
// releaseAttemptsMax failed attempts, the sweep must stop retrying
// (clear ReleasePending) and log the abandonment once at ERROR,
// incrementing engine.finalize.release_abandoned.
//
// The failure is forced deterministically via fakeDeleteErrKV (same
// seam admission_release_race_test.go uses) wired into a REPLACEMENT
// AdmissionController on the orchestrator, so every ReleaseSingletonLock
// call for this run's key returns the injected non-revision-mismatch
// error every single pass -- never succeeding.
func TestReconciler_ReleasePendingAbandonedAfterMaxAttempts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsc, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	realSingletonKV, err := jsc.KeyValue(
		context.Background(), "singleton_locks",
	)
	if err != nil {
		t.Fatalf("jetstream singleton_locks KV: %v", err)
	}

	orch := NewOrchestrator(nc)
	fakeKV := &fakeDeleteErrKV{
		KeyValue:  realSingletonKV,
		deleteErr: errors.New("simulated persistent KV failure"),
	}
	orch.admission = NewAdmissionController(
		nc, jsc, orch.tp, orch.store, nil, fakeKV,
	)

	ctx := context.Background()
	const key = "abandon-wf"
	singletonKV, err := js.KeyValue("singleton_locks")
	if err != nil {
		t.Fatalf("singleton_locks KV: %v", err)
	}
	if _, err := singletonKV.Create(
		key, []byte(`{"run_id":"rp-abandon-1"}`),
	); err != nil {
		t.Fatalf("create lock: %v", err)
	}

	run := dag.WorkflowRun{
		RunID:          "rp-abandon-1",
		WorkflowID:     "abandon-wf",
		Status:         dag.RunStatusCompleted,
		Steps:          map[string]dag.StepState{},
		CreatedAt:      time.Now().UTC(),
		SingletonKey:   key,
		ReleasePending: true,
	}
	now := time.Now().UTC()
	run.CompletedAt = &now
	if err := orch.store.Save(ctx, run); err != nil {
		t.Fatalf("save rp-abandon-1: %v", err)
	}

	// Drive reconciler passes until the sweep gives up -- bounded by
	// releaseAttemptsMax+2 iterations so a regression that never
	// abandons fails the test instead of hanging.
	var final dag.WorkflowRun
	for i := 0; i < releaseAttemptsMax+2; i++ {
		reloaded, loadErr := orch.store.Load(ctx, "rp-abandon-1")
		if loadErr != nil {
			t.Fatalf("load pass %d: %v", i, loadErr)
		}
		if !reloaded.ReleasePending {
			final = reloaded
			break
		}
		orch.reconcileReleasePending(ctx, reloaded)
	}

	// Positive: the sweep stopped (ReleasePending cleared) instead of
	// retrying forever.
	if final.RunID != "rp-abandon-1" {
		t.Fatalf(
			"ReleasePending was never cleared within %d passes -- "+
				"sweep did not abandon",
			releaseAttemptsMax+2,
		)
	}
	// Positive: it took the full bounded number of attempts, not
	// fewer (i.e. the release genuinely kept failing every pass, this
	// isn't a false pass from some other bug clearing the flag).
	if final.ReleaseAttempts < releaseAttemptsMax {
		t.Fatalf(
			"ReleaseAttempts = %d, want >= %d (releaseAttemptsMax)",
			final.ReleaseAttempts, releaseAttemptsMax,
		)
	}
}

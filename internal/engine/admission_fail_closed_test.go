// internal/engine/admission_fail_closed_test.go
// The singleton admission fails closed: a lock that cannot be read or
// moved refuses the start (the caller returns the error and the start
// is redelivered) and never admits a second run. Before, every such
// error path returned an empty admissionResult, which admits.
//
// Methodology: singletonCheck against a scripted singleton bucket
// (fakeLockKV: Create, Get and Update return what each case sets) and
// a real embedded-NATS snapshot store for the holder's run. Each case
// asserts the error (positive) and that nothing was admitted or
// cancelled (negative); the reclaim cases assert the admit side.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

var errLockBoom = errors.New("lock bucket unavailable")

// fakeLockKV scripts the three calls singletonCheck makes.
type fakeLockKV struct {
	jetstream.KeyValue
	createErr error
	getEntry  jetstream.KeyValueEntry
	getErr    error
	updateErr error
	updates   int
}

func (f *fakeLockKV) Create(
	context.Context, string, []byte, ...jetstream.KVCreateOpt,
) (uint64, error) {
	return 1, f.createErr
}

func (f *fakeLockKV) Get(context.Context, string) (jetstream.KeyValueEntry, error) {
	return f.getEntry, f.getErr
}

func (f *fakeLockKV) Update(context.Context, string, []byte, uint64) (uint64, error) {
	f.updates++
	return 2, f.updateErr
}

// fakeLockEntry is a lock record held by runID, created at created.
type fakeLockEntry struct {
	jetstream.KeyValueEntry
	runID   string
	created time.Time
}

func (e fakeLockEntry) Value() []byte      { return []byte(`{"run_id":"` + e.runID + `"}`) }
func (e fakeLockEntry) Revision() uint64   { return 7 }
func (e fakeLockEntry) Created() time.Time { return e.created }

func newFailClosedController(t *testing.T, kv jetstream.KeyValue) *AdmissionController {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return &AdmissionController{store: NewSnapshotStore(js), singletonKV: kv}
}

func checkSingleton(
	t *testing.T, ac *AdmissionController, mode dag.SingletonMode,
) (admissionResult, error) {
	t.Helper()
	result, _, err := ac.singletonCheck(context.Background(), "wf",
		&dag.SingletonConfig{Mode: mode}, "new-run", nil)
	return result, err
}

func TestSingletonCheck_FailsClosedOnLockErrors(t *testing.T) {
	saveTerminal := func(t *testing.T, ac *AdmissionController, runID string) {
		t.Helper()
		run := dag.WorkflowRun{RunID: runID, WorkflowID: "wf", Status: dag.RunStatusCompleted}
		if err := ac.store.Save(context.Background(), run); err != nil {
			t.Fatalf("save %s: %v", runID, err)
		}
	}
	cases := []struct {
		name  string
		mode  dag.SingletonMode
		kv    *fakeLockKV
		setup func(*testing.T, *AdmissionController)
	}{
		{name: "create fails", mode: dag.SingletonModeSkip,
			kv: &fakeLockKV{createErr: errLockBoom}},
		{name: "lock read fails", mode: dag.SingletonModeSkip,
			kv: &fakeLockKV{createErr: jetstream.ErrKeyExists, getErr: errLockBoom}},
		{name: "stale reclaim fails", mode: dag.SingletonModeSkip,
			kv: &fakeLockKV{createErr: jetstream.ErrKeyExists, updateErr: errLockBoom,
				getEntry: fakeLockEntry{runID: "done-run", created: time.Now()}},
			setup: func(t *testing.T, ac *AdmissionController) { saveTerminal(t, ac, "done-run") }},
		{name: "cancel move fails", mode: dag.SingletonModeCancel,
			kv: &fakeLockKV{createErr: jetstream.ErrKeyExists, updateErr: errLockBoom,
				getEntry: fakeLockEntry{runID: "unsaved-run", created: time.Now()}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac := newFailClosedController(t, tc.kv)
			if tc.setup != nil {
				tc.setup(t, ac)
			}
			result, err := checkSingleton(t, ac, tc.mode)
			if !errors.Is(err, errLockBoom) {
				t.Fatalf("err = %v, want the lock error (refuse, not admit)", err)
			}
			if result.action != admissionProceed || result.cancelID != "" {
				t.Fatalf("result = %+v, want nothing admitted or cancelled", result)
			}
		})
	}
}

// A holder whose run is not saved yet still holds the lock: admission
// claims the lock before the run's first save, so a concurrent start
// finds exactly this. Past the grace the lock is stale and reclaimed.
func TestSingletonCheck_UnsavedHolderHoldsUntilTheGrace(t *testing.T) {
	young := &fakeLockKV{createErr: jetstream.ErrKeyExists,
		getEntry: fakeLockEntry{runID: "unsaved-run", created: time.Now()}}
	result, err := checkSingleton(t, newFailClosedController(t, young), dag.SingletonModeSkip)
	if err != nil || result.action != admissionSkip || result.skippedBy != "unsaved-run" {
		t.Fatalf("young lock: %+v, %v; want skipped by unsaved-run", result, err)
	}
	if young.updates != 0 {
		t.Fatalf("young lock moved %d times, want 0", young.updates)
	}
	old := &fakeLockKV{createErr: jetstream.ErrKeyExists, getEntry: fakeLockEntry{
		runID: "lost-run", created: time.Now().Add(-2 * singletonUnsavedGrace)}}
	result, err = checkSingleton(t, newFailClosedController(t, old), dag.SingletonModeSkip)
	if err != nil || result.action != admissionProceed || old.updates != 1 {
		t.Fatalf("old lock: %+v, %v after %d moves; want reclaimed", result, err, old.updates)
	}
}

// A rival moving the lock first is not an error: the claim looks again,
// a bounded number of times.
func TestSingletonCheck_RetriesARivalMoveThenRefuses(t *testing.T) {
	kv := &fakeLockKV{createErr: jetstream.ErrKeyExists,
		updateErr: jetstream.ErrKeyRevisionMismatch,
		getEntry:  fakeLockEntry{runID: "lost-run", created: time.Now().Add(-time.Hour)}}
	result, err := checkSingleton(t, newFailClosedController(t, kv), dag.SingletonModeSkip)
	if err == nil || result.action != admissionProceed {
		t.Fatalf("result = %+v, %v; want refused after the bound", result, err)
	}
	if kv.updates != singletonClaimAttemptsMax {
		t.Fatalf("moves = %d, want %d", kv.updates, singletonClaimAttemptsMax)
	}
}

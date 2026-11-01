// internal/engine/admission_lock_key_test.go
// The singleton lock key and the admission paths around it. A key that
// is already a valid KV key keeps its exact form, so a lock taken
// before the encoding still matches; a key value KV would refuse (an
// e-mail address, a space) is replaced by a stable digest and still
// serialises its starts. An admission error no retry can cure fails
// the run with its reason. A start redelivered after its cancel-mode
// claim moved the lock, but before it published the cancel, cancels
// the displaced run.
//
// Methodology: singletonLockKey is a pure unit test. The rest drive a
// real orchestrator on an embedded NATS server through workflow.started
// events and poll the run snapshots under a bounded wait; each case
// asserts both runs (positive and negative).
package engine

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	lockKeyWait          = 10 * time.Second
	lockKeyRedeliverWait = 30 * time.Second
)

func TestSingletonLockKey_ValidKeysUnchangedInvalidHashed(t *testing.T) {
	keyed := &dag.SingletonConfig{Key: "k"}
	cases := map[string]string{
		`{"k":"acme-1"}`:    "wf.acme-1",
		`{"k":"a/b_c=d.e"}`: "wf.a/b_c=d.e",
		`{"k":7}`:           "wf.7",
	}
	for input, want := range cases {
		if got := singletonLockKey("wf", keyed, json.RawMessage(input)); got != want {
			t.Fatalf("key for %s = %q, want %q unchanged", input, got, want)
		}
	}
	if got := singletonLockKey("wf", &dag.SingletonConfig{}, nil); got != "wf" {
		t.Fatalf("unkeyed = %q, want wf", got)
	}
	email := singletonLockKey("wf", keyed, json.RawMessage(`{"k":"a@b.com"}`))
	space := singletonLockKey("wf", keyed, json.RawMessage(`{"k":"acme corp"}`))
	for _, got := range []string{email, space} {
		if !strings.HasPrefix(got, "h.") || len(got) != 2+64 || !natsutil.ValidKVKey(got) {
			t.Fatalf("encoded key %q, want h. plus a sha256 hex digest", got)
		}
	}
	if email == space {
		t.Fatal("distinct values encoded to one key")
	}
	again := singletonLockKey("wf", keyed, json.RawMessage(`{"k":"a@b.com"}`))
	if again != email {
		t.Fatalf("encoding not stable: %q then %q", email, again)
	}
}

// Keys jetstream refuses for their dots are encoded too, not passed
// through to fail every Create.
func TestSingletonLockKey_DotRulesMatchJetstream(t *testing.T) {
	keyed := &dag.SingletonConfig{Key: "k"}
	for _, value := range []string{"a..b", ".x", "x.", ""} {
		input := json.RawMessage(`{"k":"` + value + `"}`)
		got := singletonLockKey("wf", keyed, input)
		if !strings.HasPrefix(got, singletonDigestPrefix) || !natsutil.ValidKVKey(got) {
			t.Fatalf("value %q: key %q, want an encoded valid key", value, got)
		}
	}
	if got := singletonLockKey("wf", keyed, json.RawMessage(`{"k":"x.y"}`)); got != "wf.x.y" {
		t.Fatalf("dotted valid key = %q, want wf.x.y unchanged", got)
	}
}

// The digest namespace is exclusive: a workflow named "h" whose key
// value is another lock's digest gets a digest of its own.
func TestSingletonLockKey_RawKeyCannotTakeADigest(t *testing.T) {
	keyed := &dag.SingletonConfig{Key: "k"}
	billing := singletonLockKey("billing", keyed, json.RawMessage(`{"k":"a@b.com"}`))
	digest := strings.TrimPrefix(billing, singletonDigestPrefix)
	forged := singletonLockKey("h", keyed, json.RawMessage(`{"k":"`+digest+`"}`))
	if forged == billing {
		t.Fatalf("workflow h with value %s took billing's lock key %s", digest, billing)
	}
	if !strings.HasPrefix(forged, singletonDigestPrefix) || len(forged) != len(billing) {
		t.Fatalf("raw key in the digest namespace = %q, want it encoded", forged)
	}
	if got := singletonLockKey("h", &dag.SingletonConfig{}, nil); got != "h" {
		t.Fatalf("unkeyed workflow h = %q, want h unchanged", got)
	}
}

// startLockKeyOrchestrator registers wfDef and starts an orchestrator.
func startLockKeyOrchestrator(
	t *testing.T, wfDef dag.WorkflowDef,
) (*Orchestrator, nats.JetStreamContext) {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)
	mustPut(t, defKV, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)), defData)
	return NewOrchestrator(nc), js
}

// waitRunStatus polls runID's snapshot until it reaches want.
func waitRunStatus(
	t *testing.T, orch *Orchestrator, runID string, want dag.RunStatus,
) dag.WorkflowRun {
	t.Helper()
	deadline := time.Now().Add(lockKeyWait)
	for {
		run, err := orch.store.Load(context.Background(), runID)
		if err == nil && run.Status == want {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s = %v, %v; want %s", runID, run.Status, err, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func singletonDef(name string, mode dag.SingletonMode, key string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name: name, Version: "1",
		Steps:     []dag.StepDef{{ID: "a", Task: "parked-task", Type: dag.StepTypeNormal}},
		Singleton: &dag.SingletonConfig{Mode: mode, Key: key},
	}
}

func TestSingleton_InvalidKeyValuesStillSerialiseTheirStarts(t *testing.T) {
	wfDef := singletonDef("keyed-singleton", dag.SingletonModeSkip, "k")
	orch, js := startLockKeyOrchestrator(t, wfDef)
	orch.Start()
	defer orch.Stop()
	for i, value := range []string{"a@b.com", "acme corp"} {
		input := []byte(`{"k":"` + value + `"}`)
		first, second := "run-"+string(rune('a'+i))+"-1", "run-"+string(rune('a'+i))+"-2"
		startAdmissionRun(t, js, wfDef, first, input)
		running := waitRunStatus(t, orch, first, dag.RunStatusRunning)
		startAdmissionRun(t, js, wfDef, second, input)
		skipped := waitRunStatus(t, orch, second, dag.RunStatusCancelled)
		if !strings.HasPrefix(running.SingletonKey, "h.") {
			t.Fatalf("%q: lock key %q, want the encoded form", value, running.SingletonKey)
		}
		if !strings.Contains(skipped.Steps[admissionSkipStepID].Error, first) {
			t.Fatalf("%q: skip reason %q does not name %s",
				value, skipped.Steps[admissionSkipStepID].Error, first)
		}
	}
}

func TestSingleton_PermanentLockErrorFailsTheRunWithItsReason(t *testing.T) {
	wfDef := singletonDef("broken-lock-singleton", dag.SingletonModeSkip, "")
	orch, js := startLockKeyOrchestrator(t, wfDef)
	orch.admission.singletonKV = &fakeLockKV{createErr: jetstream.ErrInvalidKey}
	orch.Start()
	defer orch.Stop()
	startAdmissionRun(t, js, wfDef, "run-bad-key", nil)
	failed := waitRunStatus(t, orch, "run-bad-key", dag.RunStatusFailed)
	step, ok := failed.Steps[admissionFailedStepID]
	if !ok || !strings.Contains(step.Error, "singleton lock key") {
		t.Fatalf("failed run steps = %+v; want the admission reason", failed.Steps)
	}
	if failed.SingletonKey != "" {
		t.Fatalf("failed run holds lock key %q, want none", failed.SingletonKey)
	}
}

// A cancel-mode start that moved the lock (recording the run it
// displaced) and stopped before publishing the cancel leaves both runs
// alive. Its redelivery must cancel the displaced run.
func TestSingleton_RedeliveredStartFinishesTheCancelItsLockRecords(t *testing.T) {
	wfDef := singletonDef("crash-cancel-singleton", dag.SingletonModeCancel, "")
	orch, js := startLockKeyOrchestrator(t, wfDef)
	orch.Start()
	defer orch.Stop()
	startAdmissionRun(t, js, wfDef, "run-old", nil)
	waitRunStatus(t, orch, "run-old", dag.RunStatusRunning)
	locks, err := js.KeyValue("singleton_locks")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	entry, err := locks.Get(wfDef.Name)
	if err != nil {
		t.Fatalf("Get lock: %v", err)
	}
	// The first delivery of run-new's start: the lock moved, then the
	// engine died before the cancel was published.
	if _, err := locks.Update(wfDef.Name,
		marshalSingletonLock("run-new", "run-old"), entry.Revision()); err != nil {
		t.Fatalf("move lock: %v", err)
	}
	startAdmissionRun(t, js, wfDef, "run-new", nil)
	waitRunStatus(t, orch, "run-new", dag.RunStatusRunning)
	waitRunStatus(t, orch, "run-old", dag.RunStatusCancelled)
}

// A cancel that fails to publish must not be acked away: the start is
// redelivered, finds its own claim, and publishes the cancel again.
func TestSingleton_FailedCancelPublishRedeliversTheStart(t *testing.T) {
	wfDef := singletonDef("publish-fail-singleton", dag.SingletonModeCancel, "")
	orch, js := startLockKeyOrchestrator(t, wfDef)
	failures := &atomic.Int32{}
	publishCancelHook = func() error {
		if failures.Add(1) == 1 {
			return errLockBoom
		}
		return nil
	}
	t.Cleanup(func() { publishCancelHook = nil })
	orch.Start()
	defer orch.Stop()
	startAdmissionRun(t, js, wfDef, "run-old", nil)
	waitRunStatus(t, orch, "run-old", dag.RunStatusRunning)
	startAdmissionRun(t, js, wfDef, "run-new", nil)
	// The first redelivery waits historyRedeliverSchedule[0] (5s).
	deadline := time.Now().Add(lockKeyRedeliverWait)
	for {
		old, err := orch.store.Load(context.Background(), "run-old")
		if err == nil && old.Status == dag.RunStatusCancelled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run-old = %v, %v after %d publishes; want cancelled by the redelivery",
				old.Status, err, failures.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if failures.Load() < 2 {
		t.Fatalf("cancel published %d times, want a retry after the failure", failures.Load())
	}
	waitRunStatus(t, orch, "run-new", dag.RunStatusRunning)
}

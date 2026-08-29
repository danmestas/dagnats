// internal/engine/def_pin_test.go
//
// White-box unit tests for loadRunAndDef / loadPinnedOrLatestDef
// (#637): a run pinned via DefHash must read its immutable
// name.v.hash version instead of the mutable name -> latest pointer,
// a legacy run (DefHash=="") must fall back to the pointer, a
// MISSING pinned version must FAIL the advance loudly (never silently
// re-define a running run under whatever the mutable pointer now
// holds), and a corrupt pinned version (content hash mismatch) must
// also error rather than silently substitute something else.
// Methodology: real embedded NATS server per test; direct KV writes
// simulate both the immutable version key and the mutable pointer so
// each scenario is constructed precisely, then loadRunAndDef is
// called directly (same package -- white box). Bounded via the
// server's own request timeouts; no unbounded waits.
package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLoadRunAndDefPinnedVersionOverridesLatestPointer(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs): %v", err)
	}

	v1 := dag.WorkflowDef{Name: "pin-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	v2 := dag.WorkflowDef{Name: "pin-wf", Version: "2", Steps: []dag.StepDef{
		{ID: "a-renamed", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	v1Data := mustMarshal(t, v1)
	v2Data := mustMarshal(t, v2)
	versionKey := dag.DefVersionKey(v1.Name, dag.DefHash(v1))
	mustPut(t, defKV, versionKey, v1Data) // immutable v1 snapshot
	mustPut(t, defKV, v1.Name, v2Data)    // mutable pointer moved to v2

	run := dag.NewWorkflowRun(v1, "run-pin-1")

	orch := NewOrchestrator(nc)
	gotDef, gotRun, err := orch.loadRunAndDef(
		context.Background(), run.RunID,
	)
	// Load will fail because the run snapshot itself was never saved;
	// save it first via the store the orchestrator shares.
	if err == nil {
		t.Fatalf("expected load run error before Save, got def=%v run=%v",
			gotDef, gotRun)
	}
	if err := orch.store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save run: %v", err)
	}

	gotDef, gotRun, err = orch.loadRunAndDef(
		context.Background(), run.RunID,
	)
	if err != nil {
		t.Fatalf("loadRunAndDef: %v", err)
	}
	// Positive: the pinned v1 def was used, not the moved pointer.
	if len(gotDef.Steps) != 1 || gotDef.Steps[0].ID != "a" {
		t.Fatalf("gotDef.Steps = %+v, want v1's step \"a\"", gotDef.Steps)
	}
	// Negative: v2's renamed step must NOT appear.
	for _, step := range gotDef.Steps {
		if step.ID == "a-renamed" {
			t.Fatalf("pinned def must not contain v2's step %q", step.ID)
		}
	}
	if gotRun.RunID != run.RunID {
		t.Fatalf("gotRun.RunID = %q, want %q", gotRun.RunID, run.RunID)
	}
}

func TestLoadRunAndDefLegacyRunFallsBackToLatestPointer(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs): %v", err)
	}

	def := dag.WorkflowDef{Name: "legacy-pin-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	mustPut(t, defKV, def.Name, mustMarshal(t, def))

	run := dag.NewWorkflowRun(def, "run-legacy-1")
	run.DefHash = "" // simulate a pre-#637 snapshot

	orch := NewOrchestrator(nc)
	if err := orch.store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save run: %v", err)
	}

	gotDef, gotRun, err := orch.loadRunAndDef(
		context.Background(), run.RunID,
	)
	if err != nil {
		t.Fatalf("loadRunAndDef: %v", err)
	}
	// Positive: fell back to the by-name pointer and got the def.
	if len(gotDef.Steps) != 1 || gotDef.Steps[0].ID != "a" {
		t.Fatalf("gotDef.Steps = %+v, want by-name def's step \"a\"",
			gotDef.Steps)
	}
	// Negative: the run's DefHash stays empty -- fallback must not
	// mutate the run's identity.
	if gotRun.DefHash != "" {
		t.Fatalf("gotRun.DefHash = %q, want empty", gotRun.DefHash)
	}
}

// TestLoadRunAndDefMissingPinnedVersionFailsLoudly is the review fix
// for blocker 1: a run pinned to a version key that no longer exists
// (evicted past DefVersionsMax, or a retention bug) must FAIL the
// advance with an error, not silently fall back to whatever the
// mutable name -> latest pointer currently holds -- that fallback is
// exactly the #637 hazard (an in-flight run silently re-defined by a
// concurrent re-register) reintroduced through the back door of
// retention. A stuck-but-correct run is the safe failure mode; the
// engine.def_pin.missing_version counter is the operator signal to
// go fix retention, not the run's problem to paper over.
func TestLoadRunAndDefMissingPinnedVersionFailsLoudly(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs): %v", err)
	}

	def := dag.WorkflowDef{Name: "evicted-pin-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	// A DIFFERENT, newer def sits under the mutable pointer -- the
	// silent-fallback bug would have picked THIS up instead of erroring.
	newer := dag.WorkflowDef{Name: "evicted-pin-wf", Version: "2", Steps: []dag.StepDef{
		{ID: "a-newer", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	// Only the mutable pointer exists -- no name.v.hash version key,
	// simulating an evicted-past-DefVersionsMax version.
	mustPut(t, defKV, def.Name, mustMarshal(t, newer))

	run := dag.NewWorkflowRun(def, "run-evicted-1")
	// run.DefHash is set (stamped by NewWorkflowRun) but its version
	// key was never written -- this is the "missing" case.

	orch := NewOrchestrator(nc)
	// Swap in a manual-reader-backed counter for just this instrument
	// so the test can assert on engine.def_pin.missing_version without
	// standing up a full metrics exporter.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	counter, err := mp.Meter("def-pin-test").Int64Counter(
		"engine.def_pin.missing_version",
	)
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	orch.metrics.defPinMissingVersion = counter

	if err := orch.store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save run: %v", err)
	}
	if run.DefHash == "" {
		t.Fatalf("test setup invalid: run.DefHash must be non-empty")
	}

	gotDef, _, err := orch.loadRunAndDef(context.Background(), run.RunID)
	// Positive: the advance FAILS loudly instead of falling back.
	if err == nil {
		t.Fatalf("loadRunAndDef succeeded, want error for missing "+
			"pinned version (got def=%+v)", gotDef)
	}
	// Positive: the error identifies the run and the missing hash, so
	// an operator reading it (or the paired warn log) can act on it.
	if !strings.Contains(err.Error(), run.RunID) ||
		!strings.Contains(err.Error(), run.DefHash) {
		t.Fatalf("error %q must mention run ID %q and def_hash %q",
			err.Error(), run.RunID, run.DefHash)
	}
	// Negative: the run was never silently re-defined under the
	// newer def sitting behind the mutable pointer -- gotDef is the
	// zero value on the error path, never "newer".
	if len(gotDef.Steps) != 0 {
		t.Fatalf("gotDef must be zero-value on error, got %+v", gotDef)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	total := sumInt64Counter(t, &rm, "engine.def_pin.missing_version")
	// Negative: the counter incremented exactly once, not zero (silently
	// swallowed) and not more (double-counted).
	if total != 1 {
		t.Fatalf("engine.def_pin.missing_version total = %d, want 1", total)
	}
}

// sumInt64Counter sums every data point recorded for the named
// Int64Counter metric across every scope in rm. Fails the test if the
// metric was never recorded at all -- a silent no-op must not read as
// "zero, as expected".
func sumInt64Counter(
	t *testing.T, rm *metricdata.ResourceMetrics, name string,
) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is not an Int64 Sum: %T", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	t.Fatalf("metric %q was never recorded", name)
	return 0
}

func TestLoadRunAndDefCorruptPinnedVersionErrors(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs): %v", err)
	}

	def := dag.WorkflowDef{Name: "corrupt-pin-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	run := dag.NewWorkflowRun(def, "run-corrupt-1")

	// Write a DIFFERENT def's bytes under run.DefHash's version key --
	// the key's hash no longer matches the stored content's hash.
	corrupt := dag.WorkflowDef{Name: "corrupt-pin-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "not-a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	versionKey := dag.DefVersionKey(def.Name, run.DefHash)
	mustPut(t, defKV, versionKey, mustMarshal(t, corrupt))
	mustPut(t, defKV, def.Name, mustMarshal(t, def))

	orch := NewOrchestrator(nc)
	if err := orch.store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save run: %v", err)
	}

	_, _, err = orch.loadRunAndDef(context.Background(), run.RunID)
	// Positive: corruption is surfaced as an error.
	if err == nil {
		t.Fatalf("expected corruption error, got nil")
	}
	// Negative: it must not silently succeed with the by-name def
	// either -- the error path returns before any def, confirmed by
	// err != nil above; nothing further to substitute-check.
}

// TestLoadRunAndDefSelfHealsPointerOnlyPreExistingDef is the review
// fix for the migration hazard: on upgrade, every workflow already in
// workflow_defs has ONLY the pointer key -- no name.v.hash version
// was ever written for it (RegisterWorkflow didn't exist yet). A run
// started against such a def still gets DefHash stamped
// unconditionally (dag.NewWorkflowRun), so its first advance after
// the in-memory def is exhausted would hit the missing-pinned-version
// path. Failing loudly there would dead-letter every such workflow's
// runs until each one is re-registered -- unacceptable on upgrade.
//
// The fix: when the version key is missing, read the mutable pointer
// and compare its content hash to run.DefHash. Equal hashes prove the
// pointer's content IS byte-identical to what the run is pinned to --
// not "probably fine", but the same guarantee a version-key hit would
// have given. Self-heal by writing the version key (Create, tolerant
// of a racing writer) and proceed normally. Only a HASH MISMATCH
// falls through to the loud error -- verified separately by
// TestLoadRunAndDefMissingPinnedVersionFailsLoudly, which pins to a
// hash that does NOT match the pointer's content.
func TestLoadRunAndDefSelfHealsPointerOnlyPreExistingDef(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs): %v", err)
	}

	def := dag.WorkflowDef{Name: "pre-637-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	// Simulate a pre-#637 registration: ONLY the mutable pointer
	// exists. No name.v.hash version key was ever written.
	mustPut(t, defKV, def.Name, mustMarshal(t, def))

	run := dag.NewWorkflowRun(def, "run-pre-637-1")
	// run.DefHash is stamped unconditionally and, since it was built
	// from the exact same def the pointer holds, equals its content
	// hash -- the guard condition the self-heal checks.
	if run.DefHash != dag.DefHash(def) {
		t.Fatalf("test setup invalid: run.DefHash must equal DefHash(def)")
	}

	orch := NewOrchestrator(nc)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	counter, err := mp.Meter("self-heal-test").Int64Counter(
		"engine.def_pin.missing_version",
	)
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	orch.metrics.defPinMissingVersion = counter

	if err := orch.store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save run: %v", err)
	}

	versionKey := dag.DefVersionKey(def.Name, run.DefHash)
	if _, err := defKV.Get(versionKey); err == nil {
		t.Fatalf("test setup invalid: version key must not pre-exist")
	}

	// First advance: self-heals.
	gotDef, _, err := orch.loadRunAndDef(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("loadRunAndDef (1st advance): %v, want self-heal success", err)
	}
	if len(gotDef.Steps) != 1 || gotDef.Steps[0].ID != "a" {
		t.Fatalf("gotDef.Steps = %+v, want def's step \"a\"", gotDef.Steps)
	}
	// Positive: the version key now exists, written by the self-heal.
	entry, err := defKV.Get(versionKey)
	if err != nil {
		t.Fatalf("version key %q not created by self-heal: %v", versionKey, err)
	}
	var healed dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &healed); err != nil {
		t.Fatalf("unmarshal self-healed version: %v", err)
	}
	if dag.DefHash(healed) != run.DefHash {
		t.Fatalf("self-healed version content hash mismatch")
	}

	// Second advance: reads the now-existing version key directly
	// (completes the "advance it twice" scenario the review asked for).
	gotDef2, _, err := orch.loadRunAndDef(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("loadRunAndDef (2nd advance): %v", err)
	}
	if len(gotDef2.Steps) != 1 || gotDef2.Steps[0].ID != "a" {
		t.Fatalf("gotDef2.Steps = %+v, want def's step \"a\"", gotDef2.Steps)
	}

	// Negative: self-heal must NEVER count as a missing-version
	// failure -- it's a recognized, verified-safe migration path, not
	// an operator-facing problem.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "engine.def_pin.missing_version" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric is not an Int64 Sum: %T", m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			if total != 0 {
				t.Fatalf("engine.def_pin.missing_version = %d, want 0", total)
			}
		}
	}
}

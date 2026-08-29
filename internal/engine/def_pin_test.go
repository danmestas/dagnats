// internal/engine/def_pin_test.go
//
// White-box unit tests for loadRunAndDef / loadPinnedOrLatestDef
// (#637): a run pinned via DefHash must read its immutable
// name.v.hash version instead of the mutable name -> latest pointer,
// a legacy run (DefHash=="") must fall back to the pointer, a
// missing pinned version must fall back too (not error), and a
// corrupt pinned version (content hash mismatch) must error rather
// than silently substitute something else.
// Methodology: real embedded NATS server per test; direct KV writes
// simulate both the immutable version key and the mutable pointer so
// each scenario is constructed precisely, then loadRunAndDef is
// called directly (same package -- white box). Bounded via the
// server's own request timeouts; no unbounded waits.
package engine

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
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
	defKV, _ := js.KeyValue("workflow_defs")

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
	defKV, _ := js.KeyValue("workflow_defs")

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

func TestLoadRunAndDefMissingPinnedVersionFallsBackByName(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	def := dag.WorkflowDef{Name: "evicted-pin-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	// Only the mutable pointer exists -- no name.v.hash version key,
	// simulating an evicted-past-DefVersionsMax version.
	mustPut(t, defKV, def.Name, mustMarshal(t, def))

	run := dag.NewWorkflowRun(def, "run-evicted-1")
	// run.DefHash is set (stamped by NewWorkflowRun) but its version
	// key was never written -- this is the "missing" case.

	orch := NewOrchestrator(nc)
	if err := orch.store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save run: %v", err)
	}

	gotDef, _, err := orch.loadRunAndDef(context.Background(), run.RunID)
	// Positive: falls back rather than erroring.
	if err != nil {
		t.Fatalf("loadRunAndDef: %v, want fallback success", err)
	}
	if len(gotDef.Steps) != 1 || gotDef.Steps[0].ID != "a" {
		t.Fatalf("gotDef.Steps = %+v, want fallback def's step \"a\"",
			gotDef.Steps)
	}
	// Negative: run.DefHash was non-empty, proving this exercised the
	// fallback branch and not the legacy (DefHash=="") branch.
	if run.DefHash == "" {
		t.Fatalf("test setup invalid: run.DefHash must be non-empty")
	}
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
	defKV, _ := js.KeyValue("workflow_defs")

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

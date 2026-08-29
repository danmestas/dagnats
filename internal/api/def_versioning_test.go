// api/def_versioning_test.go
//
// Tests for the workflow_defs def-version retention cap (#637):
// DefVersionsMax retained versions per workflow name, oldest-first
// eviction of versions no non-terminal run references, and a 409
// refusal (via POST /workflows) when every retained version is still
// live. Also covers the version-key/latest-pointer bucket-sharing
// contract: ListWorkflows must not surface version keys as if they
// were registered workflows.
// Methodology: real embedded NATS server + Service (no orchestrator
// needed -- "live" runs are constructed directly and saved via the
// snapshot store, since retention only reads run snapshots, not the
// event-driven advance path). Every test asserts positive AND
// negative space. Bounded loops (<=33 iterations).
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// versionedDef returns a distinct-content def named name: n steps,
// each a unique ID, so DefHash differs for every n.
func versionedDef(name string, n int) dag.WorkflowDef {
	if name == "" {
		panic("versionedDef: name must not be empty")
	}
	if n <= 0 {
		panic("versionedDef: n must be positive")
	}
	steps := make([]dag.StepDef, n)
	for i := 0; i < n; i++ {
		steps[i] = dag.StepDef{
			ID: fmt.Sprintf("step-%d", i), Task: "t", Type: dag.StepTypeNormal,
		}
	}
	return dag.WorkflowDef{Name: name, Version: "1", Steps: steps}
}

// saveLiveRun constructs and saves a non-terminal WorkflowRun for
// workflowName pinned to defHash, bypassing the orchestrator entirely
// -- reserveDefVersionSlot only reads run snapshots via ListRecent,
// so this is sufficient to make a version "live" for retention.
func saveLiveRun(
	t *testing.T, store *engine.SnapshotStore, workflowName, defHash, runID string,
) {
	t.Helper()
	run := dag.WorkflowRun{
		RunID: runID, WorkflowID: workflowName,
		Status: dag.RunStatusRunning, DefHash: defHash,
		Steps: map[string]dag.StepState{"step-0": {Status: dag.StepStatusPending}},
	}
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("saveLiveRun: %v", err)
	}
}

func TestDefVersionRetentionEvictsOldestWhenNoLiveRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	const wfName = "retention-wf"
	var oldest, newest dag.WorkflowDef
	for i := 1; i <= DefVersionsMax+1; i++ {
		def := versionedDef(wfName, i)
		if i == 1 {
			oldest = def
		}
		if i == DefVersionsMax+1 {
			newest = def
		}
		if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
			t.Fatalf("RegisterWorkflow #%d: %v", i, err)
		}
	}

	versionKeys, err := svc.defVersionKeysForName(context.Background(), wfName)
	if err != nil {
		t.Fatalf("defVersionKeysForName: %v", err)
	}
	// Positive: exactly DefVersionsMax retained, never more.
	if len(versionKeys) != DefVersionsMax {
		t.Fatalf("retained versions = %d, want %d",
			len(versionKeys), DefVersionsMax)
	}
	// Negative: the oldest version was evicted.
	oldestKey := dag.DefVersionKey(wfName, dag.DefHash(oldest))
	if _, err := svc.defKV.Get(context.Background(), oldestKey); err == nil {
		t.Fatalf("oldest version key %q still present, want evicted", oldestKey)
	}
	// The latest pointer must reflect the most recently registered def.
	got, err := svc.GetWorkflow(wfName)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if len(got.Steps) != len(newest.Steps) {
		t.Fatalf("latest pointer has %d steps, want %d (newest)",
			len(got.Steps), len(newest.Steps))
	}
}

func TestDefVersionRetentionSurvivesLiveOldestAndEvictsOther(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	store := svc.store

	const wfName = "retention-live-oldest-wf"
	var oldest dag.WorkflowDef
	for i := 1; i <= DefVersionsMax; i++ {
		def := versionedDef(wfName, i)
		if i == 1 {
			oldest = def
		}
		if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
			t.Fatalf("RegisterWorkflow #%d: %v", i, err)
		}
	}
	// Pin ONE live run to the oldest version -- every other retained
	// version stays unreferenced and therefore evictable.
	saveLiveRun(t, store, wfName, dag.DefHash(oldest), "live-run-oldest")

	newest := versionedDef(wfName, DefVersionsMax+1)
	if err := svc.RegisterWorkflow(context.Background(), newest); err != nil {
		t.Fatalf("RegisterWorkflow newest (should evict a non-live "+
			"version, not refuse): %v", err)
	}

	oldestKey := dag.DefVersionKey(wfName, dag.DefHash(oldest))
	// Positive: the live-pinned oldest version survived.
	if _, err := svc.defKV.Get(context.Background(), oldestKey); err != nil {
		t.Fatalf("live oldest version key evicted: %v", err)
	}
	versionKeys, err := svc.defVersionKeysForName(context.Background(), wfName)
	if err != nil {
		t.Fatalf("defVersionKeysForName: %v", err)
	}
	// Negative: total retained count stays at the cap, not cap+1.
	if len(versionKeys) != DefVersionsMax {
		t.Fatalf("retained versions = %d, want %d",
			len(versionKeys), DefVersionsMax)
	}
}

func TestPostWorkflowsRefuses409WhenAllRetainedVersionsAreLive(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	store := svc.store
	handler := NewRESTHandler(svc)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	const wfName = "retention-all-live-wf"
	for i := 1; i <= DefVersionsMax; i++ {
		def := versionedDef(wfName, i)
		if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
			t.Fatalf("RegisterWorkflow #%d: %v", i, err)
		}
		// Pin a live run to EVERY retained version -- none is evictable.
		saveLiveRun(t, store, wfName, dag.DefHash(def),
			fmt.Sprintf("live-run-%d", i))
	}

	newest := versionedDef(wfName, DefVersionsMax+1)
	body, err := json.Marshal(newest)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(
		server.URL+"/workflows", "application/json", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /workflows: %v", err)
	}
	defer resp.Body.Close()
	// Positive: refused with 409, not silently evicting a live version.
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	var decoded struct {
		Error        string `json:"error"`
		LiveVersions int    `json:"live_versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode 409 body: %v", err)
	}
	if decoded.Error != "too many live workflow versions" {
		t.Fatalf("error = %q, want %q",
			decoded.Error, "too many live workflow versions")
	}
	// Negative: live_versions reports the actual retained count, not 0.
	if decoded.LiveVersions != DefVersionsMax {
		t.Fatalf("live_versions = %d, want %d",
			decoded.LiveVersions, DefVersionsMax)
	}
}

func TestRegisterWorkflowRejectsVersionKeyShapedName(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	def := versionedDef("orders", 1)
	trap := dag.WorkflowDef{
		Name:  dag.DefVersionKey("orders", dag.DefHash(def)),
		Steps: []dag.StepDef{{ID: "a", Task: "t", Type: dag.StepTypeNormal}},
	}
	err := svc.RegisterWorkflow(context.Background(), trap)
	// Positive: a name shaped like a version key is refused.
	if err == nil {
		t.Fatalf("expected error registering a version-key-shaped name")
	}
	// Negative: an ordinary name still registers fine.
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow(ordinary name): %v", err)
	}
}

func TestListWorkflowsExcludesVersionKeys(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	v1 := versionedDef("list-wf", 1)
	v2 := versionedDef("list-wf", 2)
	if err := svc.RegisterWorkflow(context.Background(), v1); err != nil {
		t.Fatalf("RegisterWorkflow v1: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), v2); err != nil {
		t.Fatalf("RegisterWorkflow v2: %v", err)
	}

	defs, err := svc.ListWorkflows(context.Background())
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	// Positive: exactly one entry for "list-wf" (the latest pointer).
	count := 0
	for _, def := range defs {
		if def.Name == "list-wf" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("ListWorkflows returned %d entries named %q, want 1",
			count, "list-wf")
	}
	// Negative: no entry's name looks like a version key (i.e. none
	// leaked the immutable name.v.hash snapshot as a "workflow").
	for _, def := range defs {
		if dag.IsDefVersionKey(def.Name) {
			t.Fatalf("ListWorkflows leaked a version key as a name: %q",
				def.Name)
		}
	}
}

// TestReserveDefVersionSlotErrorType is a narrow unit check that the
// refusal error is the typed *ErrTooManyLiveWorkflowVersions
// errors.As can unwrap -- the REST layer depends on this to map to
// 409 instead of a generic 400.
func TestReserveDefVersionSlotErrorType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	store := svc.store

	const wfName = "typed-error-wf"
	for i := 1; i <= DefVersionsMax; i++ {
		def := versionedDef(wfName, i)
		if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
			t.Fatalf("RegisterWorkflow #%d: %v", i, err)
		}
		saveLiveRun(t, store, wfName, dag.DefHash(def), fmt.Sprintf("run-%d", i))
	}

	err := svc.RegisterWorkflow(
		context.Background(), versionedDef(wfName, DefVersionsMax+1),
	)
	var typed *ErrTooManyLiveWorkflowVersions
	// Positive: errors.As succeeds.
	if !errors.As(err, &typed) {
		t.Fatalf("error is not *ErrTooManyLiveWorkflowVersions: %v", err)
	}
	// Negative: the name on the typed error matches the workflow, not
	// some other identifier.
	if typed.Name != wfName {
		t.Fatalf("typed.Name = %q, want %q", typed.Name, wfName)
	}
}

// TestReserveDefVersionSlotNeverEvictsCurrentPointerVersion is the
// reviewer's repro for the second half of blocker 1: re-registering
// byte-identical content for the OLDEST retained version moves the
// mutable name pointer back to that oldest version (by KV revision,
// it's still the "oldest" version key even though it's now what the
// pointer references). A retention pass that picks purely by revision
// would then evict the very version the pointer points at on the next
// register -- deleting the pointer's own target. oldestEvictableVersion
// must exclude whatever version the pointer currently references.
func TestReserveDefVersionSlotNeverEvictsCurrentPointerVersion(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	const wfName = "retention-pointer-guard-wf"
	var oldest dag.WorkflowDef
	for i := 1; i <= DefVersionsMax; i++ {
		def := versionedDef(wfName, i)
		if i == 1 {
			oldest = def
		}
		if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
			t.Fatalf("RegisterWorkflow #%d: %v", i, err)
		}
	}
	// Re-register the oldest version's byte-identical content -- a
	// no-growth, no-eviction no-op for the version population, but it
	// DOES move the mutable pointer back to the oldest version.
	if err := svc.RegisterWorkflow(context.Background(), oldest); err != nil {
		t.Fatalf("re-register oldest: %v", err)
	}
	got, err := svc.GetWorkflow(wfName)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if len(got.Steps) != len(oldest.Steps) {
		t.Fatalf("pointer not moved back to oldest: got %d steps, want %d",
			len(got.Steps), len(oldest.Steps))
	}

	// One more register at the cap: must NOT evict the oldest version,
	// because it's what the pointer currently references.
	newest := versionedDef(wfName, DefVersionsMax+1)
	if err := svc.RegisterWorkflow(context.Background(), newest); err != nil {
		t.Fatalf("RegisterWorkflow newest: %v", err)
	}
	oldestKey := dag.DefVersionKey(wfName, dag.DefHash(oldest))
	// Positive: the pointer's own version survived.
	if _, err := svc.defKV.Get(context.Background(), oldestKey); err != nil {
		t.Fatalf("pointer's own version key evicted: %v", err)
	}
	// Negative: the retained count stayed at the cap (something else
	// was evicted instead), proving this wasn't a silent no-op.
	versionKeys, err := svc.defVersionKeysForName(context.Background(), wfName)
	if err != nil {
		t.Fatalf("defVersionKeysForName: %v", err)
	}
	if len(versionKeys) != DefVersionsMax {
		t.Fatalf("retained versions = %d, want %d",
			len(versionKeys), DefVersionsMax)
	}
}

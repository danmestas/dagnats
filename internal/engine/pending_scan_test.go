// internal/engine/pending_scan_test.go
// Verifies findOldestPendingRun is bounded, filtered, and best-effort
// (#523). Methodology: red-green TDD against a real embedded NATS KV. Each
// test asserts a positive outcome (a pending run is still surfaced) and a
// negative property (a non-run key does not derail the scan; a run
// population past the cap degrades LOUDLY rather than silently stalling).
// The production failure was a 228k-key workflow_runs bucket where one slow
// GET made a per-completion scan start ZERO runs.
package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func seedPendingRun(
	t *testing.T, o *Orchestrator, runID, wfID string, created time.Time,
) {
	t.Helper()
	seedRun(t, o, runID, wfID, dag.RunStatusPending, created)
}

func seedRun(
	t *testing.T, o *Orchestrator, runID, wfID string,
	status dag.RunStatus, created time.Time,
) {
	t.Helper()
	run := dag.WorkflowRun{
		RunID:      runID,
		WorkflowID: wfID,
		Status:     status,
		CreatedAt:  created,
	}
	if err := o.store.Save(context.Background(), run); err != nil {
		t.Fatalf("seed run %s: %v", runID, err)
	}
}

func TestFindOldestPendingRunIgnoresNonRunKeys(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc)
	ctx := context.Background()

	seedPendingRun(t, orch, "pending-1", "wf-a", time.Now().UTC())

	// A non-run key in the workflow_runs bucket must be filtered out
	// BEFORE any GET — its garbage value must never derail the scan.
	if _, err := orch.store.kv.Put(
		ctx, "notarun.garbage", []byte("{not json")); err != nil {
		t.Fatalf("put non-run key: %v", err)
	}

	runID, found, err := orch.findOldestPendingRun(ctx, "wf-a")
	if err != nil {
		t.Fatalf("findOldestPendingRun: %v", err)
	}
	// Positive: the pending run is surfaced despite the noise key.
	if !found || runID != "pending-1" {
		t.Fatalf("found=%v runID=%q, want true/pending-1", found, runID)
	}
	// Negative: no pending run for an unrelated workflow.
	_, foundOther, err := orch.findOldestPendingRun(ctx, "wf-none")
	if err != nil {
		t.Fatalf("findOldestPendingRun(wf-none): %v", err)
	}
	if foundOther {
		t.Fatal("found a pending run for a workflow with none")
	}
}

func TestFindOldestPendingRunReturnsOldest(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc)
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour)
	seedPendingRun(t, orch, "newer", "wf-a", base.Add(30*time.Minute))
	seedPendingRun(t, orch, "oldest", "wf-a", base)
	seedPendingRun(t, orch, "mid", "wf-a", base.Add(10*time.Minute))

	runID, found, err := orch.findOldestPendingRun(ctx, "wf-a")
	if err != nil {
		t.Fatalf("findOldestPendingRun: %v", err)
	}
	// Positive: the genuinely oldest pending run wins.
	if !found || runID != "oldest" {
		t.Fatalf("found=%v runID=%q, want true/oldest", found, runID)
	}
	// Negative: a different workflow's pending run is not returned.
	seedPendingRun(t, orch, "other-wf", "wf-b", base.Add(-time.Hour))
	runID, _, err = orch.findOldestPendingRun(ctx, "wf-a")
	if err != nil {
		t.Fatalf("findOldestPendingRun after wf-b seed: %v", err)
	}
	if runID == "other-wf" {
		t.Fatal("returned a run from the wrong workflow")
	}
}

func TestFindOldestPendingRunCapDegradesLoudly(t *testing.T) {
	// #523: capping the fetched keys is safe ONLY IF operators are told,
	// otherwise a pending run permanently outside the window is a silent
	// slow-motion stall. This test makes the found assertion LOAD-BEARING:
	// the cap window is filled with terminal + other-workflow keys and the
	// sole target pending wf-a run must be the one truncation retains and
	// selection surfaces. NATS KV Keys() returns keys lexicographically, so
	// RunID prefixes ("a-".."e-") deterministically place the target inside
	// the first pendingRunScanMax keys — if truncation kept the wrong window
	// or the filter/selection regressed, found flips to false.
	prevCap := pendingRunScanMax
	pendingRunScanMax = 3
	t.Cleanup(func() { pendingRunScanMax = prevCap })

	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc)
	ctx := context.Background()

	// 5 run keys > cap of 3. Sorted window = [a-target, b-term, c-other].
	// Only a-target is Pending AND wf-a, so it alone can satisfy found.
	base := time.Now().UTC().Add(-time.Hour)
	seedPendingRun(t, orch, "a-target", "wf-a", base)
	seedRun(t, orch, "b-term", "wf-a", dag.RunStatusCompleted, base)
	seedPendingRun(t, orch, "c-other", "wf-b", base)
	seedRun(t, orch, "d-term", "wf-a", dag.RunStatusFailed, base)
	seedPendingRun(t, orch, "e-other", "wf-b", base)

	buf, restore := captureSlog(t)
	defer restore()

	runID, found, err := orch.findOldestPendingRun(ctx, "wf-a")
	if err != nil {
		t.Fatalf("findOldestPendingRun: %v", err)
	}
	// Positive: truncation retained the target and selection surfaced it.
	if !found || runID != "a-target" {
		t.Fatalf("found=%v runID=%q, want true/a-target "+
			"(truncation/selection regressed)", found, runID)
	}
	// Negative: the degradation is LOUD, not silent.
	logs := buf.String()
	if !strings.Contains(logs, "pending scan degraded") {
		t.Fatalf("want WARN \"pending scan degraded\" when capped, got:\n%s",
			logs)
	}
	if !strings.Contains(logs, "level=WARN") {
		t.Fatalf("degradation must log at WARN, got:\n%s", logs)
	}
}

func TestFindOldestPendingRunUnderCapFindsAllAndIsQuiet(t *testing.T) {
	// The common/healthy case: population <= cap. Every pending run is
	// fetched, the genuine oldest is returned even when surrounded by
	// terminal + other-workflow noise, and NO degradation is logged.
	prevCap := pendingRunScanMax
	pendingRunScanMax = 100
	t.Cleanup(func() { pendingRunScanMax = prevCap })

	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := NewOrchestrator(nc)
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour)
	seedPendingRun(t, orch, "p-newer", "wf-a", base.Add(20*time.Minute))
	seedPendingRun(t, orch, "p-oldest", "wf-a", base)
	seedRun(t, orch, "t-term", "wf-a", dag.RunStatusCompleted, base)
	seedPendingRun(t, orch, "o-other", "wf-b", base.Add(-time.Hour))

	buf, restore := captureSlog(t)
	defer restore()

	runID, found, err := orch.findOldestPendingRun(ctx, "wf-a")
	if err != nil {
		t.Fatalf("findOldestPendingRun: %v", err)
	}
	// Positive: the genuine oldest pending wf-a run is found under cap.
	if !found || runID != "p-oldest" {
		t.Fatalf("found=%v runID=%q, want true/p-oldest", found, runID)
	}
	// Negative: the healthy path is SILENT — no false degradation alarm.
	if strings.Contains(buf.String(), "pending scan degraded") {
		t.Fatalf("under-cap scan must not warn; got:\n%s", buf.String())
	}
}

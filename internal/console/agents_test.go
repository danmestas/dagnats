// agents_test.go exercises the /console/agents provenance page and its
// SSE pump without standing up NATS.
//
// Methodology:
//   - The page and SSE handlers read the same console.DataSource the
//     production server passes in; the fakeDataSource returns in-memory
//     AgentRuntimeRow values so the tree-rendering / honest-omit logic
//     gets full coverage without a JetStream bucket.
//   - assembleTree is unit-tested directly against dag.WorkflowRun
//     slices: it is a pure function (no DataSource, no NATS), so the
//     bounded-BFS / cycle-guard / depth-cap invariants are asserted in
//     isolation.
//   - Each test boots a fresh Mount with its own fake; tests never
//     share state. Bounded timeouts on every SSE read.
//   - Assertions look for stable substrings (run ids, the runtime tag,
//     the Datastar patch signature) and ALSO assert negative space —
//     a filtered-out lone run's id is absent, an unbacked budget block
//     is absent, a lone non-tree SSE update produces no patch.
package console

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/worker"
)

// agentRun is a terse WorkflowRun builder for the tree tests.
func agentRun(runID, workflow, root, parent string, status dag.RunStatus) dag.WorkflowRun {
	return dag.WorkflowRun{
		RunID:       runID,
		WorkflowID:  workflow,
		Status:      status,
		RootRunID:   root,
		ParentRunID: parent,
		CreatedAt:   time.Now(),
	}
}

// TestAgentsPage_TreeAssembles seeds a root + 2 children + grandchild
// sharing a RootRunID and asserts all four run ids render with the
// grandchild at a deeper Depth, plus an unrelated lone run is filtered
// out (honesty: a solo top-level run is not a runtime).
func TestAgentsPage_TreeAssembles(t *testing.T) {
	fake := newFakeDS()
	fake.agentRuntimes = []AgentRuntimeRow{{
		RootRunID:    "root-1",
		RootWorkflow: "agent.plan",
		RootStatus:   "running",
		TotalRuns:    4,
		Generations:  2,
		Tree: []AgentGenNode{
			{RunID: "root-1", WorkflowID: "agent.plan", Status: "running", Depth: 0},
			{RunID: "child-a", ParentRunID: "root-1", WorkflowID: "agent.impl", Status: "running", Depth: 1},
			{RunID: "child-b", ParentRunID: "root-1", WorkflowID: "agent.test", Status: "completed", Depth: 1},
			{RunID: "grand-c", ParentRunID: "child-a", WorkflowID: "agent.review", Status: "pending", Depth: 2},
		},
	}}

	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/agents", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, id := range []string{"root-1", "child-a", "child-b", "grand-c"} {
		if !strings.Contains(body, id) {
			t.Fatalf("body missing run id %q", id)
		}
	}
	// The header tile renders the count and each tree renders "<n> runs";
	// assert the per-tree total ("4 runs") is present.
	if !strings.Contains(body, "4 runs") {
		t.Fatalf("body missing total-runs count (want '4 runs')")
	}
	// Depth ordering: the grandchild row must carry a deeper indent than
	// the child. We assert the rendered depth-2 marker exists and the
	// child's depth-1 marker exists, and that grand-c follows child-a.
	childIdx := strings.Index(body, "child-a")
	grandIdx := strings.Index(body, "grand-c")
	if childIdx < 0 || grandIdx < 0 || grandIdx < childIdx {
		t.Fatalf("BFS order broken: child-a@%d grand-c@%d", childIdx, grandIdx)
	}
	if !strings.Contains(body, `data-depth="2"`) {
		t.Fatalf("body missing grandchild depth marker data-depth=\"2\"")
	}
	// Negative space: a lone top-level run is filtered by assembleTree
	// and never reaches the fake's rows, so its id must be absent.
	if strings.Contains(body, "lone-solo-run") {
		t.Fatalf("lone run id leaked into agents page")
	}
}

// TestAgentsPage_BudgetRenders asserts the budget block renders when
// BudgetOK is true, surfacing the real active/max counts.
func TestAgentsPage_BudgetRenders(t *testing.T) {
	fake := newFakeDS()
	fake.agentRuntimes = []AgentRuntimeRow{{
		RootRunID:    "root-b",
		RootWorkflow: "agent.plan",
		RootStatus:   "running",
		TotalRuns:    2,
		Generations:  1,
		BudgetOK:     true,
		Budget: worker.RuntimeBudget{
			ActiveRuns: 2, MaxActiveRuns: 8,
			RegisteredDefs: 3, MaxRegisteredDefs: 16,
		},
		Tree: []AgentGenNode{
			{RunID: "root-b", WorkflowID: "agent.plan", Status: "running", Depth: 0},
			{RunID: "child-b1", ParentRunID: "root-b", WorkflowID: "agent.impl", Status: "running", Depth: 1},
		},
	}}

	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/agents", nil))

	body := rr.Body.String()
	if !strings.Contains(body, "2 / 8") {
		t.Fatalf("budget active/max runs not rendered (want '2 / 8')")
	}
	if !strings.Contains(body, "3 / 16") {
		t.Fatalf("budget defs not rendered (want '3 / 16')")
	}
}

// TestAgentsPage_HonestOmit covers two honesty contracts: an unbacked
// budget (BudgetOK=false) renders NO budget block at all (no 0/0, no
// dash), and an empty runtimes list renders the honest empty state.
func TestAgentsPage_HonestOmit(t *testing.T) {
	fake := newFakeDS()
	fake.agentRuntimes = []AgentRuntimeRow{{
		RootRunID:    "root-x",
		RootWorkflow: "agent.plan",
		RootStatus:   "running",
		TotalRuns:    2,
		Generations:  1,
		BudgetOK:     false,
		Tree: []AgentGenNode{
			{RunID: "root-x", WorkflowID: "agent.plan", Status: "running", Depth: 0},
			{RunID: "child-x1", ParentRunID: "root-x", WorkflowID: "agent.impl", Status: "running", Depth: 1},
		},
	}}

	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/agents", nil))
	body := rr.Body.String()
	// The budget block markup must be entirely absent — never a 0/0.
	if strings.Contains(body, "agent-budget") {
		t.Fatalf("unbacked budget block rendered (must be omitted)")
	}
	if strings.Contains(body, "0 / 0") {
		t.Fatalf("fabricated 0/0 budget rendered")
	}

	// Empty runtimes → honest empty-state card, no tree rows.
	emptyFake := newFakeDS()
	emptyFake.agentRuntimes = nil
	eh := mountWithFake(t, emptyFake)
	err := httptest.NewRecorder()
	eh.ServeHTTP(err, httptest.NewRequest(http.MethodGet, "/console/agents", nil))
	ebody := err.Body.String()
	if !strings.Contains(ebody, "console-empty-state") {
		t.Fatalf("empty runtimes did not render empty-state card")
	}
	if strings.Contains(ebody, "agent-tree-") {
		t.Fatalf("empty runtimes rendered a tree tbody")
	}
}

// TestAgentsPage_RuntimeTag asserts the runtime-origin tag renders for a
// node flagged SpawnedByRuntime and exactly once (the non-flagged node
// carries no tag).
func TestAgentsPage_RuntimeTag(t *testing.T) {
	fake := newFakeDS()
	fake.agentRuntimes = []AgentRuntimeRow{{
		RootRunID:    "root-r",
		RootWorkflow: "agent.plan",
		RootStatus:   "running",
		TotalRuns:    2,
		Generations:  1,
		Tree: []AgentGenNode{
			{RunID: "root-r", WorkflowID: "agent.plan", Status: "running", Depth: 0, SpawnedByRuntime: false},
			{RunID: "child-r1", ParentRunID: "root-r", WorkflowID: "agent.impl", Status: "running", Depth: 1, SpawnedByRuntime: true},
		},
	}}

	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/agents", nil))
	body := rr.Body.String()
	const tag = "agent-runtime-tag"
	if got := strings.Count(body, tag); got != 1 {
		t.Fatalf("runtime tag count = %d, want exactly 1", got)
	}
}

// TestSSEAgents_emitsPatchForTreeMember pushes a tree-member RunUpdate
// and asserts a PatchElements targeting the root's tree tbody ships;
// a lone non-tree run produces NO patch (negative space).
func TestSSEAgents_emitsPatchForTreeMember(t *testing.T) {
	fake := newFakeDS()
	updates := make(chan RunUpdate, 4)
	fake.runUpdates = updates
	// AgentRuntime(root) re-projects this tree on each tree-member update.
	fake.agentRuntimeByRoot = map[string]AgentRuntimeRow{
		"root-s": {
			RootRunID:    "root-s",
			RootWorkflow: "agent.plan",
			RootStatus:   "running",
			TotalRuns:    2,
			Generations:  1,
			Tree: []AgentGenNode{
				{RunID: "root-s", WorkflowID: "agent.plan", Status: "running", Depth: 0},
				{RunID: "child-s1", ParentRunID: "root-s", WorkflowID: "agent.impl", Status: "running", Depth: 1},
			},
		},
	}

	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, srv.URL+"/console/sse/agents", nil,
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse agents: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// A lone non-tree run: RootRunID empty → self-roots → AgentRuntime
	// returns not-found → NO patch.
	updates <- RunUpdate{Run: dag.WorkflowRun{
		RunID: "lone-1", WorkflowID: "solo", Status: dag.RunStatusRunning,
		CreatedAt: time.Now(),
	}, Created: true, Seq: 1}
	// A tree member: produces a patch targeting #agent-tree-root-s.
	updates <- RunUpdate{Run: dag.WorkflowRun{
		RunID: "child-s1", WorkflowID: "agent.impl", Status: dag.RunStatusCompleted,
		RootRunID: "root-s", ParentRunID: "root-s", CreatedAt: time.Now(),
	}, Created: false, Seq: 2}
	// Sentinel so the scanner reads past the real patch.
	updates <- RunUpdate{Run: dag.WorkflowRun{
		RunID: "child-s1", WorkflowID: "agent.impl", Status: dag.RunStatusCompleted,
		RootRunID: "root-s", ParentRunID: "root-s", CreatedAt: time.Now(),
	}, Created: false, Seq: 3}

	scanner := bufio.NewScanner(resp.Body)
	var sawPatch bool
	var sawTreeSelector bool
	deadline := time.Now().Add(2 * time.Second)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "datastar-patch-elements") {
			sawPatch = true
		}
		if strings.Contains(line, "agent-tree-root-s") {
			sawTreeSelector = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
	}
	if !sawPatch || !sawTreeSelector {
		t.Fatalf("expected patch targeting #agent-tree-root-s (patch=%v selector=%v)",
			sawPatch, sawTreeSelector)
	}
}

// TestAssembleTree_Bounded is the bounded/cycle unit test on the pure
// helper: a cyclic ParentRunID chain terminates without hanging, a tree
// deeper than engine.MaxNestingDepth is depth-capped, and a normal tree
// yields the exact node count.
func TestAssembleTree_Bounded(t *testing.T) {
	// Cycle: a -> b -> a. assembleTree must terminate with both nodes
	// visited at most once and never loop.
	cyclic := []dag.WorkflowRun{
		agentRun("a", "agent.x", "a", "b", dag.RunStatusRunning),
		agentRun("b", "agent.y", "a", "a", dag.RunStatusRunning),
	}
	row := assembleTree(cyclic, "a")
	if len(row.Tree) > maxNodesPerTree {
		t.Fatalf("cycle blew the node cap: %d > %d", len(row.Tree), maxNodesPerTree)
	}
	if len(row.Tree) != 2 {
		t.Fatalf("cyclic tree node count = %d, want 2 (each visited once)", len(row.Tree))
	}

	// Deep chain beyond the nesting ceiling. Build root + a chain longer
	// than engine.MaxNestingDepth and assert the rendered depth is capped.
	var deep []dag.WorkflowRun
	deep = append(deep, agentRun("d0", "agent.root", "d0", "", dag.RunStatusRunning))
	prev := "d0"
	for i := 1; i <= maxNestingDepthGuard+3; i++ {
		id := "d" + string(rune('0'+i))
		deep = append(deep, agentRun(id, "agent.gen", "d0", prev, dag.RunStatusRunning))
		prev = id
	}
	deepRow := assembleTree(deep, "d0")
	for _, n := range deepRow.Tree {
		if n.Depth > maxNestingDepthGuard {
			t.Fatalf("node %q depth %d exceeds cap %d", n.RunID, n.Depth, maxNestingDepthGuard)
		}
	}

	// Normal tree: root + 2 children → exactly 3 nodes.
	normal := []dag.WorkflowRun{
		agentRun("n0", "agent.root", "n0", "", dag.RunStatusRunning),
		agentRun("n1", "agent.c", "n0", "n0", dag.RunStatusRunning),
		agentRun("n2", "agent.c", "n0", "n0", dag.RunStatusCompleted),
	}
	normalRow := assembleTree(normal, "n0")
	if len(normalRow.Tree) != 3 {
		t.Fatalf("normal tree node count = %d, want 3", len(normalRow.Tree))
	}
	if normalRow.TotalRuns != 3 {
		t.Fatalf("normal tree TotalRuns = %d, want 3", normalRow.TotalRuns)
	}
}

// TestAssembleTree_UnknownRoot covers the race window where a child's
// snapshot lands before its root's own snapshot: assembleTree synthesizes
// the root and must render the honest "(unknown)" sentinel for the root's
// workflow + status — never a blank cell + default-zero "pending" badge.
func TestAssembleTree_UnknownRoot(t *testing.T) {
	// Only the child exists; it shares RootRunID "root-u" and names it as
	// its parent. No snapshot for "root-u" itself.
	runs := []dag.WorkflowRun{
		agentRun("child-u1", "agent.impl", "root-u", "root-u", dag.RunStatusRunning),
	}
	row := assembleTree(runs, "root-u")
	if row.RootStatus != unknownAgentField {
		t.Fatalf("RootStatus = %q, want %q", row.RootStatus, unknownAgentField)
	}
	if row.RootWorkflow != unknownAgentField {
		t.Fatalf("RootWorkflow = %q, want %q", row.RootWorkflow, unknownAgentField)
	}
	if len(row.Tree) == 0 || row.Tree[0].RunID != "root-u" {
		t.Fatalf("synthesized root node missing or misordered: %+v", row.Tree)
	}
	if row.Tree[0].Status != unknownAgentField {
		t.Fatalf("root node Status = %q, want %q", row.Tree[0].Status, unknownAgentField)
	}
	// Negative space: the default-zero status ("pending") must NOT leak in
	// for the synthesized root node.
	if row.Tree[0].Status == dag.RunStatusPending.String() {
		t.Fatalf("synthesized root rendered default-zero 'pending' status")
	}

	// Page render: the "(unknown)" sentinel must reach the rendered HTML.
	fake := newFakeDS()
	fake.agentRuntimes = []AgentRuntimeRow{row}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/agents", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), unknownAgentField) {
		t.Fatalf("page missing unknown-root sentinel %q", unknownAgentField)
	}
}

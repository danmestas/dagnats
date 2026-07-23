package console

import (
	"context"
	"net/http"

	"github.com/starfederation/datastar-go/datastar"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
)

// agents.go owns the /console/agents provenance + agent-runtimes view
// (ADR-021 Phase A). It mints ZERO new event types: the lineage tree is
// reconstructed entirely from existing state — run snapshots carry
// RootRunID / ParentRunID (#377), the #378 Budget reports real quota
// counts, and the #380 `runtime.spawn` audit rows tag runtime-origin
// nodes. See docs/architecture/agent-runtimes-provenance.md.
//
// The honesty discipline is load-bearing here: only engine-backed data
// renders. A lone top-level run is NOT a runtime and is OMITTED. An
// unbacked Budget OMITS the whole block (never a fabricated 0/0). The
// runtime-origin tag appears only when an audit row backs it.

const (
	// agentRunScanMax bounds the single ListAll-style scan that feeds
	// tree assembly. Reuses the runtime-run-scan bound family (10k) so
	// the page render stays cheap regardless of run-population size.
	agentRunScanMax = 10_000
	// maxNodesPerTree caps how many nodes a single tree may render. A
	// cycle-defended BFS can never exceed the run population, but the
	// explicit cap is the belt to the visited-set's suspenders.
	maxNodesPerTree = 4_096
	// maxNestingDepthGuard is the REAL enforced ceiling on generation
	// depth — the same constant the orchestrator enforces on spawn, not
	// a console-invented number. +1 is the root row at depth 0.
	maxNestingDepthGuard = engine.MaxNestingDepth
	// agentAuditScanMax bounds the one audit read used to set the
	// runtime-origin tag. Kept small: the page only needs recent spawns.
	agentAuditScanMax = 2_000
	// agentRuntimesListMax bounds how many runtime rows the list page
	// renders.
	agentRuntimesListMax = 200
	// unknownAgentField is the honest sentinel for a synthesized root
	// whose own snapshot is absent — we genuinely don't know its workflow
	// or status, so we say so instead of rendering a blank/default cell.
	unknownAgentField = "(unknown)"
)

// agentBudgetSource is the narrow slice of *api.Service the agent-runtime
// projection needs: the one-shot run scan, the single-root Budget (detail /
// SSE path), and the BATCHED BudgetsForRoots (list path). Naming it lets the
// list path be unit-tested with a counting stub — proving the page is O(1)
// budget reads, not N+1. *api.Service satisfies it.
type agentBudgetSource interface {
	ScanRuns(
		ctx context.Context, filter api.RunsFilter, limit int,
	) ([]dag.WorkflowRun, error)
	Budget(
		ctx context.Context, ownerRunID string,
	) (worker.RuntimeBudget, string, error)
	BudgetsForRoots(
		ctx context.Context, runs []dag.WorkflowRun,
	) (map[string]worker.RuntimeBudget, error)
}

// ListAgentRuntimes scans the run population ONCE, groups by tree-root,
// filters to actual runtimes (a lone run is omitted), and assembles each tree
// with its Budget. The budgets come from a SINGLE batched BudgetsForRoots call
// (one def-key scan, zero run scans — active counts derive from the runs we
// already loaded) rather than a per-root Budget scan, so the page is
// O(runs + defkeys), not O(roots x runs). Budget ceilings come from the real
// control-plane limits (honest, not guessed).
func (a *apiServiceAdapter) ListAgentRuntimes(
	ctx context.Context, limit int,
) ([]AgentRuntimeRow, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ListAgentRuntimes: ctx is nil")
	}
	if a.svc == nil {
		panic("apiServiceAdapter.ListAgentRuntimes: svc is nil")
	}
	return a.listAgentRuntimes(ctx, a.svc, a.runtimeSpawnTargets(ctx), limit)
}

// listAgentRuntimes is the testable core of ListAgentRuntimes: it takes the
// budget source as an interface so a counting stub can prove the list path
// uses the batched BudgetsForRoots exactly once and never the per-root Budget.
// A batched-budget failure degrades ALL rows to BudgetOK=false (the template
// omits the block — never a fabricated 0/0) rather than failing the page.
func (a *apiServiceAdapter) listAgentRuntimes(
	ctx context.Context, src agentBudgetSource,
	spawned map[string]bool, limit int,
) ([]AgentRuntimeRow, error) {
	runs, err := src.ScanRuns(ctx, api.RunsFilter{}, agentRunScanMax)
	if err != nil {
		return nil, err
	}
	byRoot := groupByRoot(runs)
	budgets, err := src.BudgetsForRoots(ctx, runs)
	if err != nil {
		a.logger.Warn("console: agent budgets batch", "err", err)
		budgets = nil // signals the degrade path in buildRuntimeRowBatched
	}
	rows := make([]AgentRuntimeRow, 0, len(byRoot))
	for root, group := range byRoot {
		if !isRuntimeTree(group, root) {
			continue
		}
		if limit > 0 && len(rows) >= limit {
			break
		}
		rows = append(rows,
			buildRuntimeRowBatched(runs, root, spawned, budgets))
	}
	return rows, nil
}

// AgentRuntime re-projects one root's tree — the single-root SSE path.
func (a *apiServiceAdapter) AgentRuntime(
	ctx context.Context, root string,
) (AgentRuntimeRow, bool, error) {
	if ctx == nil {
		panic("apiServiceAdapter.AgentRuntime: ctx is nil")
	}
	if a.svc == nil {
		panic("apiServiceAdapter.AgentRuntime: svc is nil")
	}
	if root == "" {
		return AgentRuntimeRow{}, false, nil
	}
	runs, err := a.svc.ScanRuns(ctx, api.RunsFilter{}, agentRunScanMax)
	if err != nil {
		return AgentRuntimeRow{}, false, err
	}
	group := groupByRoot(runs)[root]
	if !isRuntimeTree(group, root) {
		return AgentRuntimeRow{}, false, nil
	}
	spawned := a.runtimeSpawnTargets(ctx)
	return a.buildRuntimeRow(ctx, runs, root, spawned), true, nil
}

// buildRuntimeRow assembles one tree, layers the runtime-origin tag and the
// per-root Budget, and degrades the row (BudgetOK=false) on a Budget read error
// rather than failing the page. This is the SINGLE-ROOT path (detail / SSE):
// one root, one cheap Budget call. The LIST page uses buildRuntimeRowBatched
// instead, so it never pays N+1 Budget scans.
func (a *apiServiceAdapter) buildRuntimeRow(
	ctx context.Context, runs []dag.WorkflowRun, root string,
	spawned map[string]bool,
) AgentRuntimeRow {
	row := assembleTree(runs, root)
	tagRuntimeOrigin(&row, spawned)
	budget, _, err := a.svc.Budget(ctx, root)
	if err != nil {
		a.logger.Warn("console: agent budget read", "root", root, "err", err)
		row.BudgetOK = false
		return row
	}
	row.Budget = budget
	row.BudgetOK = true
	return row
}

// buildRuntimeRowBatched assembles one tree and layers the runtime-origin tag,
// pulling its Budget from the precomputed BudgetsForRoots map (the list path).
// A nil map (batched read failed) OR a root missing from the map degrades the
// row to BudgetOK=false — the template then omits the budget block rather than
// rendering a fabricated 0/0. No per-root store scan happens here.
func buildRuntimeRowBatched(
	runs []dag.WorkflowRun, root string,
	spawned map[string]bool, budgets map[string]worker.RuntimeBudget,
) AgentRuntimeRow {
	row := assembleTree(runs, root)
	tagRuntimeOrigin(&row, spawned)
	budget, ok := budgets[root]
	if !ok {
		row.BudgetOK = false
		return row
	}
	row.Budget = budget
	row.BudgetOK = true
	return row
}

// tagRuntimeOrigin flips SpawnedByRuntime on every tree node whose RunID a
// `runtime.spawn` audit row named as its Target. Shared by both the list and
// single-root row builders so the tag rule lives in one place.
func tagRuntimeOrigin(row *AgentRuntimeRow, spawned map[string]bool) {
	if row == nil {
		panic("tagRuntimeOrigin: row must not be nil")
	}
	for i := range row.Tree {
		if spawned[row.Tree[i].RunID] {
			row.Tree[i].SpawnedByRuntime = true
		}
	}
}

// runtimeSpawnTargets reads one bounded audit page and returns the set of
// RunIDs that a `runtime.spawn` row named as its Target. When the audit
// KV is empty / unwired the set is empty and the runtime tag is simply
// absent everywhere (honest: no fabricated provenance).
func (a *apiServiceAdapter) runtimeSpawnTargets(
	ctx context.Context,
) map[string]bool {
	out := make(map[string]bool)
	events, err := a.ListAuditEvents(ctx, agentAuditScanMax)
	if err != nil {
		a.logger.Warn("console: agent audit read", "err", err)
		return out
	}
	for i := range events {
		if events[i].Action == string(ActionRuntimeSpawn) && events[i].Target != "" {
			out[events[i].Target] = true
		}
	}
	return out
}

// groupByRoot partitions the run population by tree-root (single pass,
// bounded by len(runs)). engine.RootRunIDOf is the SINGLE definition of a
// run's root — a run with RootRunID=="" self-roots.
func groupByRoot(runs []dag.WorkflowRun) map[string][]dag.WorkflowRun {
	byRoot := make(map[string][]dag.WorkflowRun, len(runs))
	for i := range runs {
		root := engine.RootRunIDOf(runs[i])
		byRoot[root] = append(byRoot[root], runs[i])
	}
	return byRoot
}

// AgentRuntimeRow is one spawn-tree the /console/agents page renders.
// Every field is engine-backed. Budget is meaningful only when BudgetOK
// is true; the template omits the whole budget block otherwise (honesty:
// a zero-valued Budget would lie about being tracked).
type AgentRuntimeRow struct {
	RootRunID    string
	RootWorkflow string
	RootStatus   string
	TotalRuns    int
	Generations  int
	Budget       worker.RuntimeBudget
	BudgetOK     bool
	Tree         []AgentGenNode
}

// AgentGenNode is one run in a spawn tree, in BFS order. Depth is the
// generation distance from the root (root == 0), capped at
// maxNestingDepthGuard. SpawnedByRuntime is true only when a
// `runtime.spawn` audit row names this RunID as its Target.
type AgentGenNode struct {
	RunID            string
	ParentRunID      string
	WorkflowID       string
	Status           string
	Depth            int
	SpawnedByRuntime bool
}

// AgentRuntimesView is what the agent-runtimes template binds.
type AgentRuntimesView struct {
	Header   PageHeader
	Runtimes []AgentRuntimeRow
}

// assembleTree is the ONE deep helper both DataSource entry points call.
// It groups the run population by tree-root, then for `root` runs a
// bounded, cycle-defended iterative BFS (no recursion) linking children
// to parents and carrying Depth. Depth is capped at maxNestingDepthGuard
// and node count at maxNodesPerTree. Budget / runtime-origin are layered
// on by the caller — this helper is pure (no DataSource, no NATS) so the
// bounded invariants are unit-testable in isolation.
func assembleTree(runs []dag.WorkflowRun, root string) AgentRuntimeRow {
	if root == "" {
		panic("assembleTree: root must not be empty")
	}
	if runs == nil {
		panic("assembleTree: runs must not be nil")
	}
	childrenOf := make(map[string][]dag.WorkflowRun, len(runs))
	byID := make(map[string]dag.WorkflowRun, len(runs))
	var rootRun dag.WorkflowRun
	var haveRoot bool
	for i := range runs {
		run := runs[i]
		if engine.RootRunIDOf(run) != root {
			continue
		}
		byID[run.RunID] = run
		if run.RunID == root {
			rootRun, haveRoot = run, true
			continue
		}
		childrenOf[run.ParentRunID] = append(childrenOf[run.ParentRunID], run)
	}
	if !haveRoot {
		// No explicit root snapshot (a child arrived before the root's own
		// snapshot landed, or a legacy run). Synthesize a minimal root so
		// the tree still anchors on the root id.
		rootRun = dag.WorkflowRun{RunID: root}
	}
	nodes := bfsNodes(rootRun, childrenOf)
	rootWorkflow := rootRun.WorkflowID
	rootStatus := rootRun.Status.String()
	if !haveRoot {
		// Honesty: with no root snapshot we genuinely don't know the root's
		// workflow or status. Render an explicit sentinel rather than a blank
		// cell + a default-zero "pending" badge that would read as broken.
		rootWorkflow = unknownAgentField
		rootStatus = unknownAgentField
		if len(nodes) > 0 {
			nodes[0].WorkflowID = unknownAgentField
			nodes[0].Status = unknownAgentField
		}
	}
	return AgentRuntimeRow{
		RootRunID:    root,
		RootWorkflow: rootWorkflow,
		RootStatus:   rootStatus,
		TotalRuns:    len(nodes),
		Generations:  treeGenerations(nodes),
		Tree:         nodes,
	}
}

// bfsNodes walks the spawn tree breadth-first from rootRun using an
// explicit slice-queue and a visited set (cycle defense). Bounded by
// maxNodesPerTree and maxNestingDepthGuard — children below the depth
// ceiling are not enqueued, mirroring the orchestrator's spawn cap.
func bfsNodes(
	rootRun dag.WorkflowRun, childrenOf map[string][]dag.WorkflowRun,
) []AgentGenNode {
	if childrenOf == nil {
		panic("bfsNodes: childrenOf must not be nil")
	}
	if rootRun.RunID == "" {
		// An empty root id would enter the visited set under "" and let a
		// "" ParentRunID child masquerade as the root — corrupting the tree.
		panic("bfsNodes: rootRun.RunID must not be empty")
	}
	type queued struct {
		run   dag.WorkflowRun
		depth int
	}
	visited := make(map[string]bool, maxNodesPerTree)
	out := make([]AgentGenNode, 0, maxNodesPerTree)
	queue := []queued{{run: rootRun, depth: 0}}
	for len(queue) > 0 && len(out) < maxNodesPerTree {
		head := queue[0]
		queue = queue[1:]
		if visited[head.run.RunID] {
			continue
		}
		visited[head.run.RunID] = true
		out = append(out, AgentGenNode{
			RunID:       head.run.RunID,
			ParentRunID: head.run.ParentRunID,
			WorkflowID:  head.run.WorkflowID,
			Status:      head.run.Status.String(),
			Depth:       head.depth,
		})
		if head.depth >= maxNestingDepthGuard {
			continue
		}
		kids := childrenOf[head.run.RunID]
		for i := range kids {
			if visited[kids[i].RunID] {
				continue
			}
			queue = append(queue, queued{run: kids[i], depth: head.depth + 1})
		}
	}
	return out
}

// treeGenerations is the count of distinct depth levels below the root
// (a root-only tree has 0 generations; root + child has 1).
func treeGenerations(nodes []AgentGenNode) int {
	maxDepth := 0
	for i := range nodes {
		if nodes[i].Depth > maxDepth {
			maxDepth = nodes[i].Depth
		}
	}
	return maxDepth
}

// isRuntimeTree reports whether a root's run group is an actual runtime:
// more than one member, OR a member that names root as its ParentRunID.
// A lone top-level run is not a runtime and is omitted (honesty).
//
// The second condition is NOT dead code: it covers the race window where a
// child's snapshot lands before its parent root's own snapshot. The group
// then has exactly one member (the child) whose ParentRunID == root, so the
// len>1 test misses it — but it is genuinely a runtime and must render.
func isRuntimeTree(group []dag.WorkflowRun, root string) bool {
	if len(group) > 1 {
		return true
	}
	for i := range group {
		if group[i].ParentRunID == root {
			return true
		}
	}
	return false
}

// servePageAgentRuntimes renders /console/agents. A scan failure degrades
// to an empty list (the empty-state card), never a 500 — matching the
// degrade-don't-500 contract of the concurrency / connections pages.
func servePageAgentRuntimes(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageAgentRuntimes: w is nil")
	}
	if r == nil {
		panic("servePageAgentRuntimes: r is nil")
	}
	data, ok := requireData(w, cfg, "list agent runtimes")
	if !ok {
		return
	}
	rows, err := data.ListAgentRuntimes(r.Context(), agentRuntimesListMax)
	if err != nil {
		cfg.Logger.Error("console: list agent runtimes", "err", err)
		rows = nil
	}
	renderPage(w, r, ts, cfg, "agent-runtimes", pageData{
		Title:   "Agent runtimes",
		Section: "agents",
		Page: AgentRuntimesView{
			Header:   buildAgentRuntimesHeader(rows),
			Runtimes: rows,
		},
	})
}

// buildAgentRuntimesHeader assembles the count strip: live runtimes and
// total runs across all trees. Both are neutral facts (ToneInfo).
func buildAgentRuntimesHeader(rows []AgentRuntimeRow) PageHeader {
	var totalRuns int
	for i := range rows {
		totalRuns += rows[i].TotalRuns
	}
	tiles := []Tile{
		{Label: "runtimes", Count: len(rows), Tone: ToneInfo,
			Tooltip: "Active spawn trees (a run that spawned at least one child)"},
		{Label: "total runs", Count: totalRuns, Tone: ToneInfo,
			Tooltip: "Sum of runs across all spawn trees"},
	}
	header, err := NewPageHeader(PageHeader{
		Title:    "Agent runtimes",
		Subtitle: "Spawn-tree provenance — lineage reconstructed from run snapshots, no synthetic events.",
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Agent runtimes"}
	}
	return header
}

// serveSSEAgents streams agent-runtime tree updates. It reuses WatchRuns
// (no new watcher): each RunUpdate carrying a non-empty tree-root
// re-projects ONLY that root's tree (single-root path, F2) and patches
// the matching tree tbody. Bound to r.Context() for goroutine cleanup.
func serveSSEAgents(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSSEAgents: w is nil")
	}
	if r == nil {
		panic("serveSSEAgents: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ds, ok := requireData(w, cfg, "sse-agents")
	if !ok {
		return
	}
	// Agent trees need the full bucket replay to seed the tree on
	// connect, so liveOnly stays false.
	ch, err := ds.WatchRuns(r.Context(), false)
	if err != nil {
		cfg.Logger.Error("console: sse agents watch", "err", err)
		http.Error(w, "watch failed", http.StatusServiceUnavailable)
		return
	}
	sse := datastar.NewSSE(w, r)
	pumpAgentUpdates(r.Context(), sse, ts, ds, ch, cfg)
}

// pumpAgentUpdates translates tree-member RunUpdate values into
// PatchElements events targeting that root's tree tbody. A lone non-tree
// run (AgentRuntime returns not-found) produces NO patch. Bounded loop,
// exits on ctx done or channel close.
func pumpAgentUpdates(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	ts *templateSet, ds DataSource,
	ch <-chan RunUpdate, cfg Config,
) {
	if sse == nil {
		panic("pumpAgentUpdates: sse is nil")
	}
	if ds == nil {
		panic("pumpAgentUpdates: ds is nil")
	}
	const maxIters = 1_000_000_000
	for i := 0; i < maxIters; i++ {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-ch:
			if !ok {
				return
			}
			if !emitAgentPatch(ctx, sse, ts, ds, update, cfg) {
				return
			}
		}
	}
}

// emitAgentPatch re-projects the single root the update belongs to and
// patches its tree tbody. Returns false only when the SSE write fails
// (the pump should then exit). A non-tree update (empty/unknown root)
// is a no-op that returns true.
func emitAgentPatch(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	ts *templateSet, ds DataSource,
	update RunUpdate, cfg Config,
) bool {
	if update.Run.RunID == "" {
		return true
	}
	root := engine.RootRunIDOf(update.Run)
	row, ok, err := ds.AgentRuntime(ctx, root)
	if err != nil {
		cfg.Logger.Error("console: sse agents project", "root", root, "err", err)
		return true
	}
	if !ok {
		return true // lone non-tree run: no patch
	}
	html, err := renderFragment(ts.base, "agent-tree-rows", row)
	if err != nil {
		cfg.Logger.Error("console: sse agents render", "root", root, "err", err)
		return true
	}
	if err := sse.PatchElements(html,
		datastar.WithSelector("#agent-tree-"+root),
		datastar.WithMode(datastar.ElementPatchModeInner),
	); err != nil {
		cfg.Logger.Error("console: sse agents patch", "root", root, "err", err)
		return false
	}
	return true
}

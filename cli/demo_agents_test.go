// cli/demo_agents_test.go
// Integration test for the demo agent-runtime tree. Methodology: wire a real
// embedded NATS server + orchestrator + api micro-service + a GRANTED
// control-plane, exactly as `dagnats serve` does when its config grants the
// supervisor workflow. dagnatstest.NewHarness does NOT start the NATS API
// endpoints or wire a grant holder, so the control-plane subjects would be
// unserved and every supervisor step would get a nil handle; this test wires
// the full stack itself (mirroring internal/api's newCPHarnessFull). It
// starts ONE supervisor run and proves the demo forms the parent->child run
// tree the console Agents page renders via isRuntimeTree: the supervisor run
// spawns >=1 child run carrying ParentRunID == the supervisor run and the
// same tree-root. Bounded <=20s waits; asserts positive (children present,
// lineage correct) and negative (no self-rooted orphan child) space.
package cli

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/nats-io/nats.go/jetstream"
)

// TestDemoSupervisorFormsRuntimeTree proves a granted supervisor run spawns a
// real fleet of child runs, populating the console Agents page.
func TestDemoSupervisorFormsRuntimeTree(t *testing.T) {
	t.Parallel()
	nc := dagnatstest.Server(t)

	// The grant: without demo-supervisor on the control-plane grant list the
	// server strips the capability and the handler gets a nil handle.
	holder := &engine.GrantPolicyHolder{}
	holder.Store(engine.NewGrantPolicy(
		[]string{demoWorkflowAgentSupervisor}, nil))

	orch := engine.NewOrchestrator(nc, engine.WithGrantPolicyHolder(holder))
	orch.Start()
	t.Cleanup(orch.Stop)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	auditKV, err := auditkv.NewKV(context.Background(), js)
	if err != nil {
		t.Fatalf("audit KV: %v", err)
	}
	svc := api.NewServiceWithLimits(nc, api.RuntimeLimits{},
		api.WithGrantPolicyHolder(holder), api.WithAuditKV(auditKV))
	natsAPI := api.NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)

	if err := ensureRichWorkflows(svc); err != nil {
		t.Fatalf("ensureRichWorkflows: %v", err)
	}
	workers, err := startRichWorker(nc, svc)
	if err != nil {
		t.Fatalf("startRichWorker: %v", err)
	}
	t.Cleanup(func() {
		for _, w := range workers {
			w.Stop()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	input := buildWorkflowInput(demoWorkflowAgentSupervisor, outcomeCompleted)
	rootRunID, err := svc.StartRun(ctx, demoWorkflowAgentSupervisor, input)
	if err != nil {
		t.Fatalf("StartRun supervisor: %v", err)
	}

	children := waitForChildRuns(t, svc, rootRunID)
	if len(children) < 1 {
		t.Fatalf("no child runs formed under supervisor %q (want a tree)",
			rootRunID)
	}
	for _, child := range children {
		if child.ParentRunID != rootRunID {
			t.Errorf("child %q ParentRunID = %q, want %q",
				child.RunID, child.ParentRunID, rootRunID)
		}
		// The child shares the supervisor's tree-root — the exact grouping
		// key the Agents page uses (engine.RootRunIDOf).
		if got := engine.RootRunIDOf(child); got != rootRunID {
			t.Errorf("child %q tree-root = %q, want %q",
				child.RunID, got, rootRunID)
		}
	}
}

// waitForChildRuns polls the run population until it sees the expected fleet
// under rootRunID or the deadline passes, returning whatever children exist
// so the caller asserts the tree shape. Bounded by the deadline.
func waitForChildRuns(
	t *testing.T, svc *api.Service, rootRunID string,
) []dag.WorkflowRun {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		kids := childRunsOf(t, svc, rootRunID)
		if len(kids) >= demoSupervisorChildCount {
			return kids
		}
		select {
		case <-deadline:
			return childRunsOf(t, svc, rootRunID)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// childRunsOf returns every run whose ParentRunID is rootRunID. Uses the same
// ScanRuns scan the console Agents adapter uses.
func childRunsOf(
	t *testing.T, svc *api.Service, rootRunID string,
) []dag.WorkflowRun {
	t.Helper()
	runs, err := svc.ScanRuns(context.Background(), api.RunsFilter{}, 500)
	if err != nil {
		return nil
	}
	var kids []dag.WorkflowRun
	for _, run := range runs {
		if run.ParentRunID == rootRunID {
			kids = append(kids, run)
		}
	}
	return kids
}

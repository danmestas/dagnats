// internal/engine/run_terminal_heal_test.go
// Methodology: white-box (package engine) fault-injection test for
// #634 review round 2's create-only-save-is-the-claim fix. Calls
// handleWorkflowStarted directly (bypassing the NATS consumer
// plumbing dispatchEvent normally wraps it in) so a "redelivery" is
// simply a second direct call with the identical event -- exactly
// what JetStream redelivering the same message would produce. The
// failure is injected via a KeyValue wrapper that fails Create a
// bounded number of times before delegating to the real bucket, so
// it clears on its own before the "redelivery" call, matching a
// transient fault, not a permanent one. Uses a real embedded NATS
// server.
//
// #634 review round 3 removed this file's second test
// (EnqueueFailureThenRedeliveryHeals): it exercised healRun
// redispatching a step already marked Queued, which round 3 found
// unsafe (TASK_QUEUES has no Duplicates window, so a redispatch past
// the ~2 minute default can double-dispatch a step to two workers)
// and removed from healRun entirely -- see orchestrator.go's healRun
// doc comment. The "step already Queued at redelivery" scenario is
// now covered at the e2e level instead
// (internal/trigger/run_terminal_e2e_test.go), which exercises it
// against real task dispatch rather than a synthetic publish failure.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// flakyCreateKV wraps a real jetstream.KeyValue and fails the next
// failN calls to Create with an injected error before delegating to
// the wrapped bucket for every call after that. A caller retrying
// after a transient Create failure sees the SAME semantics a real
// transient NATS error would produce: the write never landed, so a
// subsequent Create for the same key still succeeds.
type flakyCreateKV struct {
	jetstream.KeyValue
	failN int32
}

func (f *flakyCreateKV) Create(
	ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt,
) (uint64, error) {
	if atomic.AddInt32(&f.failN, -1) >= 0 {
		return 0, errors.New("injected Create failure")
	}
	return f.KeyValue.Create(ctx, key, value, opts...)
}

// runTerminalHealWorkflow returns a single-step echo workflow, same
// shape internal/trigger's e2e tests use.
func runTerminalHealWorkflow(name string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    name,
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
	}
}

// runTerminalChainEvent builds the workflow.started protocol.Event a
// run_terminal trigger fire produces (mirrors
// internal/trigger/registrar_run_terminal.go's runTerminalChainPayload
// and runTerminalChainInput) for runID starting targetWorkflow.
func runTerminalChainEvent(
	t *testing.T, runID, targetWorkflow, sourceRunID string,
) protocol.Event {
	t.Helper()
	input, err := json.Marshal(map[string]string{
		"run_id":      sourceRunID,
		"workflow_id": "source-wf",
		"status":      "completed",
	})
	if err != nil {
		t.Fatalf("marshal chain input: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"trigger":       "run_terminal",
		"source":        "trig-heal-test",
		"workflow_id":   targetWorkflow,
		"input":         json.RawMessage(input),
		"trigger_depth": 0,
	})
	if err != nil {
		t.Fatalf("marshal chain payload: %v", err)
	}
	return protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
}

func TestHandleWorkflowStarted_RunTerminal_SaveFailureThenRedeliveryStarts(t *testing.T) {
	// #634 review round 2, failing test first: a Create failure on
	// the FIRST delivery must leave NOTHING behind -- no placeholder,
	// no partial state -- so a redelivery starts the run cleanly from
	// scratch.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")
	wfDef := runTerminalHealWorkflow("heal-save-fail-wf")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))
	mustPut(t, defKV, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)), mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	realKV, err := jsNew.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		t.Fatalf("workflow_runs KV: %v", err)
	}
	flaky := &flakyCreateKV{KeyValue: realKV, failN: 1}
	orch.store = &SnapshotStore{kv: flaky}

	evt := runTerminalChainEvent(t, "heal-save-fail-run", wfDef.Name, "src-1")

	// First delivery: the injected Create failure must surface as an
	// error (so the real caller NAKs and redelivery happens).
	if err := orch.handleWorkflowStarted(context.Background(), evt); err == nil {
		t.Fatal("expected error from first delivery (injected Create failure)")
	}

	// Negative: nothing was persisted -- no placeholder, no partial
	// run -- ErrRunNotFound, not some half-written row.
	if _, loadErr := orch.store.Load(
		context.Background(), "heal-save-fail-run",
	); !errors.Is(loadErr, ErrRunNotFound) {
		t.Fatalf("after failed first delivery, Load err = %v, want ErrRunNotFound",
			loadErr)
	}

	// Second delivery ("redelivery"): the flaky wrapper has exhausted
	// its injected failure, so this one uses the real store.
	if err := orch.handleWorkflowStarted(context.Background(), evt); err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	// Positive: exactly one run, Running, correct workflow.
	run, loadErr := orch.store.Load(context.Background(), "heal-save-fail-run")
	if loadErr != nil {
		t.Fatalf("load after redelivery: %v", loadErr)
	}
	if run.Status != dag.RunStatusRunning {
		t.Fatalf("run.Status = %s, want running", run.Status)
	}
	if run.WorkflowID != wfDef.Name {
		t.Fatalf("run.WorkflowID = %q, want %q", run.WorkflowID, wfDef.Name)
	}
}

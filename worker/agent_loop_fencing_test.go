// worker/agent_loop_fencing_test.go
// Probes for Gap A (checkpoint and continue are not atomic) and Gap C (a
// worker whose AckWait expired is not fenced off) in agent loops.
//
// Methodology: a real orchestrator and two real Workers on one embedded
// NATS server, with a short AckWait. The first Worker stands in for a
// worker that dies mid-iteration: its handler blocks inside iteration 0
// and the Worker is stopped, so JetStream redelivers the task to the
// second Worker after AckWait. Gap A: the crashing handler checkpoints
// before blocking, and the test checks what state the redelivered
// iteration 0 loads. Gap C: the blocked handler wakes up after the
// second Worker has continued the loop into iteration 1, and calls
// Complete; the test checks whether the engine accepts it. All waits are
// bounded.
package worker

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const fencingAckWait = 500 * time.Millisecond

// startLoopEngine starts NATS and an orchestrator with an agent-loop
// def whose single step "s" runs taskType.
func startLoopEngine(
	t *testing.T, name, taskType string,
) (*nats.Conn, nats.JetStreamContext, jetstream.JetStream, []byte) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: name, Version: "1",
		Steps: []dag.StepDef{{
			ID: "s", Task: taskType, Type: dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 10}),
		}},
	}
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	for _, key := range []string{
		wfDef.Name, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)),
	} {
		if _, err := defKV.Put(key, defData); err != nil {
			t.Fatalf("put def %q: %v", key, err)
		}
	}
	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	return nc, js, jsNew, defData
}

// startFencingWorker starts a Worker for taskType with the short AckWait
// and a drain timeout short enough to stop it while a handler blocks.
func startFencingWorker(
	t *testing.T, nc *nats.Conn, taskType string, handler HandlerFunc,
) *Worker {
	t.Helper()
	w := NewWorker(nc)
	w.drainTimeout = 100 * time.Millisecond
	w.Handle(taskType, handler, WithAckWait(fencingAckWait))
	w.Start()
	t.Cleanup(w.Stop)
	return w
}

// waitClosed fails the test unless ch closes within timeout.
func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

type checkpointObservation struct {
	iteration int
	loaded    string
}

func TestAgentLoop_RedeliveredIterationSeesPreIterationCheckpoint(t *testing.T) {
	const taskType = "ckpt-loop-task"
	nc, js, jsNew, defData := startLoopEngine(t, "ckpt-loop", taskType)
	var mu sync.Mutex
	var seen []checkpointObservation
	crashed, release := make(chan struct{}), make(chan struct{})
	crashedExit := make(chan struct{})
	var crashOnce sync.Once
	handler := func(tc TaskContext) error {
		saved, err := tc.LoadCheckpoint()
		if err != nil {
			return err
		}
		mu.Lock()
		seen = append(seen, checkpointObservation{
			iteration: tc.(*taskContext).iteration, loaded: string(saved),
		})
		mu.Unlock()
		count := 0
		if saved != nil {
			if err := json.Unmarshal(saved, &count); err != nil {
				return err
			}
		}
		count++
		state, err := json.Marshal(count)
		if err != nil {
			return err
		}
		if err := tc.Checkpoint(state); err != nil {
			return err
		}
		crashNow := false
		crashOnce.Do(func() { crashNow = true })
		if crashNow {
			// Dies between Checkpoint and Continue: never continues, never acks.
			close(crashed)
			<-release
			close(crashedExit)
			return nil
		}
		if count >= 3 {
			return tc.Complete(state)
		}
		return tc.Continue(state)
	}
	defer func() { close(release); waitClosed(t, crashedExit, "crashed handler exit") }()

	dying := startFencingWorker(t, nc, taskType, handler)
	startStepRun(t, js, "run-ckpt-1", defData)
	waitClosed(t, crashed, "iteration 0 to checkpoint and crash")
	dying.Stop()
	startFencingWorker(t, nc, taskType, handler)
	run := waitStepSettled(t, jsNew, "run-ckpt-1", 20*time.Second)
	if run.Steps["s"].Status != dag.StepStatusCompleted {
		t.Fatalf("step = %v, want completed", run.Steps["s"].Status)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 2 || seen[1].iteration != 0 {
		t.Fatalf("observations = %+v, want iteration 0 redelivered second", seen)
	}
	// The redelivered iteration 0 must start from the state iteration 0
	// started from (no checkpoint), not the one the crashed run wrote.
	if seen[1].loaded != seen[0].loaded {
		t.Errorf("redelivered iteration 0 loaded checkpoint %q, first delivery loaded %q",
			seen[1].loaded, seen[0].loaded)
	}
	// One turn per iteration: the final count equals Iterations+1.
	var count int
	if err := json.Unmarshal(run.Steps["s"].Output, &count); err != nil {
		t.Fatalf("decode output %q: %v", run.Steps["s"].Output, err)
	}
	if want := run.Steps["s"].Iterations + 1; count != want {
		t.Errorf("turns counted = %d across %d iterations, want %d (all observations %+v)",
			count, run.Steps["s"].Iterations, want, seen)
	}
}

func TestAgentLoop_StaleWorkerCompleteAfterTakeoverIsRejected(t *testing.T) {
	const taskType = "stale-loop-task"
	nc, js, jsNew, defData := startLoopEngine(t, "stale-loop", taskType)
	stalled, wake := make(chan struct{}), make(chan struct{})
	staleDone := make(chan error, 1)
	iteration1Running, finish := make(chan struct{}), make(chan struct{})
	var stallOnce, iteration1Once sync.Once
	handler := func(tc TaskContext) error {
		stallNow := false
		stallOnce.Do(func() { stallNow = true })
		if stallNow {
			// Stalls past AckWait, then wakes after another worker took over.
			close(stalled)
			<-wake
			err := tc.Complete([]byte(`"stale"`))
			staleDone <- err
			return err
		}
		if tc.(*taskContext).iteration == 0 {
			return tc.Continue(nil)
		}
		iteration1Once.Do(func() { close(iteration1Running) })
		<-finish
		return tc.Complete([]byte(`"fresh"`))
	}
	wakeOnce := sync.OnceFunc(func() { close(wake) })
	defer close(finish)
	defer wakeOnce()

	stale := startFencingWorker(t, nc, taskType, handler)
	startStepRun(t, js, "run-stale-1", defData)
	waitClosed(t, stalled, "iteration 0 to stall")
	stale.Stop()
	startFencingWorker(t, nc, taskType, handler)
	waitClosed(t, iteration1Running, "takeover worker to reach iteration 1")
	wakeOnce()
	select {
	case err := <-staleDone:
		if err != nil {
			t.Fatalf("stale Complete publish: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stale handler never published")
	}

	run := waitStepSettled(t, jsNew, "run-stale-1", 3*time.Second)
	step := run.Steps["s"]
	if step.Status == dag.StepStatusCompleted {
		t.Errorf("stale iteration-0 Complete accepted while iteration 1 runs: "+
			"step=%v output=%s iterations=%d run=%v",
			step.Status, step.Output, step.Iterations, run.Status)
	}
	if step.Status != dag.StepStatusRunning || step.Iterations != 1 {
		t.Errorf("step = %v at iteration %d, want running at iteration 1",
			step.Status, step.Iterations)
	}
}

// startStepRun publishes workflow.started for runID.
func startStepRun(t *testing.T, js nats.JetStreamContext, runID string, defData []byte) {
	t.Helper()
	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, runID, defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	if _, err := js.Publish(startEvt.NATSSubject(), data,
		nats.MsgId(startEvt.NATSMsgID())); err != nil {
		t.Fatalf("publish start: %v", err)
	}
}

// waitStepSettled polls the snapshot until step "s" completes or the
// window passes, and returns the last snapshot.
func waitStepSettled(
	t *testing.T, jsNew jetstream.JetStream, runID string, window time.Duration,
) dag.WorkflowRun {
	t.Helper()
	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(window)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, err := store.Load(context.Background(), runID)
		if err == nil {
			run = r
			if r.Steps["s"].Status == dag.StepStatusCompleted {
				return r
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if run.RunID == "" {
		t.Fatalf("run %q never saved", runID)
	}
	return run
}

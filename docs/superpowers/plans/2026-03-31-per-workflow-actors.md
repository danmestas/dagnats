# Per-Workflow Actors — Implementation Plan (Phase 2)

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create an actor-based orchestrator where each workflow run is a supervised actor holding state in memory — replacing per-event KV loads with in-memory state and per-run mutexes with actor-level sequential processing.

**Architecture:** New `engine/workflow_actor.go` defines a WorkflowActor (implements `actor.Actor`) that caches `WorkflowRun` and `WorkflowDef` in memory, processing events sequentially via its mailbox. New `engine/actor_orch.go` defines `ActorOrchestrator` which subscribes to the history stream and routes events to per-run actors, spawning them on demand with OneForOne supervision. Snapshots still save to KV on every state change (durability), but loads only happen on actor start (recovery). The existing `Orchestrator` stays unchanged — `ActorOrchestrator` is additive, users choose which to use.

**Tech Stack:** Go, `actor/` package (Phase 1), existing `engine/`, `dag/`, `protocol/` packages

**Spec:** `docs/superpowers/specs/2026-03-31-actor-model-evaluation.md` (Part 4, Phase 2)

---

## File Structure

| File | Responsibility |
|------|---------------|
| `engine/workflow_actor.go` | WorkflowActor — per-run actor holding state in memory |
| `engine/workflow_actor_test.go` | Unit tests for WorkflowActor event handling |
| `engine/actor_orch.go` | ActorOrchestrator — routes history events to per-run actors |
| `engine/actor_orch_test.go` | Integration tests with real NATS |

---

## Chunk 1: WorkflowActor

### Task 1: WorkflowActor — event processing with in-memory state

**Files:**
- Create: `engine/workflow_actor.go`
- Test: `engine/workflow_actor_test.go`

- [ ] **Step 1: Write failing test for WorkflowActor handling WorkflowStarted**

Create `engine/workflow_actor_test.go`:

```go
package engine

// Methodology: test WorkflowActor event handling in isolation using
// the actor runtime directly. No NATS — tests inject messages manually.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/actor"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

func TestWorkflowActorHandlesStarted(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name:    "test-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-1", nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-1"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Send workflow.started event
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-1", defData)
	rt.Send(addr, actor.Message{Payload: evt})

	// Wait for processing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() != dag.RunStatusPending {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: run status is Running
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}

	// Positive: step s1 is Queued
	state := wa.StepState("s1")
	if state.Status != dag.StepStatusQueued {
		t.Fatalf("step s1 status = %v, want Queued", state.Status)
	}
}

func TestWorkflowActorHandlesStepCompleted(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name:    "test-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-2", nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-2"}
	rt.Spawn(addr, wa)

	// Start the workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-2", defData)
	rt.Send(addr, actor.Message{Payload: startEvt})
	time.Sleep(50 * time.Millisecond)

	// Complete step s1
	completeEvt := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, "run-2", []byte(`"done"`))
	completeEvt.StepID = "s1"
	rt.Send(addr, actor.Message{Payload: completeEvt})

	// Wait for processing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() == dag.RunStatusCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: run is completed
	if wa.RunStatus() != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed", wa.RunStatus())
	}

	// Positive: step has output
	state := wa.StepState("s1")
	if state.Status != dag.StepStatusCompleted {
		t.Fatalf("step status = %v, want Completed", state.Status)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestWorkflowActor -v -timeout 15s`
Expected: FAIL — `NewWorkflowActor` undefined

- [ ] **Step 3: Implement WorkflowActor**

Create `engine/workflow_actor.go`:

```go
package engine

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/danmestas/dagnats/actor"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

// WorkflowActor manages one workflow run as a supervised actor.
// State lives in memory — snapshots save to KV for durability but
// loads only happen on actor start (recovery).
type WorkflowActor struct {
	runID string
	def   *dag.WorkflowDef
	run   *dag.WorkflowRun
	store *SnapshotStore // nil in unit tests
	mu    sync.RWMutex   // protects read access to run state
}

// NewWorkflowActor creates a workflow actor for the given run.
// store may be nil for testing without NATS.
func NewWorkflowActor(
	runID string, store *SnapshotStore,
) *WorkflowActor {
	if runID == "" {
		panic("NewWorkflowActor: runID must not be empty")
	}
	return &WorkflowActor{
		runID: runID,
		store: store,
	}
}

// Receive processes workflow events from the actor mailbox.
func (wa *WorkflowActor) Receive(
	ctx *actor.Context, msg actor.Message,
) error {
	evt, ok := msg.Payload.(protocol.Event)
	if !ok {
		return fmt.Errorf(
			"unexpected message type: %T", msg.Payload,
		)
	}
	return wa.handleEvent(evt)
}

// handleEvent dispatches the event to the appropriate handler.
func (wa *WorkflowActor) handleEvent(evt protocol.Event) error {
	switch evt.Type {
	case protocol.EventWorkflowStarted:
		return wa.handleStarted(evt)
	case protocol.EventStepCompleted:
		return wa.handleStepCompleted(evt)
	case protocol.EventStepFailed:
		return wa.handleStepFailed(evt)
	case protocol.EventStepContinue:
		return wa.handleStepContinue(evt)
	default:
		return nil
	}
}

func (wa *WorkflowActor) handleStarted(
	evt protocol.Event,
) error {
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(evt.Payload, &wfDef); err != nil {
		return fmt.Errorf("unmarshal WorkflowDef: %w", err)
	}
	run := dag.NewWorkflowRun(wfDef, wa.runID)
	run.Status = dag.RunStatusRunning

	wa.mu.Lock()
	wa.def = &wfDef
	wa.run = &run
	wa.mu.Unlock()

	// Resolve and queue ready steps
	completed := completedSet(run)
	queued := queuedSet(run)
	ready := dag.ResolveReady(wfDef, completed, queued)
	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}

	return wa.saveIfStore()
}

func (wa *WorkflowActor) handleStepCompleted(
	evt protocol.Event,
) error {
	if wa.run == nil || wa.def == nil {
		return fmt.Errorf("workflow not started")
	}

	wa.mu.Lock()
	state := wa.run.Steps[evt.StepID]
	state.Status = dag.StepStatusCompleted
	state.Output = evt.Payload
	wa.run.Steps[evt.StepID] = state

	completed := completedSet(*wa.run)
	if dag.IsComplete(*wa.def, completed) {
		wa.run.Status = dag.RunStatusCompleted
		wa.mu.Unlock()
		return wa.saveIfStore()
	}

	// Resolve newly ready steps
	queued := queuedSet(*wa.run)
	ready := dag.ResolveReady(*wa.def, completed, queued)
	for _, step := range ready {
		s := wa.run.Steps[step.ID]
		s.Status = dag.StepStatusQueued
		wa.run.Steps[step.ID] = s
	}
	wa.mu.Unlock()

	return wa.saveIfStore()
}

func (wa *WorkflowActor) handleStepFailed(
	evt protocol.Event,
) error {
	if wa.run == nil {
		return fmt.Errorf("workflow not started")
	}

	wa.mu.Lock()
	state := wa.run.Steps[evt.StepID]
	state.Status = dag.StepStatusFailed
	if evt.Payload != nil {
		state.Error = string(evt.Payload)
	}
	wa.run.Steps[evt.StepID] = state
	wa.run.Status = dag.RunStatusFailed
	wa.mu.Unlock()

	return wa.saveIfStore()
}

func (wa *WorkflowActor) handleStepContinue(
	evt protocol.Event,
) error {
	if wa.run == nil || wa.def == nil {
		return fmt.Errorf("workflow not started")
	}

	wa.mu.Lock()
	state := wa.run.Steps[evt.StepID]
	state.Iterations++
	wa.run.Steps[evt.StepID] = state
	wa.mu.Unlock()

	return wa.saveIfStore()
}

// saveIfStore persists the run to KV if a store is configured.
func (wa *WorkflowActor) saveIfStore() error {
	if wa.store == nil || wa.run == nil {
		return nil
	}
	wa.mu.RLock()
	defer wa.mu.RUnlock()
	return wa.store.Save(*wa.run)
}

// RunStatus returns the current run status (thread-safe).
func (wa *WorkflowActor) RunStatus() dag.RunStatus {
	wa.mu.RLock()
	defer wa.mu.RUnlock()
	if wa.run == nil {
		return dag.RunStatusPending
	}
	return wa.run.Status
}

// StepState returns a step's current state (thread-safe).
func (wa *WorkflowActor) StepState(stepID string) dag.StepState {
	wa.mu.RLock()
	defer wa.mu.RUnlock()
	if wa.run == nil {
		return dag.StepState{}
	}
	return wa.run.Steps[stepID]
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestWorkflowActor -v -timeout 15s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add engine/workflow_actor.go engine/workflow_actor_test.go
git commit -m "feat(engine): add WorkflowActor — per-run actor with in-memory state"
```

---

## Chunk 2: ActorOrchestrator

### Task 2: ActorOrchestrator — routes events to per-run actors

**Files:**
- Create: `engine/actor_orch.go`
- Test: `engine/actor_orch_test.go`

- [ ] **Step 1: Write failing test for ActorOrchestrator**

Create `engine/actor_orch_test.go`:

```go
package engine

// Methodology: integration test with real embedded NATS. Verify the
// ActorOrchestrator spawns per-run actors and routes events correctly.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestActorOrchBasicWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Register workflow
	wfDef := dag.WorkflowDef{
		Name:    "actor-test",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("actor-test", defData)

	// Start ActorOrchestrator
	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish workflow.started
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "arun-1", defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg)

	// Wait for actor to process
	time.Sleep(200 * time.Millisecond)

	// Positive: actor was spawned for this run
	wa := orch.GetWorkflowActor("arun-1")
	if wa == nil {
		t.Fatalf("expected workflow actor for arun-1")
	}

	// Positive: run is in Running state
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}
}

func TestActorOrchRoutesCompletionToActor(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "actor-test-2",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("actor-test-2", defData)

	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "arun-2", defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg)
	time.Sleep(200 * time.Millisecond)

	// Complete step s1
	completeEvt := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, "arun-2", []byte(`"result"`))
	completeEvt.StepID = "s1"
	data2, _ := completeEvt.Marshal()
	msg2 := &nats.Msg{
		Subject: completeEvt.NATSSubject(),
		Data:    data2,
		Header:  nats.Header{"Nats-Msg-Id": {completeEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg2)

	// Wait for completion
	deadline := time.Now().Add(3 * time.Second)
	wa := orch.GetWorkflowActor("arun-2")
	for time.Now().Before(deadline) {
		if wa != nil && wa.RunStatus() == dag.RunStatusCompleted {
			break
		}
		wa = orch.GetWorkflowActor("arun-2")
		time.Sleep(50 * time.Millisecond)
	}

	if wa == nil {
		t.Fatalf("expected workflow actor for arun-2")
	}

	// Positive: workflow completed
	if wa.RunStatus() != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed", wa.RunStatus())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestActorOrch -v -timeout 30s`
Expected: FAIL — `NewActorOrchestrator` undefined

- [ ] **Step 3: Implement ActorOrchestrator**

Create `engine/actor_orch.go`:

```go
package engine

import (
	"sync"
	"time"

	"github.com/danmestas/dagnats/actor"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// ActorOrchestrator is an actor-based workflow orchestrator. It
// subscribes to the WORKFLOW_HISTORY stream and routes events to
// per-run WorkflowActors managed by the actor runtime.
//
// Unlike Orchestrator, run state lives in-memory within each
// WorkflowActor. Snapshots still save to KV for durability.
type ActorOrchestrator struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	tel     *observe.Telemetry
	rt      *actor.Runtime
	store   *SnapshotStore
	sub     *nats.Subscription
	actors  sync.Map // runID → *WorkflowActor
}

// NewActorOrchestrator creates an actor-based orchestrator.
func NewActorOrchestrator(
	nc *nats.Conn, tel *observe.Telemetry,
) *ActorOrchestrator {
	if nc == nil {
		panic("NewActorOrchestrator: nc must not be nil")
	}
	if tel == nil {
		panic("NewActorOrchestrator: tel must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewActorOrchestrator: JetStream: " + err.Error())
	}
	return &ActorOrchestrator{
		nc:    nc,
		js:    js,
		tel:   tel,
		rt:    actor.NewRuntime(),
		store: NewSnapshotStore(js),
	}
}

// Start subscribes to the WORKFLOW_HISTORY stream.
func (ao *ActorOrchestrator) Start() {
	if ao.sub != nil {
		panic("ActorOrchestrator.Start: already started")
	}
	sub, err := ao.js.Subscribe("history.>", ao.handleEvent,
		nats.DeliverAll(),
		nats.AckExplicit(),
	)
	if err != nil {
		panic("ActorOrchestrator.Start: subscribe: " + err.Error())
	}
	ao.sub = sub
}

// Stop drains and terminates all actors.
func (ao *ActorOrchestrator) Stop() {
	if ao.sub != nil {
		ao.sub.Unsubscribe()
		ao.sub = nil
	}
	ao.rt.StopAll()
}

// handleEvent routes a history event to the per-run actor.
func (ao *ActorOrchestrator) handleEvent(msg *nats.Msg) {
	if msg == nil {
		return
	}
	evt, err := protocol.UnmarshalEvent(msg.Data)
	if err != nil {
		ao.tel.Logger.Error("unmarshal event", err)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	if !isHandledEventType(evt.Type) {
		msg.Ack()
		return
	}

	// Ensure actor exists for this run
	ao.ensureActor(evt.RunID)

	// Route event to actor
	addr := actor.Address{Type: "workflow", ID: evt.RunID}
	sendErr := ao.rt.Send(addr, actor.Message{Payload: evt})
	if sendErr != nil {
		ao.tel.Logger.Error("route event to actor", sendErr,
			observe.String("run_id", evt.RunID),
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
}

// ensureActor spawns a WorkflowActor for the run if one doesn't
// exist. Idempotent — safe to call multiple times.
func (ao *ActorOrchestrator) ensureActor(runID string) {
	if _, loaded := ao.actors.Load(runID); loaded {
		return
	}

	wa := NewWorkflowActor(runID, ao.store)
	addr := actor.Address{Type: "workflow", ID: runID}

	err := ao.rt.Spawn(addr, wa,
		actor.WithSupervision(&actor.OneForOne{}),
	)
	if err != nil {
		// Already exists (race between concurrent events) — fine
		return
	}
	ao.actors.Store(runID, wa)
}

// GetWorkflowActor returns the actor for a run, or nil if not found.
// Used for testing and inspection.
func (ao *ActorOrchestrator) GetWorkflowActor(
	runID string,
) *WorkflowActor {
	val, ok := ao.actors.Load(runID)
	if !ok {
		return nil
	}
	return val.(*WorkflowActor)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestActorOrch -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run all engine tests + full suite**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -v -count=1 -timeout 60s`
Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add engine/actor_orch.go engine/actor_orch_test.go
git commit -m "feat(engine): add ActorOrchestrator — per-run actors with in-memory state"
```

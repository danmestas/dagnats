# Extract Orchestrator into Focused Subsystems

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompose the Orchestrator God object (72 methods, 3000+ lines, 15 responsibilities) into focused subsystems that each own their state, KV buckets, and tests.

**Architecture:** Extract subsystems bottom-up, starting with the most isolated (fewest callers, clearest boundaries) and working toward the most entangled. Each extraction introduces an interface the Orchestrator depends on, moves methods and fields to the new subsystem, and updates tests. The Orchestrator becomes a thin event loop that delegates to subsystems.

**Tech Stack:** Go, NATS JetStream KV, OpenTelemetry tracing/metrics

**Extraction order (least coupled → most coupled):**
1. AdmissionController — singleton + concurrency gating (already isolated in admission.go + concurrency.go)
2. ApprovalGate — approval token lifecycle (already isolated in approval.go)
3. StickyRouter — worker affinity bindings (already isolated in sticky.go)
4. TaskPublisher — rate limiting + fan-out + subject routing (task_publish.go + rate limit methods)
5. StepDispatcher — step-type routing (map, sleep, wait, sub-workflow, approval, planner, normal)
6. RecoveryManager — on-failure, compensation, dead letters

**Invariants across all tasks:**
- Each extraction is a standalone commit that leaves all tests passing
- No behavioral changes — pure structural refactor
- Orchestrator field count decreases with each extraction
- New subsystems are tested at their own boundary
- 70-line function limit, 100-column line limit, `gofmt`

---

## Chunk 1: AdmissionController

### Task 1: Define AdmissionController interface and type

**Files:**
- Create: `internal/engine/admission_controller.go`
- Modify: `internal/engine/admission.go` (move `admissionAction`, `admissionResult`)
- Modify: `internal/engine/concurrency.go` (becomes internal to AdmissionController)

- [ ] **Step 1: Create `admission_controller.go` with the new type**

The AdmissionController consolidates singleton checks, concurrency gating, and priority resolution behind a single `Admit()` entry point. It owns the `ConcurrencyManager`, `singletonKV`, and the admission pipeline.

```go
// admission_controller.go

package engine

import (
	"context"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go/jetstream"
)

// AdmissionController gates workflow runs through singleton,
// concurrency, and priority checks. Owns all KV buckets for
// admission state so the orchestrator doesn't need to.
type AdmissionController struct {
	concurrency *ConcurrencyManager
	singletonKV jetstream.KeyValue
}

// NewAdmissionController creates a controller. Both parameters
// may be nil for graceful degradation.
func NewAdmissionController(
	concurrency *ConcurrencyManager,
	singletonKV jetstream.KeyValue,
) *AdmissionController {
	return &AdmissionController{
		concurrency: concurrency,
		singletonKV: singletonKV,
	}
}

// Admit runs the admission pipeline for a workflow run.
// Returns the action to take (proceed, queue, skip) and
// metadata (e.g., cancelID for singleton replacement).
func (ac *AdmissionController) Admit(
	ctx context.Context,
	def dag.WorkflowDef,
	run *dag.WorkflowRun,
) (admissionResult, error) {
	// Move body of o.admitRun() here, replacing o.concurrency
	// and o.singletonKV with ac.concurrency and ac.singletonKV
}

// AcquireRun delegates to ConcurrencyManager.
func (ac *AdmissionController) AcquireRun(
	ctx context.Context,
	workflowName string,
	maxRuns int,
) (bool, error) {
	if ac.concurrency == nil {
		return true, nil
	}
	return ac.concurrency.AcquireRun(ctx, workflowName, maxRuns)
}

// ReleaseRun delegates to ConcurrencyManager.
func (ac *AdmissionController) ReleaseRun(
	ctx context.Context,
	workflowName string,
) error {
	if ac.concurrency == nil {
		return nil
	}
	return ac.concurrency.ReleaseRun(ctx, workflowName)
}

// AcquireTask delegates to ConcurrencyManager.
func (ac *AdmissionController) AcquireTask(
	ctx context.Context,
	taskType string,
	maxTasks int,
) (bool, error) {
	if ac.concurrency == nil {
		return true, nil
	}
	return ac.concurrency.AcquireTask(ctx, taskType, maxTasks)
}

// ReleaseTask delegates to ConcurrencyManager.
func (ac *AdmissionController) ReleaseTask(
	ctx context.Context,
	taskType string,
) error {
	if ac.concurrency == nil {
		return nil
	}
	return ac.concurrency.ReleaseTask(ctx, taskType)
}

// ReleaseSingletonLock removes the singleton lock for a run.
func (ac *AdmissionController) ReleaseSingletonLock(
	ctx context.Context,
	run *dag.WorkflowRun,
) {
	// Move body of o.releaseSingletonLock() here
}
```

- [ ] **Step 2: Move `admitRun`, `singletonCheck`, `applySingletonMode`, `releaseSingletonLock`, `publishWorkflowCancelledEvent` from orchestrator methods to AdmissionController methods**

Each method changes receiver from `(o *Orchestrator)` to `(ac *AdmissionController)`. Replace field access:
- `o.concurrency` → `ac.concurrency`
- `o.singletonKV` → `ac.singletonKV`

The `publishWorkflowCancelledEvent` method needs `nc` and `js` — pass them as parameters or store on AdmissionController.

- [ ] **Step 3: Update Orchestrator to use AdmissionController**

In `orchestrator.go`:
- Remove fields: `concurrency`, `singletonKV`
- Add field: `admission *AdmissionController`
- In `NewOrchestrator()`: create `AdmissionController` from the KV buckets
- Replace all `o.concurrency.X()` calls with `o.admission.X()`
- Replace `o.admitRun()` with `o.admission.Admit()`
- Replace `o.releaseSingletonLock()` with `o.admission.ReleaseSingletonLock()`

- [ ] **Step 4: Run tests**

```bash
go test ./internal/engine/... -timeout 120s -count=1
go vet ./internal/engine/...
```

Expected: ALL tests pass (no behavioral change).

- [ ] **Step 5: Commit**

```bash
git add internal/engine/
git commit -m "refactor(engine): extract AdmissionController from Orchestrator

Moves admission pipeline (singleton, concurrency, priority) into a
focused AdmissionController type. Orchestrator delegates to it via
the admission field. No behavioral change.

Closes #71 (subsumes AdmissionController issue)"
```

---

## Chunk 2: ApprovalGate

### Task 2: Extract ApprovalGate subsystem

**Files:**
- Create: `internal/engine/approval_gate.go`
- Modify: `internal/engine/approval.go` (move types, keep as-is or merge)
- Modify: `internal/engine/orchestrator.go` (remove approval methods, add field)

- [ ] **Step 1: Create `approval_gate.go` with the ApprovalGate type**

```go
// approval_gate.go

package engine

import (
	"context"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

// ApprovalGate manages approval step lifecycle: token generation,
// storage, timeout scheduling, and grant/reject handling.
type ApprovalGate struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	approvalKV jetstream.KeyValue
	sleepTimer *SleepTimer
	tracer     trace.Tracer
}

func NewApprovalGate(
	nc *nats.Conn,
	js jetstream.JetStream,
	approvalKV jetstream.KeyValue,
	sleepTimer *SleepTimer,
	tracer trace.Tracer,
) *ApprovalGate {
	return &ApprovalGate{
		nc: nc, js: js,
		approvalKV: approvalKV,
		sleepTimer: sleepTimer,
		tracer:     tracer,
	}
}
```

- [ ] **Step 2: Move all approval methods from Orchestrator to ApprovalGate**

Methods to move (from `approval.go`):
- `enqueueApprovalStep` → `ApprovalGate.Enqueue()`
- `activateApprovalGate` → `ApprovalGate.activate()`
- `storeApprovalToken` → `ApprovalGate.storeToken()`
- `publishApprovalRequested` → `ApprovalGate.publishRequested()`
- `scheduleApprovalTimeout` → `ApprovalGate.scheduleTimeout()`
- `handleApprovalGranted` → `ApprovalGate.HandleGranted()`
- `handleApprovalRejected` → `ApprovalGate.HandleRejected()`
- `handleApprovalExpired` → `ApprovalGate.HandleExpired()`
- `cleanupApprovalTokens` → `ApprovalGate.CleanupTokens()`
- `deleteApprovalToken` → `ApprovalGate.deleteToken()`

Methods that need `saveSnapshot` or `enqueueReady` should take callback parameters or return side-effects that the Orchestrator executes.

- [ ] **Step 3: Update Orchestrator**

- Remove approvalKV field
- Add `approval *ApprovalGate` field
- In `NewOrchestrator()`: create ApprovalGate
- In `dispatchReadySteps()`: replace `o.enqueueApprovalStep()` with `o.approval.Enqueue()`
- In `dispatchEvent()`: replace `o.handleApprovalGranted/Rejected/Expired()` with `o.approval.HandleGranted/Rejected/Expired()`
- In `handleWorkflowCancelled()`: replace `o.cleanupApprovalTokens()` with `o.approval.CleanupTokens()`

- [ ] **Step 4: Run tests**

```bash
go test ./internal/engine/... -timeout 120s -count=1
```

Expected: ALL tests pass including all 5 approval_test.go tests.

- [ ] **Step 5: Commit**

```bash
git add internal/engine/
git commit -m "refactor(engine): extract ApprovalGate from Orchestrator

Moves approval token lifecycle (generate, store, timeout, grant,
reject, cleanup) into focused ApprovalGate type."
```

---

## Chunk 3: StickyRouter

### Task 3: Extract StickyRouter subsystem

**Files:**
- Create: `internal/engine/sticky_router.go`
- Modify: `internal/engine/sticky.go` (move or rename)
- Modify: `internal/engine/orchestrator.go`

- [ ] **Step 1: Create StickyRouter type**

```go
// sticky_router.go

package engine

import (
	"github.com/nats-io/nats.go/jetstream"
)

// StickyRouter manages worker affinity bindings. When a workflow
// uses sticky routing, the first completed step binds the run
// to the worker that completed it.
type StickyRouter struct {
	kv jetstream.KeyValue // sticky_bindings bucket
}

func NewStickyRouter(
	kv jetstream.KeyValue,
) *StickyRouter {
	if kv == nil {
		return nil
	}
	return &StickyRouter{kv: kv}
}
```

- [ ] **Step 2: Move sticky methods**

From `sticky.go`, change receiver from `Orchestrator` to `StickyRouter`:
- `createStickyBinding` → `StickyRouter.CreateBinding()`
- `getStickyWorker` → `StickyRouter.GetWorker()`
- `deleteStickyBinding` → `StickyRouter.DeleteBinding()`
- `publishStickyTask` → `StickyRouter.PublishTask()` (needs nc, js, tracer as params or fields)

- [ ] **Step 3: Update Orchestrator**

- Remove `stickyKV` field
- Add `sticky *StickyRouter` field (nil-safe)
- Replace `o.createStickyBinding()` → `o.sticky.CreateBinding()`
- Replace `o.getStickyWorker()` → `o.sticky.GetWorker()`
- Replace `o.deleteStickyBinding()` → `o.sticky.DeleteBinding()`
- Replace `o.publishStickyTask()` → `o.sticky.PublishTask()`

- [ ] **Step 4: Run tests, commit**

```bash
go test ./internal/engine/... -timeout 120s -count=1
git add internal/engine/
git commit -m "refactor(engine): extract StickyRouter from Orchestrator

Moves worker affinity binding lifecycle into focused StickyRouter type."
```

---

## Chunk 4: TaskPublisher

### Task 4: Extract TaskPublisher subsystem

This is the largest extraction — it owns rate limiting, task concurrency, subject routing, and the actual NATS publish. 

**Files:**
- Create: `internal/engine/task_publisher.go` (new type)
- Modify: `internal/engine/task_publish.go` (existing helpers become methods)
- Modify: `internal/engine/ratelimit.go` (becomes internal to TaskPublisher)
- Modify: `internal/engine/orchestrator.go`

- [ ] **Step 1: Define TaskPublisher type**

```go
// task_publisher.go

package engine

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/metric"
)

// TaskPublisher handles all task publication: rate limiting,
// task concurrency, sticky routing, subject resolution, and
// the actual NATS publish with dedup headers.
type TaskPublisher struct {
	nc          *nats.Conn
	js          jetstream.JetStream
	rateLimiter *RateLimiter
	admission   *AdmissionController // for task concurrency
	sticky      *StickyRouter
	sleepTimer  *SleepTimer
	stepRoutes  map[dag.StepType]string
	tracer      trace.Tracer

	// metrics
	stepEnqueueCount        metric.Int64Counter
	taskConcurrencyAcquired metric.Int64Counter
	taskConcurrencyRejected metric.Int64Counter
}
```

- [ ] **Step 2: Move task publishing methods to TaskPublisher**

From `orchestrator.go`:
- `publishTask` → `TaskPublisher.Publish()`
- `checkRateLimit` → `TaskPublisher.checkRateLimit()`
- `applyGlobalRateLimit` → `TaskPublisher.applyGlobalRateLimit()`
- `applyKeyedRateLimit` → `TaskPublisher.applyKeyedRateLimit()`
- `scheduleRateRetry` → `TaskPublisher.scheduleRateRetry()`
- `scheduleTaskConcurrencyRetry` → `TaskPublisher.scheduleConcurrencyRetry()`
- `doPublishTask` → `TaskPublisher.doPublish()`
- `publishIterationTask` → `TaskPublisher.PublishIteration()`
- `stepSubject` → `TaskPublisher.stepSubject()`
- `buildTaskMsg` → `TaskPublisher.buildMsg()`

From `task_publish.go`:
- `publishReadyTasks` → `TaskPublisher.PublishBatch()`
- `collectReadyMessages` → `TaskPublisher.collectMessages()`
- `publishAtomicBatches` → `TaskPublisher.publishAtomicBatches()`
- `publishWorkflowEvent` → stays on Orchestrator (workflow lifecycle, not task)

- [ ] **Step 3: Update Orchestrator**

- Remove fields: `rateLimiter`, `stepRoutes`, `stepEnqueueCount`, `taskConcurrencyAcquired`, `taskConcurrencyRejected`
- Add field: `publisher *TaskPublisher`
- Replace all `o.publishTask()` → `o.publisher.Publish()`
- Replace `o.publishReadyTasks()` → `o.publisher.PublishBatch()`
- Replace `o.publishIterationTask()` → `o.publisher.PublishIteration()`

- [ ] **Step 4: Run tests, commit**

```bash
go test ./internal/engine/... -timeout 120s -count=1
git add internal/engine/
git commit -m "refactor(engine): extract TaskPublisher from Orchestrator

Moves rate limiting, task concurrency, sticky routing, subject
resolution, and NATS publish into focused TaskPublisher type.
Orchestrator delegates all task dispatch through publisher."
```

---

## Chunk 5: RecoveryManager

### Task 5: Extract RecoveryManager subsystem

**Files:**
- Create: `internal/engine/recovery.go` (new type)
- Modify: `internal/engine/orchestrator.go`

- [ ] **Step 1: Define RecoveryManager type**

```go
// recovery.go

package engine

// RecoveryManager handles failure recovery: on-failure handlers,
// saga compensation chains, and dead-letter publishing.
type RecoveryManager struct {
	nc        *nats.Conn
	js        jetstream.JetStream
	publisher *TaskPublisher
	tracer    trace.Tracer
}
```

- [ ] **Step 2: Move recovery methods**

From `orchestrator.go`:
- `handlePermanentFailure` → `RecoveryManager.HandlePermanentFailure()`
- `failAuxStep` → `RecoveryManager.failAuxStep()`
- `tryOnFailureHandler` → `RecoveryManager.TryOnFailure()`
- `startCompensation` → `RecoveryManager.StartCompensation()`
- `handleCompensateStepCompleted` → `RecoveryManager.HandleCompensateCompleted()`
- `findCompensateSource` → `RecoveryManager.findCompensateSource()`
- `buildCompensateInput` → `RecoveryManager.buildCompensateInput()`
- `publishDeadLetter` → `RecoveryManager.PublishDeadLetter()`
- `recoverIfOnFailure` → `RecoveryManager.RecoverIfOnFailure()`

- [ ] **Step 3: Update Orchestrator**

- Add field: `recovery *RecoveryManager`
- In `handleStepFailed()`: delegate permanent failure to `o.recovery.HandlePermanentFailure()`
- In `handleStepCompleted()`: delegate compensation check to `o.recovery.HandleCompensateCompleted()`
- In `failWorkflow()`: delegate dead letter to `o.recovery.PublishDeadLetter()`

- [ ] **Step 4: Run tests, commit**

```bash
go test ./internal/engine/... -timeout 120s -count=1
git add internal/engine/
git commit -m "refactor(engine): extract RecoveryManager from Orchestrator

Moves on-failure handlers, saga compensation, and dead-letter
publishing into focused RecoveryManager type."
```

---

## Chunk 6: Final Cleanup

### Task 6: Verify Orchestrator is now a thin coordinator

- [ ] **Step 1: Audit remaining Orchestrator methods**

After all extractions, the Orchestrator should own only:
- **Lifecycle**: `NewOrchestrator`, `Start`, `Stop`
- **Event loop**: `handleEventJS`, `isHandledEventType`, `dispatchEvent`, `getRunLock`
- **Workflow lifecycle**: `handleWorkflowStarted`, `handleWorkflowCancelled`, `completeWorkflow`, `failWorkflow`, `startNextPendingRun`, `findOldestPendingRun`, `transitionPendingToRunning`
- **Step routing**: `handleStepCompleted`, `handleStepFailed`, `handleStepContinue`, `enqueueReady`, `dispatchReadySteps`
- **Child workflows**: `handleWorkflowSpawn`, `createChildRun`, `notifyParentIfChild`, `handleChildCompleted`, `handleChildFailed`
- **Map steps**: `enqueueMapStep`, `handleMapInstanceCompleted`, `handleMapInstanceFailed` (could be a future extraction)
- **Wait/Sleep/Sub-workflow enqueue**: `enqueueWaitForEventStep`, `enqueueSleepStep`, `enqueueSubWorkflow`
- **State**: `saveSnapshot`, `loadRunAndDef`
- **Events**: `publishWorkflowCompleted`, `publishWorkflowFailed`
- **Utilities**: `completedSet`, `queuedSet`, `countActiveSteps`, `findStepDef`, `checkLoopBounds`, `parseTraceparent`

Target: ~40 methods (down from 72), with the complex logic pushed into subsystems.

- [ ] **Step 2: Verify field count reduction**

Orchestrator fields should be approximately:
```go
type Orchestrator struct {
    nc         *nats.Conn
    js         jetstream.JetStream
    defKV      jetstream.KeyValue
    store      *SnapshotStore
    tracer     trace.Tracer
    cc         jetstream.ConsumeContext
    runLocks   sync.Map

    // Subsystems (5 extracted)
    admission  *AdmissionController
    approval   *ApprovalGate
    sticky     *StickyRouter
    publisher  *TaskPublisher
    recovery   *RecoveryManager

    // Remaining infrastructure
    sleepTimer *SleepTimer
    correlator *Correlator

    // Metrics (only workflow-level, task metrics moved to publisher)
    runsActive    metric.Int64UpDownCounter
    runsCompleted metric.Int64Counter
    runsFailed    metric.Int64Counter
    snapshotDuration metric.Float64Histogram
    failNonRetriable metric.Int64Counter
    failRetryAfter   metric.Int64Counter
}
```

Down from 24 fields to ~19, with clear ownership boundaries.

- [ ] **Step 3: Run full test suite**

```bash
go test ./... -timeout 120s -count=1
go vet ./...
```

- [ ] **Step 4: Final commit**

```bash
git add .
git commit -m "refactor(engine): verify Orchestrator extraction complete

Orchestrator reduced from 72 methods to ~40, with 5 subsystems
extracted: AdmissionController, ApprovalGate, StickyRouter,
TaskPublisher, RecoveryManager. All tests passing."
```

---

## Summary

| Chunk | Subsystem | Methods Moved | Fields Moved | Risk |
|-------|-----------|---------------|--------------|------|
| 1 | AdmissionController | 6 | 2 (concurrency, singletonKV) | Low |
| 2 | ApprovalGate | 10 | 1 (approvalKV) | Medium |
| 3 | StickyRouter | 4 | 1 (stickyKV) | Low |
| 4 | TaskPublisher | 14 | 4 (rateLimiter, stepRoutes, 3 metrics) | Medium |
| 5 | RecoveryManager | 9 | 0 (uses publisher, js, nc) | Medium |
| 6 | Cleanup | 0 | 0 | Low |
| **Total** | **5 subsystems** | **~43 methods** | **~8 fields** | |

Each chunk is independently committable and leaves all 81 engine tests passing.

# Idempotency & Sticky Workers Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement idempotency by expression (dedup workflow runs by input key) and sticky workers (bind runs to specific workers).

**Architecture:** Idempotency is API-layer only — KV check before run creation with atomic Create for race safety. Sticky workers add KV binding in the engine, worker-specific NATS subjects, and a `publishStickyTask` helper for hard/soft routing. Both features are independent and can be tested in isolation.

**Tech Stack:** Go, NATS JetStream KV, SLEEP_TIMERS (for soft sticky fallback)

**Specs:**
- `docs/superpowers/specs/2026-04-04-idempotency-design.md`
- `docs/superpowers/specs/2026-04-04-sticky-workers-design.md`

**Skills:** @tigerstyle @idiomatic-go @test-driven-development

---

## Part 1: Idempotency by Expression

### Task 1: Add IdempotencyKey to WorkflowDef + builder + validation

**Files:**
- Modify: `dag/types.go` — add `IdempotencyKey string` to WorkflowDef
- Modify: `dag/builder.go` — add `WithIdempotencyKey()` method
- Modify: `dag/validate.go` — validate dot-path syntax
- Test: `dag/builder_test.go`, `dag/validate_test.go`

- [ ] **Step 1: Add field to WorkflowDef**

In `dag/types.go`, add after `AuxSteps`:
```go
IdempotencyKey string `json:"idempotency_key,omitempty"`
```

- [ ] **Step 2: Add builder method**

In `dag/builder.go`:
```go
func (b *WorkflowBuilder) WithIdempotencyKey(
    dotPath string,
) *WorkflowBuilder {
    if dotPath == "" {
        panic("WithIdempotencyKey: dotPath must not be empty")
    }
    b.idempotencyKey = dotPath
    return b
}
```

Add `idempotencyKey string` to `WorkflowBuilder` struct. Set it on the def in `Build()`.

- [ ] **Step 3: Add validation**

In `dag/validate.go`, add `validateIdempotencyKey`:
```go
func validateIdempotencyKey(key string) error {
    if key == "" {
        return nil
    }
    if key[0] == '.' || key[len(key)-1] == '.' {
        return fmt.Errorf(
            "idempotency_key %q: must not start or end with dot",
            key,
        )
    }
    for i := range len(key) - 1 {
        if key[i] == '.' && key[i+1] == '.' {
            return fmt.Errorf(
                "idempotency_key %q: must not have empty segments",
                key,
            )
        }
    }
    return nil
}
```

Call from `Validate()` after step validation.

- [ ] **Step 4: Write tests, run, verify**

Run: `go test ./dag/ -v`

- [ ] **Step 5: Commit**

```bash
git add dag/types.go dag/builder.go dag/validate.go dag/builder_test.go dag/validate_test.go
git commit -m "feat(dag): add IdempotencyKey to WorkflowDef with builder and validation"
```

---

### Task 2: Add idempotency_keys KV bucket

**Files:**
- Modify: `natsutil/conn.go`

- [ ] **Step 1: Add KV bucket**

Add to `SetupKVBuckets`:
```go
{Bucket: "idempotency_keys", TTL: 24 * time.Hour},
```

- [ ] **Step 2: Run natsutil tests**

Run: `go test ./natsutil/ -v`

- [ ] **Step 3: Commit**

```bash
git add natsutil/conn.go
git commit -m "feat(natsutil): add idempotency_keys KV bucket"
```

---

### Task 3: Implement idempotency check in StartRun

**Files:**
- Modify: `api/service.go` — add idempotency logic to `startRunInner`
- Test: `api/service_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestStartRunIdempotency(t *testing.T) {
    // Register workflow with IdempotencyKey
    // Start run with input containing the key
    // Start same run again with same input
    // Positive: second call returns same run ID
    // Negative: only one workflow.started event published
}
```

- [ ] **Step 2: Implement idempotency check**

In `startRunInner`, after loading the def and before generating runID:

1. Unmarshal def (already done for schema validation)
2. If `IdempotencyKey` set, extract value via `dag.ExtractDotPath`
3. Hash: `sha256(workflowName + "." + value)` full hex
4. Try `idempotencyKV.Get(kvKey)` — if exists, return existing run ID
5. After creating run, `idempotencyKV.Create(kvKey, runID)` — if fails (race), get and return existing

- [ ] **Step 3: Run tests**

Run: `go test ./api/ -run TestStartRunIdempotency -v -timeout 30s`

- [ ] **Step 4: Commit**

```bash
git add api/service.go api/service_test.go
git commit -m "feat(api): implement idempotency check in StartRun"
```

---

## Part 2: Sticky Workers

### Task 4: Add StickyStrategy to WorkflowDef + builder + validation

**Files:**
- Modify: `dag/types.go` — add `StickyStrategy` type and `Sticky` field
- Modify: `dag/builder.go` — add `WithSticky()` method
- Modify: `dag/validate.go` — validate sticky constraints
- Test: `dag/builder_test.go`, `dag/validate_test.go`

- [ ] **Step 1: Add types**

```go
type StickyStrategy string

const (
    StickyNone StickyStrategy = ""
    StickySoft StickyStrategy = "soft"
    StickyHard StickyStrategy = "hard"
)
```

Add `Sticky StickyStrategy` to `WorkflowDef`.

- [ ] **Step 2: Add builder + validation**

Builder: `WithSticky(s StickyStrategy)`.
Validation: must be valid value; hard requires Timeout; reject per-step WorkerGroup when sticky.

- [ ] **Step 3: Write tests, run, verify**

Run: `go test ./dag/ -v`

- [ ] **Step 4: Commit**

```bash
git add dag/types.go dag/builder.go dag/validate.go dag/builder_test.go dag/validate_test.go
git commit -m "feat(dag): add StickyStrategy to WorkflowDef with builder and validation"
```

---

### Task 5: Add sticky_bindings KV bucket + WorkerID in events

**Files:**
- Modify: `natsutil/conn.go` — add `sticky_bindings` KV
- Modify: `worker/context.go` — include WorkerID in completion events
- Modify: `protocol/protocol.go` — add WorkerID field to step event payloads
- Test: `worker/context_test.go`

- [ ] **Step 1: Add KV bucket**

```go
{Bucket: "sticky_bindings", TTL: 25 * time.Hour},
```

- [ ] **Step 2: Add WorkerID to completion events**

Worker's `Complete()` and `Continue()` should include `workerID` in the event payload if set. Add `workerID` field to `taskContext` struct, set from worker config.

- [ ] **Step 3: Write tests, run**

Run: `go test ./worker/ -v && go test ./natsutil/ -v`

- [ ] **Step 4: Commit**

```bash
git add natsutil/conn.go worker/context.go worker/worker.go protocol/protocol.go
git commit -m "feat: add sticky_bindings KV and WorkerID in step events"
```

---

### Task 6: Implement sticky routing in engine

**Files:**
- Modify: `engine/orchestrator.go` — binding creation + `publishStickyTask`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing integration test**

Test: sticky workflow where step 1 runs, binding created, step 2 routes to same worker.

- [ ] **Step 2: Implement binding creation in handleStepCompleted**

After step completes, if workflow is sticky and no binding exists, read WorkerID from event payload, write to `sticky_bindings.{runID}`.

- [ ] **Step 3: Implement publishStickyTask**

```go
func (o *Orchestrator) publishStickyTask(
    ctx context.Context, runID string, step dag.StepDef,
    input []byte, attempt int, workerID string, strategy dag.StickyStrategy,
) error
```

Hard: publish to `task.{type}.{workerID}.{runID}` only.
Soft: publish to sticky subject, schedule SLEEP_TIMERS fallback (5s), fallback re-publishes to normal subject.

- [ ] **Step 4: Wire into publishTask**

Before `doPublishTask`, check sticky binding. If exists, delegate to `publishStickyTask`.

- [ ] **Step 5: Add binding cleanup on workflow complete/fail/cancel**

Delete `sticky_bindings.{runID}` in `completeWorkflow`, `failWorkflow`, `handleWorkflowCancelled`.

- [ ] **Step 6: Run tests**

Run: `go test ./engine/ -v -timeout 60s`

- [ ] **Step 7: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): implement sticky worker routing with binding + fallback"
```

---

### Task 7: Worker-specific subscriptions

**Files:**
- Modify: `worker/worker.go` — subscribe to `task.{type}.{workerID}.>` subjects
- Test: `worker/worker_test.go`

- [ ] **Step 1: Add workerID-based subscriptions in Start()**

If worker has a workerID, subscribe to both `task.{type}.>` and `task.{type}.{workerID}.>` for each task type.

- [ ] **Step 2: Write test**

Test: worker with workerID receives messages on worker-specific subject.

- [ ] **Step 3: Run tests**

Run: `go test ./worker/ -v`

- [ ] **Step 4: Commit**

```bash
git add worker/worker.go worker/worker_test.go
git commit -m "feat(worker): subscribe to worker-specific subjects for sticky routing"
```

---

### Task 8: Final verification

- [ ] **Step 1: Full test suite**

Run: `go test ./... -timeout 120s`

- [ ] **Step 2: Linters**

Run: `go vet ./... && gofmt -l .`

- [ ] **Step 3: Function length check**

All new functions under 70 lines.

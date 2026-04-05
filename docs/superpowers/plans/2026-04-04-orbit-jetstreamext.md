# Orbit jetstreamext Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> (if subagents available) or superpowers:executing-plans to implement this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add atomic task fan-out and batch stream reads using orbit.go/jetstreamext.

**Architecture:** After the JetStream API migration (separate plan), the engine
already holds `jetstream.JetStream`. This plan adds `jetstreamext` as a dep,
replaces the publish loop with `PublishMsgBatch`, and adds batch reads.

**Tech Stack:** Go, synadia-io/orbit.go/jetstreamext v0.2.1

**Spec:** `docs/superpowers/specs/2026-04-04-orbit-jetstreamext-design.md`

**Prerequisite:** JetStream API migration Chunk 1 (task_publish.go migrated).

---

## Chunk 1: Atomic Task Fan-Out

### Task 1: Add jetstreamext dependency and replace publish loop

**Files:**
- Modify: `go.mod`
- Modify: `internal/engine/task_publish.go`
- Create or modify: `internal/engine/task_publish_test.go`

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/synadia-io/orbit.go/jetstreamext@latest
```

This time the import will be used immediately, so `go mod tidy` keeps it.

- [ ] **Step 2: Write a failing integration test**

Test that `enqueueReadySteps` publishes all tasks atomically to TASK_QUEUES.
Use a real embedded NATS server. Verify message count in the stream.

Two assertions: all messages land (positive), stream has exactly N messages
(negative — no duplicates or extras).

- [ ] **Step 3: Enable AllowAtomicPublish on TASK_QUEUES**

`natsutil.EnableAtomicPublish(nc, "TASK_QUEUES")` is already implemented.
Call it in the test setup and in `SetupAll`.

- [ ] **Step 4: Replace the publish loop with PublishMsgBatch**

In `enqueueReadySteps`, replace the per-step publish loop with:

```go
msgs, err := collectReadyMessages(run.RunID, ready, run)
if err != nil {
    return err
}
// Split by stream (normal tasks vs agent tasks)
var taskMsgs, agentMsgs []*nats.Msg
for _, msg := range msgs {
    if strings.HasPrefix(msg.Subject, "agent_task.") {
        agentMsgs = append(agentMsgs, msg)
    } else {
        taskMsgs = append(taskMsgs, msg)
    }
}
if len(taskMsgs) > 0 {
    _, err = jetstreamext.PublishMsgBatch(
        ctx, o.jsNew, taskMsgs,
    )
    if err != nil {
        return fmt.Errorf("atomic task publish: %w", err)
    }
}
if len(agentMsgs) > 0 {
    _, err = jetstreamext.PublishMsgBatch(
        ctx, o.jsNew, agentMsgs,
    )
    if err != nil {
        return fmt.Errorf("atomic agent publish: %w", err)
    }
}
```

Remove the package-level `publishTask` function (only called from
`enqueueReadySteps`). Do NOT remove `Orchestrator.publishTask` (different
function in `orchestrator.go`).

- [ ] **Step 5: Run integration test**

```bash
go test ./internal/engine/ -run TestEnqueueReadySteps -v
```

- [ ] **Step 6: Run full engine + E2E tests**

```bash
go test ./internal/engine/ -v -timeout 120s
go test ./e2e/features/ -v -timeout 180s
```

- [ ] **Step 7: Add observability metric**

Add `publishBatchSize` histogram to Orchestrator. Record after each
`PublishMsgBatch` call.

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum internal/engine/
git commit -m "feat(engine): atomic task fan-out via jetstreamext.PublishMsgBatch"
```

---

## Chunk 2: Batch History Retrieval

### Task 2: Replace throwaway consumers with GetLastMsgsFor

**Files:**
- Modify: `internal/api/service.go` (or relevant history query method)

- [ ] **Step 1: Investigate current history query path**

Read `cli/inspect.go`, `cli/dlq.go`, `internal/api/service.go` to find
how messages are currently retrieved.

- [ ] **Step 2: Replace with jetstreamext.GetLastMsgsFor**

The API service needs `jetstream.JetStream` — add it as a field if not
already present from the migration.

```go
iter, err := jetstreamext.GetLastMsgsFor(
    ctx, svc.jsNew, "WORKFLOW_HISTORY",
    []string{"workflow." + workflowID + ".>"},
)
for msg, err := range iter {
    // process
}
```

- [ ] **Step 3: Run API + CLI tests**

```bash
go test ./internal/api/ ./cli/ -v -timeout 60s
```

- [ ] **Step 4: Commit**

```bash
git add internal/api/ cli/
git commit -m "refactor(api): batch history retrieval via jetstreamext"
```

### Task 3: Final validation

- [ ] **Step 1: `go test ./... -timeout 300s`**
- [ ] **Step 2: `go vet ./...`**

# Orbit pcgroups Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> (if subagents available) or superpowers:executing-plans to implement this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace worker subscriptions with pcgroups elastic consumer groups for
partition-based affinity, ordered delivery per workflow, and automatic failover.

**Architecture:** After the JetStream API migration (separate plan), the worker
holds `jetstream.JetStream` and handles `jetstream.Msg`. This plan adds pcgroups
as a dep, adds `WithPartitions`/`HandleSingleton` options, and replaces
subscription creation with `CreateElastic` + `ElasticConsume`.

**Tech Stack:** Go, synadia-io/orbit.go/pcgroups v0.2.1

**Spec:** `docs/superpowers/specs/2026-04-04-orbit-pcgroups-design.md`

**Prerequisite:** JetStream API migration Chunk 3 (worker subscriptions migrated).

---

## Chunk 1: Dependency + Singleton Schema

### Task 1: Add pcgroups dependency and Singleton field

**Files:**
- Modify: `go.mod`
- Modify: `dag/types.go`
- Modify: `dag/types_test.go`

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/synadia-io/orbit.go/pcgroups@latest
```

- [ ] **Step 2: Inspect the actual API**

```bash
go doc github.com/synadia-io/orbit.go/pcgroups CreateElastic
go doc github.com/synadia-io/orbit.go/pcgroups ElasticConsumerGroupConfig
```

Note the exact config struct fields for group creation.

- [ ] **Step 3: Write failing test for Singleton field**

```go
// dag/types_test.go
func TestStepDef_SingletonJSON(t *testing.T) {
    step := StepDef{ID: "x", Task: "t", Singleton: true}
    data, _ := json.Marshal(step)
    var got StepDef
    json.Unmarshal(data, &got)
    // Positive
    if !got.Singleton { t.Error("Singleton lost in round-trip") }
    // Negative
    step2 := StepDef{ID: "y", Task: "t"}
    data2, _ := json.Marshal(step2)
    if strings.Contains(string(data2), "singleton") {
        t.Error("non-singleton should omit field")
    }
}
```

- [ ] **Step 4: Add Singleton field to StepDef**

```go
Singleton bool `json:"singleton,omitempty"`
```

- [ ] **Step 5: Run tests**

```bash
go test ./dag/ -v
```

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum dag/
git commit -m "feat(dag): add Singleton field, add pcgroups dependency"
```

---

## Chunk 2: Elastic Consumer Groups in Worker

### Task 2: Add WithPartitions and HandleSingleton

**Files:**
- Modify: `worker/worker.go`
- Create: `worker/worker_pcgroups_test.go`
- Create: `worker/worker_singleton_test.go`

The Worker struct already has `groups []string` (worker groups). Use
`elasticGroups` for pcgroups handles. Use `singletons map[string]bool`
for singleton task types.

- [ ] **Step 1: Write failing integration test**

Test that a worker with `WithPartitions(2)` receives messages via
elastic consumer groups. Two tasks published, both received.

- [ ] **Step 2: Add fields and options**

```go
// Worker struct additions
partitions    int
singletons    map[string]bool
elasticGroups []pcgroups.ConsumerGroupConsumeContext
```

```go
func WithPartitions(n int) WorkerOption {
    if n < 0 { panic("WithPartitions: n must be >= 0") }
    if n > 256 { panic("WithPartitions: n must be <= 256") }
    return func(w *Worker) { w.partitions = n }
}

func (w *Worker) HandleSingleton(taskType string, handler HandlerFunc) {
    if taskType == "" { panic("HandleSingleton: taskType empty") }
    if handler == nil { panic("HandleSingleton: handler nil") }
    w.handlers[taskType] = handler
    if w.singletons == nil { w.singletons = make(map[string]bool) }
    w.singletons[taskType] = true
}
```

- [ ] **Step 3: Modify Start() for pcgroups path**

When `w.partitions > 0`, use pcgroups instead of legacy subscribe:

```go
if w.partitions > 0 {
    // Ensure group exists (idempotent)
    pcgroups.CreateElastic(ctx, w.jsNew, "TASK_QUEUES",
        pcgroups.ElasticConsumerGroupConfig{...})

    cc, err := pcgroups.ElasticConsume(
        ctx, w.jsNew, "TASK_QUEUES",
        "workers-"+taskType, w.workerID,
        func(msg jetstream.Msg) {
            w.handleMessage(tt, h, msg)
        },
        jetstream.ConsumerConfig{
            FilterSubject: "task." + taskType + ".>",
            AckPolicy:     jetstream.AckExplicitPolicy,
        },
    )
    w.elasticGroups = append(w.elasticGroups, cc)
}
```

For singletons, the group name is `"singleton-"+taskType` and
the consumer group config uses a single partition.

Handle the interaction with worker groups (`w.groups`): when both
are configured, the filter subject includes the group name.

- [ ] **Step 4: Update Stop() to close elastic groups**

```go
for _, cc := range w.elasticGroups {
    cc.Stop()
}
```

- [ ] **Step 5: Run integration tests**

```bash
go test ./worker/ -run TestWorker_ElasticConsume -v -timeout 30s
go test ./worker/ -run TestWorker_Singleton -v -timeout 30s
```

- [ ] **Step 6: Run full worker + E2E tests**

```bash
go test ./worker/ -v -timeout 60s
go test ./e2e/features/ -v -timeout 180s
```

- [ ] **Step 7: Add observability**

Add `partitionActiveMembers` gauge and `partitionRebalances` counter.
Log partition assignment on startup.

- [ ] **Step 8: Commit**

```bash
git add worker/
git commit -m "feat(worker): elastic consumer groups and singleton via pcgroups"
```

### Task 3: Final validation

- [ ] **Step 1: `go test ./... -timeout 300s`**
- [ ] **Step 2: `go vet ./...`**

# E2E Test Suite Design

## Goal

Validate that every DagNats feature works correctly through the full stack (API → orchestrator → worker → NATS), across multiple NATS topologies, including under infrastructure disruption. Serves as a CI gate — no PR merges with broken features.

## Architecture

### Write Once, Run Everywhere

Each test is a function with signature `func(t *testing.T, nc *nats.Conn)`. The harness provides the connection — the test does not know or care whether it talks to an embedded server or a 5-node supercluster. A `RunE2E` function iterates over enabled topologies and runs each test against each one.

```go
type E2ETest func(t *testing.T, nc *nats.Conn)

func RunE2E(t *testing.T, name string, test E2ETest) {
    topos := enabledTopologies()
    if len(topos) == 0 {
        t.Fatal("RunE2E: no topologies enabled — check E2E_TOPOLOGY")
    }
    for _, topo := range topos {
        t.Run(topo.Name, func(t *testing.T) {
            nc := topo.Connect(t)
            test(t, nc)
        })
    }
}
```

`enabledTopologies()` panics on unrecognized `E2E_TOPOLOGY` values — a typo like `embeded` is a programmer error, not a silent empty set.

Topology selection via environment variable:

```bash
E2E_TOPOLOGY=embedded go test ./e2e/features/...       # fast, CI default
E2E_TOPOLOGY=local_cluster go test ./e2e/features/...   # single nats-server
E2E_TOPOLOGY=supercluster go test ./e2e/features/...    # full topology
go test ./e2e/features/...                               # all topologies
```

### Three Topologies

**Embedded** — Single in-process NATS server. Uses existing `natsutil.StartTestServer`. Per-test lifecycle. Fastest (~20s for all feature tests).

**Local Cluster** — One `nats-server` instance with JetStream, started as an embedded Go server with a separate config (not the test helper's minimal config). Per-suite lifecycle via `TestMain`. Each test uses unique workflow names and run IDs for isolation (the existing `SetupAll` creates fixed-name buckets; tests share them but don't collide on keys). Validates that a production-like single-server config works (~30s).

**Supercluster** — Five in-process NATS servers:

```
┌─────────────┐     gateway     ┌─────────────┐
│  Cluster A  │ ◄─────────────► │  Cluster B  │
│  (2 nodes)  │                 │  (2 nodes)  │
└──────┬──────┘                 └─────────────┘
       │ leaf
┌──────┴──────┐
│  Leaf Node  │
│ (embedded)  │
└─────────────┘
```

All five servers run in-process using the `nats-server` Go library (already a dependency). Random ports, zero external dependencies. Tests connect through the leaf node — the full routing path is exercised: leaf → Cluster A → gateway → Cluster B.

Port allocation: 5 client + 4 cluster route + 4 gateway + 2 leaf listener = 15 random ports.

JetStream: enabled on both clusters with no domain isolation (plain `nc.JetStream()` works from the leaf without domain prefixes). Leaf has no JetStream of its own — proxies through Cluster A.

Per-suite lifecycle. ~45s for all feature tests.

### Topology Interface

Feature tests receive `*nats.Conn`. Resilience tests receive a richer `Topology` interface for infrastructure manipulation:

```go
type Topology interface {
    Connect(t *testing.T) *nats.Conn
    KillNode(name string) error
    RestartNode(name string) error
    DisconnectLeaf() error
    ReconnectLeaf() error
}
```

**Bounds on node names:** `KillNode`/`RestartNode` panic on unknown node names (programmer error). Valid names for supercluster: `"a1"`, `"a2"`, `"b1"`, `"b2"`.

**Bounded waits on infrastructure methods:** `RestartNode` and `ReconnectLeaf` wait up to 10s for cluster rejoin / leaf connection. Panic on timeout — a server that doesn't rejoin is a test infrastructure failure, not a test result.

Only the supercluster provider implements the full interface. Resilience tests skip on simpler topologies via `t.Skip("requires supercluster")`.

## Test Categories

### Category 1: Feature Correctness (17 tests)

Each test exercises one feature through the full stack. Happy path plus the most important failure path per feature.

| Test | What it validates |
|---|---|
| `TestLinearWorkflow` | A→B→C sequential steps complete in order. Verify history stream contains workflow.started, step.completed×3, workflow.completed in sequence. |
| `TestParallelFanOut` | A→(B,C,D)→E — three parallel steps all execute, join step waits for all three. Verify all 5 steps complete, E runs last. |
| `TestRetryExhaustion` | Step returns a retryable error. Workflow has `DefaultRetry` with `MaxAttempts: 3`. Step fails 4 times. Verify retries exhausted, step appears in DLQ. Replay via `api.Service.ReplayDeadLetter`. Verify replayed task arrives on queue. |
| `TestNonRetryableError` | Step returns `worker.NewNonRetryableError`. Verify immediate failure — no retries, step goes directly to DLQ. |
| `TestSignalWait` | Two-step workflow: step 1 calls `WaitForSignal("approval", timeout)`. External call via `api.Service.SendSignal` delivers the signal. Verify step 1 unblocks and completes. |
| `TestChildWorkflow` | Parent workflow has a step that publishes a `workflow.spawn` event (via the orchestrator's spawn mechanism). Child workflow runs and completes. Verify `workflow.child.completed` event on parent's history. Verify both runs in KV with `ParentRunID`/`ParentStepID` linkage. Note: the orchestrator notifies the parent via `notifyParentIfChild` but does not auto-complete the parent step — the test verifies the notification event, not automatic step completion. |
| `TestAgentLoop` | Step calls `Continue()` 5 times with `Checkpoint()` each iteration. On 6th iteration, calls `Complete()`. Verify 6 `step.continue` events in history, checkpoint KV entries persist. |
| `TestConcurrencyLimit` | Register workflow with `Concurrency: {MaxRuns: 2}`. Start 3 runs. Verify 2 running, 1 pending. Complete one run. Verify pending auto-starts (via `startNextPendingRun`). |
| `TestWorkflowTimeout` | Workflow with 2s timeout. Step is an agent loop that calls `Continue()` every 500ms. Verify that on the next event dispatch after the deadline, the orchestrator cancels the workflow. The timeout is enforced lazily — checked in `dispatchEvent` before routing to the handler (not by a background timer). |
| `TestConditionalSkip` | Three-step workflow: A→B→C. Step A outputs `{"action":"skip"}`. Step B has `SkipIf: &ParentCond{StepID:"A", Field:"action", Op:"==", Value:"skip"}`. Verify B status is Skipped, C still runs (skipped steps count as satisfied for downstream deps). |
| `TestOnFailureHandler` | Two steps: main + fallback (linked via `OnFailure` field). Main step fails permanently. Verify fallback step executes with error context in its input (`failed_step` and `error` fields). |
| `TestCronTrigger` | Register cron trigger via `api.Service.CreateTrigger`. Force tick via `TriggerService.TickNow()`. Verify workflow run created, events published to history, workflow completes. |
| `TestWebhookTrigger` | Register webhook trigger with path. HTTP POST to `TriggerService.WebhookHandler()` via `httptest`. Verify workflow run created and completes. |
| `TestSubjectTrigger` | Register a subject trigger that fires on a NATS subject. Publish a message to that subject. Verify workflow run starts and completes. |
| `TestInputSchemaValidation` | Register workflow with `InputSchema`. Start run with invalid input. Verify run is created with `RunStatusFailed` immediately (not queued). Start run with valid input. Verify it proceeds normally. |
| `TestWorkerGroups` | Workflow with step that has `WorkerGroup: "gpu"`. Start two workers: one with `WithGroups("gpu")`, one without. Verify only the GPU worker receives the task (task routes to `task.{task}.gpu.{runID}`). |
| `TestDeduplication` | Publish the same workflow.started event twice with identical `Nats-Msg-Id`. Verify only one run is created (JetStream dedup). |

### Category 2: Multi-Topology (same 17 tests)

No new test code. `RunE2E` runs each feature test against all enabled topologies. This validates that NATS gateway routing, leaf node forwarding, and cross-cluster JetStream replication do not break any feature.

### Category 3: Resilience (4 tests, supercluster only)

| Test | What it validates |
|---|---|
| `TestNodeFailureMidWorkflow` | Start a 3-step workflow on supercluster. After step 1 completes, kill one node in Cluster A. Verify steps 2 and 3 complete via surviving node. Restart killed node. Verify cluster healthy. |
| `TestLeafDisconnectReconnect` | Start a workflow. Disconnect leaf after step 1. Worker is unreachable. Reconnect leaf. Verify step 2 is delivered and workflow completes. No duplicate step executions (check via MsgId dedup). |
| `TestClusterFailover` | Start workflow. Kill entire Cluster A (both nodes). Verify Cluster B continues serving. Restart Cluster A. Verify eventual consistency — all KV state matches. |
| `TestSplitBrainRecovery` | Simulate partition by shutting down and restarting gateway nodes with routes removed, then restoring routes and restarting again. This is a full-restart simulation, not a true network partition — documented limitation of the in-process approach. Verify no duplicate run IDs, no lost step completions, KV state converges after healing. |

## Package Structure

```
e2e/
├── harness/
│   ├── harness.go          # RunE2E, E2ETest type, enabledTopologies()
│   ├── topology.go         # Topology interface definition
│   ├── embedded.go         # Embedded single-server provider
│   ├── local_cluster.go    # Single nats-server provider
│   ├── supercluster.go     # 2 clusters + leaf node provider
│   └── helpers.go          # waitForRunStatus, registerAndStart, etc.
├── features/
│   ├── linear_test.go
│   ├── fanout_test.go
│   ├── retry_dlq_test.go   # TestRetryExhaustion + TestNonRetryableError
│   ├── signals_test.go
│   ├── child_test.go
│   ├── agent_loop_test.go
│   ├── concurrency_test.go
│   ├── timeout_test.go
│   ├── conditional_test.go
│   ├── on_failure_test.go
│   ├── cron_test.go
│   ├── webhook_test.go
│   ├── subject_trigger_test.go
│   ├── input_schema_test.go
│   ├── worker_groups_test.go
│   └── dedup_test.go
└── resilience/
    ├── node_failure_test.go
    ├── leaf_reconnect_test.go
    ├── cluster_failover_test.go
    └── split_brain_test.go
```

**`e2e/harness/`** — not a test package. Exports the harness, topology providers, and helpers. Imported by `e2e/features/` and `e2e/resilience/`.

**`e2e/features/`** — one file per feature test. Each file contains one or two `TestXxx` functions that call `harness.RunE2E`. Tests import only public APIs: `api.Service`, `engine.Orchestrator`, `worker.Worker`, `trigger.TriggerService`.

**`e2e/resilience/`** — one file per resilience scenario. Each test creates a supercluster topology directly and uses the `Topology` interface to manipulate infrastructure.

## Shared Helpers

```go
// harness/helpers.go

// waitForRunStatus polls until the run reaches the expected status
// or the timeout expires. Bounded polling with 250ms interval
// (generous enough for cross-cluster propagation, negligible
// overhead on embedded).
func waitForRunStatus(
    t *testing.T, svc *api.Service,
    runID string, status dag.RunStatus, timeout time.Duration,
) dag.WorkflowRun

// registerAndStart registers a workflow definition and starts a run.
// Returns the run ID. Fails the test on any error.
func registerAndStart(
    t *testing.T, svc *api.Service,
    wfDef dag.WorkflowDef, input []byte,
) string

// subscribeWorker creates a Worker with the given handler, starts it,
// and registers cleanup via t.Cleanup(). Returns the worker for
// additional configuration if needed.
func subscribeWorker(
    t *testing.T, nc *nats.Conn,
    taskName string, handler worker.HandlerFunc,
) *worker.Worker

// assertHistoryContains verifies that the run's history stream
// contains the expected event types in order.
func assertHistoryContains(
    t *testing.T, svc *api.Service,
    runID string, eventTypes ...protocol.EventType,
)

// newTestService creates an api.Service with noop telemetry,
// calls SetupAll, and registers cleanup. The standard way to
// get a fully wired service in E2E tests.
func newTestService(
    t *testing.T, nc *nats.Conn,
) *api.Service

// uniqueName returns a test-unique name using t.Name() to prevent
// key collisions when tests share KV buckets (local_cluster, supercluster).
func uniqueName(t *testing.T, base string) string
```

## Test Conventions

**Methodology comments:** Every test file opens with a comment describing goal and methodology. Example:

```go
// e2e/features/signals_test.go
// Tests cross-step signal coordination. Methodology: start a two-step
// workflow where step 1 blocks on WaitForSignal, deliver signal via
// api.Service.SendSignal, verify step completes and workflow finishes.
```

**Minimum 2 assertions per test:** Each test asserts both positive space (the thing happened) and negative space (the thing that shouldn't have happened didn't). Examples:
- `TestDeduplication`: assert one run exists (positive) AND assert no second run exists (negative)
- `TestConditionalSkip`: assert B is Skipped (positive) AND assert C completed (not skipped, not failed)
- `TestWorkerGroups`: assert GPU worker received task (positive) AND assert non-GPU worker did not (negative)

**Bounded timeouts on all waits:** Every `waitForRunStatus` call uses an explicit timeout. No unbounded polling.

## Supercluster Implementation

All five servers use the `nats-server` embedded Go library (`github.com/nats-io/nats-server/v2/server`). No shell processes.

**Startup sequence:**
1. Allocate 13 random ports (5 client + 4 cluster route + 4 gateway)
2. Start Cluster A node 1 and node 2 with cluster routes pointing to each other
3. Start Cluster B node 1 and node 2 with cluster routes pointing to each other
4. Configure gateway routes: A→B and B→A
5. Start leaf node with leaf remote pointing to Cluster A node 1
6. Wait for all cluster formations and gateway connections (bounded 10s timeout)
7. Connect to leaf node, call `SetupAll` to create streams/KV

**Resilience methods:**
- `KillNode("a1")` → `clusterA[0].Shutdown()`, store opts for restart
- `RestartNode("a1")` → create new server from stored opts, wait for cluster rejoin
- `DisconnectLeaf()` → `leaf.Shutdown()`
- `ReconnectLeaf()` → create new leaf from stored opts, wait for leaf connection
- `Partition("a", "b")` → shutdown gateway nodes, restart without gateway routes (full-restart simulation, not true network partition — this is a documented limitation of the in-process approach; true partition testing would require OS-level network manipulation or an external tool like Antithesis)

**Cleanup:** `t.Cleanup` shuts down in reverse order: leaf → Cluster B → Cluster A.

## CI Integration

```yaml
jobs:
  test:
    # Existing: unit + integration
    steps:
      - run: go test ./... -count=1 -race -timeout 120s

  e2e:
    needs: test
    strategy:
      matrix:
        topology: [embedded, local_cluster, supercluster]
    steps:
      - run: >-
          E2E_TOPOLOGY=${{ matrix.topology }}
          go test ./e2e/features/... -count=1 -race -timeout 120s

  e2e-resilience:
    needs: e2e
    steps:
      - run: go test ./e2e/resilience/... -count=1 -timeout 180s
```

**Gating:** All three jobs must pass for PR merge. `needs` chain short-circuits — unit failure skips E2E.

**No `-race` on resilience tests** — race detector overhead interacts poorly with server shutdown/restart timing.

**Timeouts:**
- Feature tests: 120s (17 tests, each bounded at ~5s, per topology)
- Resilience tests: 180s (4 tests with node restart delays up to 10s each)

## What Is NOT Tested

- **Performance/benchmarks** — this suite validates correctness, not throughput
- **Multi-process deployment** — all components run in-process; separate binary deployment is an ops concern
- **External integrations** — no real Jaeger, no real cron daemon, no real HTTP clients beyond httptest
- **REST/NATS API routing** — tests exercise `api.Service` directly, not `api/rest.go` or `api/natsapi.go` HTTP/NATS handlers (those are covered by existing per-package tests in `api/rest_test.go` and `api/natsapi_test.go`)
- **Actor system internals** — `actor/` package and `engine/workflow_actor.go` are tested by existing per-package integration tests; E2E tests exercise the orchestrator which uses actors internally
- **PutStream / Heartbeat** — worker streaming and heartbeat extension are TaskContext features best validated at the integration level (worker package tests)
- **Existing per-package tests** — the E2E suite complements, not replaces. Unit and integration tests remain as-is.

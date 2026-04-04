# E2E Test Suite

## Design Decision: Write Once, Run Everywhere

Each test is a function `func(t *testing.T, nc *nats.Conn)`. The harness provides the connection — the test does not know or care whether it talks to an embedded server or a 5-node supercluster. `RunE2E` iterates over enabled topologies and runs each test against each one.

Topology selection via `E2E_TOPOLOGY` env var: `embedded`, `local_cluster`, `supercluster`, or empty for all. Panics on unrecognized values (typo = programmer error).

## Three Topologies

| Topology | Servers | Lifecycle | Speed |
|----------|---------|-----------|-------|
| Embedded | 1 in-process NATS | Per-test | ~20s |
| Local Cluster | 1 production-like NATS (explicit JetStream limits) | Per-suite | ~30s |
| Supercluster | 2 clusters (2 nodes each) + 1 leaf node | Per-suite | ~45s |

**Supercluster layout:**

```
Cluster A (2 nodes) <--gateway--> Cluster B (2 nodes)
       |
     leaf
  Leaf Node
```

All 5 servers run in-process via `nats-server` Go library. 15 random ports. Tests connect through the leaf — full routing path exercised. JetStream enabled on both clusters, no domain isolation. Leaf proxies through Cluster A.

## Test Categories

**Feature Correctness (17 tests):** Each exercises one feature through the full stack. Happy path plus the most important failure path per feature.

| Test | Validates |
|------|-----------|
| LinearWorkflow | A->B->C sequential completion and event ordering |
| ParallelFanOut | A->(B,C,D)->E fan-out/join |
| RetryExhaustion | Retries exhaust -> DLQ entry -> replay |
| NonRetryableError | Immediate failure, no retries, direct to DLQ |
| SignalWait | WaitForSignal blocks, SendSignal unblocks |
| ChildWorkflow | Parent spawn -> child runs -> parent notified |
| AgentLoop | Continue() iterations with Checkpoint() persistence |
| ConcurrencyLimit | MaxRuns enforced, pending auto-starts on release |
| WorkflowTimeout | Lazy deadline check cancels on next event dispatch |
| ConditionalSkip | SkipIf evaluates, skipped steps satisfy downstream deps |
| OnFailureHandler | Permanent failure -> fallback step with error context |
| CronTrigger | Cron expression -> TickNow() -> workflow run |
| WebhookTrigger | HTTP POST -> webhook handler -> workflow run |
| SubjectTrigger | NATS publish -> subject subscription -> workflow run |
| InputSchemaValidation | Invalid input -> immediate fail; valid -> proceeds |
| WorkerGroups | Group routing: only gpu-group worker receives gpu task |
| Deduplication | Duplicate Nats-Msg-Id -> single run created |

**Resilience (4 tests, supercluster only):**

| Test | Validates |
|------|-----------|
| NodeFailureMidWorkflow | Kill node after step 1, remaining steps complete via survivor |
| LeafDisconnectReconnect | Disconnect leaf, reconnect, step delivered without duplicates |
| ClusterFailover | Kill entire cluster, other continues, restart, KV converges |
| SplitBrainRecovery | Gateway restart simulation, no duplicate runs, KV converges |

## Topology Interface

```go
type Topology interface {
    Name() string
    Connect(t *testing.T) *nats.Conn
    Setup(t *testing.T, nc *nats.Conn)
}

type Resilient interface {
    Topology
    KillNode(name string) error      // "a1", "a2", "b1", "b2"
    RestartNode(name string) error   // waits 10s for rejoin
    DisconnectLeaf() error
    ReconnectLeaf() error
}
```

Panics on unknown node names, panics on rejoin timeout. Infrastructure failures are test infrastructure errors, not test results.

## Shared Helpers

- `RunE2E(t, test)` — iterate topologies, connect, setup, run
- `UniqueName(t, base)` — atomic counter prevents KV key collisions
- `NewTestService(t, nc)` — api.Service with noop telemetry
- `RegisterAndStart(t, svc, wfDef, input)` — register + start, returns runID
- `WaitForRunStatus(t, svc, runID, status, timeout)` — 250ms poll, bounded
- `SubscribeWorker(t, nc, taskName, handler)` — start worker with t.Cleanup
- `AssertHistoryContains(t, svc, runID, eventTypes...)` — subsequence check

## Test Conventions

- Methodology comment at top of every test file
- Minimum 2 assertions per test (positive + negative space)
- Bounded timeouts on all waits (15s for WaitForRunStatus)
- Orchestrator started per-test (RunE2E only provides connection)
- No `-race` on resilience tests (detector overhead + server restart timing)

## CI Integration

```yaml
jobs:
  test:        # unit + integration: go test ./...
  e2e:         # matrix: [embedded, local_cluster, supercluster] x feature tests
    needs: test
  e2e-resilience:  # supercluster resilience tests
    needs: e2e
```

All three jobs must pass for PR merge. `needs` chain short-circuits.

## Package Structure

```
e2e/
  harness/       # RunE2E, topology providers, helpers (not a test package)
  features/      # one file per feature test
  resilience/    # one file per resilience scenario (supercluster only)
```

## What Is NOT Tested

- Performance/benchmarks (correctness only)
- Multi-process deployment (all in-process)
- REST/NATS API routing (covered by per-package tests)
- Actor system internals (covered by engine integration tests)
- PutStream/Heartbeat (worker-level integration tests)

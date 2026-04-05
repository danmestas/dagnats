# DX Quick Wins Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the 3 highest-leverage DX gaps: export test helpers, add missing examples, and surface Checkpointable/Signaler on TaskContext.

**Architecture:** Three independent changes. Test helpers get a public `dagnatstest` package. Examples are standalone Go programs. TaskContext interface grows to include checkpoint/signal methods directly (removing the type-assertion requirement).

**Tech Stack:** Go, NATS JetStream

**Note:** Two items from the original audit are already done:
- Error messages already have "Hint: run 'dagnats serve'" guidance
- `dagnats workflow validate` already exists

---

## Task 1: Export Test Helpers (`dagnatstest` package)

The `internal/natsutil.StartTestServer` is used in 60+ test files but lives in
`internal/` — external users can't import it. Create a public `dagnatstest`
package that provides test server + setup in one call.

**Files:**
- Create: `dagnatstest/dagnatstest.go`
- Create: `dagnatstest/dagnatstest_test.go`

- [ ] **Step 1: Create the package**

```go
// dagnatstest/dagnatstest.go
// Test helpers for DagNats workflows. Provides a one-call setup
// that starts an embedded NATS server with all streams and KV
// buckets, ready for workflow testing.
package dagnatstest

import (
    "testing"

    "github.com/danmestas/dagnats/internal/natsutil"
    "github.com/nats-io/nats.go"
)

// Server starts an embedded NATS server with JetStream and all
// required streams/KV buckets provisioned. Returns the connected
// client. Server and connection are cleaned up automatically when
// the test ends.
//
// Usage:
//
//     func TestMyWorkflow(t *testing.T) {
//         nc := dagnatstest.Server(t)
//         // nc is ready — register workflows, start workers, etc.
//     }
func Server(t *testing.T) *nats.Conn {
    t.Helper()
    _, nc := natsutil.StartTestServer(t)
    if err := natsutil.SetupAll(nc); err != nil {
        t.Fatalf("dagnatstest.Server: SetupAll failed: %v", err)
    }
    return nc
}
```

- [ ] **Step 2: Write test**

```go
// dagnatstest/dagnatstest_test.go
package dagnatstest

import "testing"

func TestServer(t *testing.T) {
    nc := Server(t)
    // Positive: connection is live
    if !nc.IsConnected() {
        t.Fatal("expected connected NATS client")
    }
    // Positive: JetStream available
    js, err := nc.JetStream()
    if err != nil {
        t.Fatalf("JetStream: %v", err)
    }
    // Positive: workflow_defs bucket exists
    _, err = js.KeyValue("workflow_defs")
    if err != nil {
        t.Fatalf("workflow_defs bucket: %v", err)
    }
}
```

- [ ] **Step 3: Run test**

Run: `go test ./dagnatstest/ -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add dagnatstest/
git commit -m "feat: add dagnatstest package for one-call test setup"
```

---

## Task 2: Surface Checkpointable/Signaler on TaskContext

Currently developers must type-assert to `worker.Checkpointable` and
`worker.Signaler` — a discoverable API gap. Add the methods directly
to `TaskContext` so they appear in autocomplete and documentation.

**Files:**
- Modify: `worker/worker.go` — expand TaskContext interface
- Modify: `worker/worker.go` — keep Checkpointable/Signaler as aliases (backward compat)

- [ ] **Step 1: Expand TaskContext interface**

Add Checkpoint/Signal methods to TaskContext directly:

```go
type TaskContext interface {
    Input() []byte
    RunID() string
    StepID() string
    RetryCount() int
    Complete(output []byte) error
    Fail(err error) error
    FailPermanent(err error) error
    FailRetryAfter(err error, after time.Duration) error
    Continue(output []byte) error
    PutStream(data []byte) error
    Heartbeat() error
    Checkpoint(state []byte) error
    LoadCheckpoint() ([]byte, error)
    Pause(name string, duration time.Duration) error
    WaitForSignal(name string, timeout time.Duration) ([]byte, error)
    SendSignal(runID, name string, data []byte) error
}
```

Keep `Checkpointable` and `Signaler` interfaces as type aliases for backward
compatibility (anyone type-asserting still compiles):

```go
// Checkpointable is satisfied by TaskContext directly.
// Kept for backward compatibility — prefer using TaskContext methods.
type Checkpointable = TaskContext

// Signaler is satisfied by TaskContext directly.
// Kept for backward compatibility — prefer using TaskContext methods.
type Signaler = TaskContext
```

Wait — that won't work because `Checkpointable` has a subset of methods.
The simpler approach: keep both interfaces, update the doc comment on
`TaskContext` to list all available methods, and add a code example.
Actually the cleanest approach: just merge them. The `taskContext` struct
already implements all methods. Anyone type-asserting will still compile
because the concrete type satisfies both the old interfaces and the new
combined one.

- [ ] **Step 2: Verify existing tests pass**

Run: `go test ./worker/ -v -timeout 30s`
Expected: All PASS — concrete `taskContext` already has all methods

- [ ] **Step 3: Update doc comment**

Update TaskContext comment to list checkpoint/signal methods and remove the
"type-assert to Checkpointable or Signaler" instruction.

- [ ] **Step 4: Commit**

```bash
git add worker/worker.go
git commit -m "feat(worker): surface checkpoint and signal methods on TaskContext"
```

---

## Task 3: Add Map, SubWorkflow, and Agent Examples

Three new example directories following the existing hello-world pattern:
each has a `workflow.json` and `main.go`.

**Files:**
- Create: `examples/map-step/workflow.json`
- Create: `examples/map-step/main.go`
- Create: `examples/sub-workflow/workflow.json`
- Create: `examples/sub-workflow/main.go`

(Agent example deferred — requires dagnats-agents repo integration)

- [ ] **Step 1: Create map-step example**

`examples/map-step/main.go` — fan-out over a list of URLs, fetch each in
parallel, collect results.

- [ ] **Step 2: Create sub-workflow example**

`examples/sub-workflow/main.go` — parent workflow spawns a child workflow
and waits for its result.

- [ ] **Step 3: Verify examples compile**

Run: `go build ./examples/map-step/ && go build ./examples/sub-workflow/`
Expected: Both compile

- [ ] **Step 4: Commit**

```bash
git add examples/map-step/ examples/sub-workflow/
git commit -m "docs: add map-step and sub-workflow examples"
```

---

## Task 4: Final verification

- [ ] **Step 1: Full test suite**

Run: `go test ./... -timeout 120s`

- [ ] **Step 2: Linters**

Run: `go vet ./... && gofmt -l .`

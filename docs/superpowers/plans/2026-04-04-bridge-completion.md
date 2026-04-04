# Bridge Completion Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the HTTP bridge by integrating it into `dagnats serve`, adding signal support, observability, and a Go HTTP reference client.

**Architecture:** Four independent additions to the existing bridge. Each is shippable on its own. The bridge package already exists with connect/poll/resolve endpoints, ack map, and bearer token auth.

**Tech Stack:** Go, NATS JetStream, net/http, observe.Telemetry

**Spec:** `docs/superpowers/specs/2026-04-04-tier1-workflow-primitives-design.md` sections 8a-8d

**Critical existing signatures:**
- `bridge.NewBridge(nc *nats.Conn, js nats.JetStreamContext) *Bridge`
- `bridge.Handler() http.Handler` — returns mux with `/v1/` routes
- `server/server.go:startHTTP()` — creates mux, mounts REST API + health + webhooks
- `server/server.go:startComponents()` — creates orchestrator, triggers, workers
- `worker/context.go` has `WaitForSignal(name, timeout)` and `SendSignal(runID, name, data)` using `signals` KV bucket
- `observe.Telemetry` has `Tracer`, `Logger`, `Metrics`, `Errors` interfaces

---

## Task 1: Bridge Integration with `dagnats serve`

**Files:**
- Modify: `server/server.go:28-55` (Server struct — add bridge field)
- Modify: `server/server.go:86-191` (startComponents — create bridge)
- Modify: `server/server.go:193-223` (startHTTP — mount bridge handler)
- Test: `server/server_test.go`

- [ ] **Step 1: Write failing test**

```go
// server/server_test.go
func TestServerMountsBridgeEndpoints(t *testing.T) {
    cfg := testConfig(t)
    srv := New(cfg)
    errCh := make(chan error, 1)
    go func() { errCh <- srv.Run() }()

    // Wait for ready
    readyURL := fmt.Sprintf("http://%s/ready", cfg.HTTPAddr)
    waitForReady(t, readyURL, 10*time.Second)

    // Verify bridge endpoint responds
    body := `{"worker_id":"w-1","task_types":["echo"],"max_tasks":1}`
    resp, err := http.Post(
        fmt.Sprintf("http://%s/v1/workers/connect", cfg.HTTPAddr),
        "application/json",
        strings.NewReader(body),
    )
    assert(t, err == nil, "connect must succeed: %v", err)
    assert(t, resp.StatusCode == 200, "expected 200, got %d", resp.StatusCode)
    resp.Body.Close()

    srv.Shutdown()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestServerMountsBridgeEndpoints -v -timeout 30s`

- [ ] **Step 3: Add bridge to Server struct and lifecycle**

In `server/server.go`:

Add to Server struct:
```go
bridge *bridge.Bridge
```

In `startComponents()`, after orchestrator and trigger service start:
```go
js, _ := s.nc.JetStream()
s.bridge = bridge.NewBridge(s.nc, js)
printStep(os.Stderr, "http bridge ready")
```

In `startHTTP()`, mount bridge handler on the mux:
```go
if s.bridge != nil {
    mux.Handle("/v1/", s.bridge.Handler())
}
```

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Run all server tests**

Run: `go test ./server/ -v -timeout 30s`

- [ ] **Step 6: Commit**

```bash
git add server/server.go server/server_test.go
git commit -m "feat: mount HTTP bridge on dagnats serve HTTP mux"
```

---

## Task 2: Signal Support in Bridge Resolve

**Files:**
- Modify: `bridge/resolve.go` (add wait_signal and send_signal actions)
- Modify: `bridge/bridge.go` (add signalKV field)
- Test: `bridge/resolve_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestResolveSendSignal(t *testing.T) {
    // Setup: real NATS, SetupAll with signals KV bucket
    // Create bridge, poll a task
    // POST /v1/tasks/{id}/resolve with action=send_signal
    // Verify signal was written to signals KV bucket
}

func TestResolveWaitSignalImmediate(t *testing.T) {
    // Setup: write signal to KV first
    // Poll a task, POST resolve with action=wait_signal
    // Verify signal data returned immediately in response
}

func TestResolveWaitSignalTimeout(t *testing.T) {
    // Poll a task, POST resolve with action=wait_signal, timeout_ms=200
    // Don't send signal
    // Verify HTTP 408 after timeout
}
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Add signalKV to Bridge**

In `bridge/bridge.go`, add `signalKV nats.KeyValue` field. In `NewBridge`, try to bind:
```go
b.signalKV, _ = js.KeyValue("signals") // optional — nil if bucket missing
```

- [ ] **Step 4: Add send_signal action to dispatchAction**

In `bridge/resolve.go`:
```go
case "send_signal":
    return b.resolveSendSignal(taskID, msg, req, w)
case "wait_signal":
    return b.resolveWaitSignal(taskID, msg, req, w, r)
```

`resolveSendSignal`:
1. Extract `run_id`, `name`, `data` from request
2. Write to `signals.{runID}.{name}` KV bucket
3. Return 200 (message stays in-flight — InProgress)

`resolveWaitSignal`:
1. Extract `name`, `timeout_ms` from request
2. Get runID from taskID (split)
3. Check if signal already in KV — if so, return data immediately
4. Start KV watch on `signals.{runID}.{name}` with timeout
5. Call `msg.InProgress()` periodically (every 15s) during watch
6. On signal: return data as JSON
7. On timeout: return HTTP 408

- [ ] **Step 5: Run tests to verify they pass**

- [ ] **Step 6: Run all bridge tests**

Run: `go test ./bridge/ -v -timeout 60s`

- [ ] **Step 7: Commit**

```bash
git add bridge/bridge.go bridge/resolve.go bridge/resolve_test.go
git commit -m "feat: add wait_signal and send_signal actions to bridge resolve"
```

---

## Task 3: Bridge Observability

**Files:**
- Modify: `bridge/bridge.go` (add tel field)
- Modify: `bridge/connect.go` (add spans + logging)
- Modify: `bridge/poll.go` (add spans + metrics)
- Modify: `bridge/resolve.go` (add spans + logging)
- Test: `bridge/bridge_test.go`

- [ ] **Step 1: Add Telemetry to Bridge struct**

Read `api/service.go` for the exact pattern. The API service has:
- `tel *observe.Telemetry` field
- Pre-allocated metric instruments in constructor
- Instrumented wrapper methods that start spans and record metrics

In `bridge/bridge.go`:
```go
type Bridge struct {
    nc       *nats.Conn
    js       nats.JetStreamContext
    ackMap   *AckMap
    tel      *observe.Telemetry
    // Pre-allocated metrics
    requestCount  observe.Counter
    pollDuration  observe.Histogram
    ackMapSize    observe.Gauge
}
```

Update `NewBridge` to accept `tel *observe.Telemetry` (nil defaults to noop). Pre-allocate metrics.

**IMPORTANT:** This changes the `NewBridge` signature. Update all callers:
- `server/server.go` (pass `s.tel`)
- `bridge/*_test.go` (pass `nil` for tests — noop telemetry)

- [ ] **Step 2: Add spans to each endpoint**

In `handleConnect`:
```go
_, span := b.tel.Tracer.Start(r.Context(), "bridge.connect")
defer span.End()
b.tel.Logger.Info("worker connected", observe.String("worker_id", req.WorkerID))
```

In `handlePoll`:
```go
ctx, span := b.tel.Tracer.Start(r.Context(), "bridge.poll")
defer span.End()
b.pollDuration.Observe(elapsed)
b.requestCount.Inc()
```

In `handleResolve`:
```go
_, span := b.tel.Tracer.Start(r.Context(), "bridge.resolve")
defer span.End()
b.tel.Logger.Info("task resolved", observe.String("action", req.Action))
b.requestCount.Inc()
```

- [ ] **Step 3: Update all test files to pass nil telemetry**

Grep for `NewBridge(` in test files and add nil tel parameter.

- [ ] **Step 4: Run all tests**

Run: `go test ./bridge/ ./server/ -v -timeout 60s`

- [ ] **Step 5: Commit**

```bash
git add bridge/ server/server.go
git commit -m "feat: add observability to HTTP bridge — spans, metrics, structured logging"
```

---

## Task 4: Go HTTP Reference Client

**Files:**
- Create: `sdk/httpclient/client.go`
- Create: `sdk/httpclient/client_test.go`

- [ ] **Step 1: Create client package**

```go
// sdk/httpclient/client.go
package httpclient

// Client implements the DagNats worker protocol over HTTP.
// It is the reference implementation for other language SDKs.
type Client struct {
    baseURL    string
    token      string
    http       *http.Client
    workerID   string
    taskTypes  []string
    cancelConn context.CancelFunc // stops SSE heartbeat
}

func New(baseURL, token string) *Client {
    if baseURL == "" {
        panic("httpclient.New: baseURL must not be empty")
    }
    return &Client{
        baseURL: strings.TrimRight(baseURL, "/"),
        token:   token,
        http:    &http.Client{Timeout: 0}, // no global timeout — per-request
    }
}

func (c *Client) Connect(ctx context.Context, reg WorkerRegistration) error
func (c *Client) Disconnect() error
func (c *Client) Poll(ctx context.Context, taskTypes []string, maxTasks int, timeout time.Duration) ([]TaskPayload, error)
func (c *Client) Resolve(ctx context.Context, taskID string, resolution TaskResolution) error

// Convenience methods wrapping Resolve
func (c *Client) Complete(ctx context.Context, taskID string, output json.RawMessage) error
func (c *Client) Fail(ctx context.Context, taskID string, errMsg string) error
func (c *Client) Pause(ctx context.Context, taskID string, name string, duration time.Duration, checkpoint json.RawMessage) error
func (c *Client) Checkpoint(ctx context.Context, taskID string, data json.RawMessage) error
func (c *Client) WaitSignal(ctx context.Context, taskID string, name string, timeout time.Duration) (json.RawMessage, error)
func (c *Client) SendSignal(ctx context.Context, taskID string, runID string, name string, data json.RawMessage) error
```

Use `protocol.TaskPayload`, `protocol.TaskResolution`, `worker.WorkerRegistration` types directly.

`Connect` implementation:
1. POST to `/v1/workers/connect` with registration JSON
2. Start background goroutine reading SSE stream (heartbeat keepalive)
3. Store cancel func to stop on Disconnect

`Poll` implementation:
1. POST to `/v1/tasks/poll` with task_types, max_tasks, timeout_ms
2. Return decoded TaskPayload slice

`Resolve` implementation:
1. POST to `/v1/tasks/{taskID}/resolve` with TaskResolution JSON
2. Check HTTP status

Convenience methods construct the appropriate `TaskResolution` and call `Resolve`.

- [ ] **Step 2: Write E2E test**

```go
// sdk/httpclient/client_test.go
func TestClientE2EWorkflowCompletion(t *testing.T) {
    // 1. Start full DagNats server (embedded NATS + orchestrator + bridge)
    // 2. Register workflow with one task
    // 3. Create HTTP client pointing at server
    // 4. Connect as worker
    // 5. Start workflow run via API
    // 6. Poll for tasks
    // 7. Complete the task
    // 8. Verify workflow completed
    // 9. Disconnect
}
```

This is the proof that the wire protocol works end-to-end.

- [ ] **Step 3: Run tests**

Run: `go test ./sdk/httpclient/ -v -timeout 30s`

- [ ] **Step 4: Commit**

```bash
git add sdk/httpclient/
git commit -m "feat: add Go HTTP reference client for polyglot worker SDK validation"
```

---

## Final Validation

- [ ] **Step 1: Full test suite**

Run: `go test ./... -timeout 120s -count=1`

- [ ] **Step 2: Linters**

Run: `go vet ./...`

- [ ] **Step 3: Formatting**

Run: `gofmt -l .`

- [ ] **Step 4: Push**

```bash
git push
```

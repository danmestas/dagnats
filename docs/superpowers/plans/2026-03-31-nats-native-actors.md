# NATS-Native Actor Primitives — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a lightweight actor runtime to DagNats that provides hierarchical supervision, per-actor mailboxes, and lifecycle management — using Go goroutines and channels, no external framework.

**Architecture:** New `actor/` package with four files: types (`actor.go`), runtime (`runtime.go`), supervision strategies (`supervision.go`), and a restart tracker (`restarts.go`). Actors are goroutines with buffered channel mailboxes. The Runtime spawns actors, routes messages, and applies supervision strategies when actors fail. All pure Go — NATS integration deferred to Phase 2 (per-workflow actors).

**Tech Stack:** Go, stdlib only (no NATS imports in `actor/` package — pure like `dag/`)

**Spec:** `docs/superpowers/specs/2026-03-31-actor-model-evaluation.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `actor/actor.go` | Actor interface, Address, Message, Context, Directive types |
| `actor/actor_test.go` | Tests for types (Address.String, Directive.String) |
| `actor/supervision.go` | SupervisionStrategy interface, OneForOne, AllForOne |
| `actor/supervision_test.go` | Strategy decision tests |
| `actor/restarts.go` | RestartTracker — bounded restart counting with time window |
| `actor/restarts_test.go` | Tracker window and limit tests |
| `actor/runtime.go` | Runtime — spawn, stop, send, supervision loop |
| `actor/runtime_test.go` | Integration tests: lifecycle, supervision, messaging |

---

## Chunk 1: Types and Supervision

### Task 1: Actor types — Address, Message, Actor interface

**Files:**
- Create: `actor/actor.go`
- Test: `actor/actor_test.go`

- [ ] **Step 1: Write failing test for Address.String()**

Create `actor/actor_test.go`:

```go
package actor

// Methodology: unit tests for actor primitive types. No NATS dependency.
// Each test verifies both positive behavior and boundary/negative cases.

import "testing"

func TestAddressString(t *testing.T) {
	addr := Address{Type: "workflow", ID: "run-1"}

	// Positive: formatted as type.id
	got := addr.String()
	want := "workflow.run-1"
	if got != want {
		t.Fatalf("Address.String() = %q, want %q", got, want)
	}

	// Positive: different type
	addr2 := Address{Type: "worker", ID: "w-5"}
	if got2 := addr2.String(); got2 != "worker.w-5" {
		t.Fatalf("Address.String() = %q, want %q", got2, "worker.w-5")
	}
}

func TestDirectiveString(t *testing.T) {
	// Positive: known directives
	if Restart.String() != "restart" {
		t.Fatalf("Restart.String() = %q", Restart.String())
	}
	if Stop.String() != "stop" {
		t.Fatalf("Stop.String() = %q", Stop.String())
	}
	if Escalate.String() != "escalate" {
		t.Fatalf("Escalate.String() = %q", Escalate.String())
	}
	if Resume.String() != "resume" {
		t.Fatalf("Resume.String() = %q", Resume.String())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -run TestAddress -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Implement actor types**

Create `actor/actor.go`:

```go
// actor/actor.go
// The actor package provides a lightweight actor runtime for DagNats.
// Actors are goroutines with channel mailboxes, supervised by parent actors.
// Pure Go — no NATS imports. NATS integration lives in engine/.
package actor

import "fmt"

// Address uniquely identifies an actor within the runtime.
type Address struct {
	Type string // e.g. "workflow", "worker", "tool"
	ID   string // e.g. run ID, worker ID
}

// String formats the address as "type.id" for logging and map keys.
func (a Address) String() string {
	return a.Type + "." + a.ID
}

// Message is the envelope delivered to an actor's mailbox.
type Message struct {
	From    Address
	Payload interface{}
}

// Directive tells a supervisor how to handle a failed child.
type Directive int

const (
	Restart  Directive = iota // Restart the failed actor
	Stop                      // Stop the actor permanently
	Escalate                  // Escalate failure to parent
	Resume                    // Ignore the error, continue
)

var directiveStrings = [...]string{
	"restart", "stop", "escalate", "resume",
}

// String returns the lowercase name of the directive.
func (d Directive) String() string {
	if int(d) < len(directiveStrings) {
		return directiveStrings[d]
	}
	panic(fmt.Sprintf("unknown Directive %d", d))
}

// Actor is the interface all actors implement. Receive processes one
// message at a time — the runtime guarantees sequential delivery.
type Actor interface {
	// Receive processes a single message. Returning an error
	// triggers the supervision strategy.
	Receive(ctx *Context, msg Message) error
}

// Lifecycle extends Actor with optional startup/shutdown hooks.
// Actors that don't need hooks can implement Actor alone.
type Lifecycle interface {
	// PreStart runs before the actor begins receiving messages.
	// Errors here trigger supervision (the actor never starts).
	PreStart(ctx *Context) error

	// PostStop runs after the actor stops receiving messages.
	// Used for cleanup. Errors are logged but not supervised.
	PostStop(ctx *Context)
}

// Context provides services to a running actor.
type Context struct {
	self    Address
	runtime *Runtime
}

// Self returns this actor's address.
func (c *Context) Self() Address { return c.self }

// Send delivers a message to another actor's mailbox. Returns an
// error if the target actor is not found or its mailbox is full.
func (c *Context) Send(to Address, payload interface{}) error {
	return c.runtime.Send(to, Message{
		From:    c.self,
		Payload: payload,
	})
}

// Spawn creates a child actor supervised by this actor.
func (c *Context) Spawn(
	addr Address, actor Actor, opts ...SpawnOption,
) error {
	so := spawnDefaults()
	for _, opt := range opts {
		opt(&so)
	}
	return c.runtime.spawn(addr, actor, c.self, so)
}

// SpawnOption configures actor spawning.
type SpawnOption func(*spawnOptions)

type spawnOptions struct {
	mailboxSize int
	strategy    SupervisionStrategy
}

func spawnDefaults() spawnOptions {
	return spawnOptions{
		mailboxSize: 64,
		strategy:    nil, // no children = no strategy needed
	}
}

// WithMailboxSize sets the buffered channel capacity.
func WithMailboxSize(size int) SpawnOption {
	return func(o *spawnOptions) {
		if size < 1 {
			panic("actor: mailbox size must be >= 1")
		}
		o.mailboxSize = size
	}
}

// WithSupervision sets the strategy for this actor's children.
func WithSupervision(s SupervisionStrategy) SpawnOption {
	return func(o *spawnOptions) {
		if s == nil {
			panic("actor: supervision strategy must not be nil")
		}
		o.strategy = s
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add actor/actor.go actor/actor_test.go
git commit -m "feat(actor): add Actor interface, Address, Message, Directive, Context types"
```

---

### Task 2: Supervision strategies — OneForOne, AllForOne

**Files:**
- Create: `actor/supervision.go`
- Test: `actor/supervision_test.go`

- [ ] **Step 1: Write failing tests for supervision strategies**

Create `actor/supervision_test.go`:

```go
package actor

// Methodology: test supervision strategy decisions in isolation.
// Each strategy is a pure function from error → Directive.

import (
	"errors"
	"testing"
)

func TestOneForOneDefaultsToRestart(t *testing.T) {
	s := &OneForOne{}

	// Positive: default decision is Restart
	got := s.Decide(errors.New("boom"))
	if got != Restart {
		t.Fatalf("Decide = %v, want Restart", got)
	}

	// Positive: nil error still returns Restart
	got2 := s.Decide(nil)
	if got2 != Restart {
		t.Fatalf("Decide(nil) = %v, want Restart", got2)
	}
}

func TestOneForOneCustomDecider(t *testing.T) {
	permanent := errors.New("permanent")
	s := &OneForOne{
		Decider: func(err error) Directive {
			if errors.Is(err, permanent) {
				return Stop
			}
			return Restart
		},
	}

	// Positive: permanent error → Stop
	if got := s.Decide(permanent); got != Stop {
		t.Fatalf("Decide(permanent) = %v, want Stop", got)
	}

	// Positive: transient error → Restart
	if got := s.Decide(errors.New("transient")); got != Restart {
		t.Fatalf("Decide(transient) = %v, want Restart", got)
	}
}

func TestAllForOneDefaultsToRestart(t *testing.T) {
	s := &AllForOne{}

	// Positive: default decision
	if got := s.Decide(errors.New("boom")); got != Restart {
		t.Fatalf("Decide = %v, want Restart", got)
	}

	// Positive: RestartScope is AllChildren
	if s.RestartScope() != RestartAll {
		t.Fatalf("RestartScope = %v, want RestartAll", s.RestartScope())
	}
}

func TestOneForOneRestartScope(t *testing.T) {
	s := &OneForOne{}
	// Positive: scope is only the failed actor
	if s.RestartScope() != RestartOne {
		t.Fatalf("RestartScope = %v, want RestartOne", s.RestartScope())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -run TestOneForOne -v`
Expected: FAIL — `OneForOne` undefined

- [ ] **Step 3: Implement supervision strategies**

Create `actor/supervision.go`:

```go
package actor

// SupervisionStrategy decides how to handle a failed child actor.
// The runtime calls Decide when an actor's Receive returns an error.
type SupervisionStrategy interface {
	// Decide returns the directive for handling the failure.
	Decide(err error) Directive

	// RestartScope controls whether a failure restarts one child
	// or all siblings under the same supervisor.
	RestartScope() RestartScope
}

// RestartScope controls which actors restart on a failure.
type RestartScope int

const (
	RestartOne RestartScope = iota // Only the failed actor
	RestartAll                      // All children of the supervisor
)

// OneForOne restarts only the failed child. The default strategy.
type OneForOne struct {
	// Decider maps errors to directives. If nil, defaults to Restart.
	Decider func(error) Directive
}

// Decide returns the directive for the given error.
func (s *OneForOne) Decide(err error) Directive {
	if s.Decider != nil {
		return s.Decider(err)
	}
	return Restart
}

// RestartScope returns RestartOne — only the failed child restarts.
func (s *OneForOne) RestartScope() RestartScope {
	return RestartOne
}

// AllForOne restarts all siblings when any child fails.
// Useful when children have interdependent state.
type AllForOne struct {
	// Decider maps errors to directives. If nil, defaults to Restart.
	Decider func(error) Directive
}

// Decide returns the directive for the given error.
func (s *AllForOne) Decide(err error) Directive {
	if s.Decider != nil {
		return s.Decider(err)
	}
	return Restart
}

// RestartScope returns RestartAll — all siblings restart.
func (s *AllForOne) RestartScope() RestartScope {
	return RestartAll
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -run "TestOneForOne|TestAllForOne" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add actor/supervision.go actor/supervision_test.go
git commit -m "feat(actor): add OneForOne and AllForOne supervision strategies"
```

---

### Task 3: Restart tracker — bounded restart counting

**Files:**
- Create: `actor/restarts.go`
- Test: `actor/restarts_test.go`

- [ ] **Step 1: Write failing tests for RestartTracker**

Create `actor/restarts_test.go`:

```go
package actor

// Methodology: test restart tracker in isolation. Verifies
// that restart limits within a time window are enforced.

import (
	"testing"
	"time"
)

func TestRestartTrackerAllowsWithinLimit(t *testing.T) {
	tr := NewRestartTracker(3, 1*time.Minute)

	// Positive: first three restarts allowed
	for i := 0; i < 3; i++ {
		if !tr.Allow() {
			t.Fatalf("restart %d should be allowed", i+1)
		}
	}

	// Negative: fourth exceeds limit
	if tr.Allow() {
		t.Fatalf("restart 4 should be denied (limit 3)")
	}
}

func TestRestartTrackerResetsAfterWindow(t *testing.T) {
	// Use a tiny window so we can test expiry
	tr := NewRestartTracker(2, 50*time.Millisecond)

	// Positive: two allowed
	if !tr.Allow() {
		t.Fatalf("restart 1 should be allowed")
	}
	if !tr.Allow() {
		t.Fatalf("restart 2 should be allowed")
	}

	// Negative: third denied
	if tr.Allow() {
		t.Fatalf("restart 3 should be denied")
	}

	// Wait for window to expire
	time.Sleep(60 * time.Millisecond)

	// Positive: allowed again after window expires
	if !tr.Allow() {
		t.Fatalf("restart after window should be allowed")
	}
}

func TestNewRestartTrackerPanicsOnBadArgs(t *testing.T) {
	// Negative: zero limit panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for zero limit")
		}
	}()
	NewRestartTracker(0, time.Minute)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -run TestRestartTracker -v`
Expected: FAIL — `NewRestartTracker` undefined

- [ ] **Step 3: Implement RestartTracker**

Create `actor/restarts.go`:

```go
package actor

import "time"

// RestartTracker enforces a bounded number of restarts within a
// sliding time window. If the limit is exceeded, the tracker
// signals that the actor should be stopped or escalated.
type RestartTracker struct {
	limit    int
	window   time.Duration
	restarts []time.Time
}

// NewRestartTracker creates a tracker allowing limit restarts
// within the given window. Panics if limit < 1.
func NewRestartTracker(limit int, window time.Duration) *RestartTracker {
	if limit < 1 {
		panic("actor: restart limit must be >= 1")
	}
	return &RestartTracker{
		limit:    limit,
		window:   window,
		restarts: make([]time.Time, 0, limit),
	}
}

// Allow returns true if a restart is permitted. It prunes expired
// entries, then checks the count against the limit. If allowed,
// it records the restart time.
func (tr *RestartTracker) Allow() bool {
	now := time.Now()
	cutoff := now.Add(-tr.window)

	// Prune expired restarts (iterative, no recursion)
	valid := 0
	for _, t := range tr.restarts {
		if t.After(cutoff) {
			tr.restarts[valid] = t
			valid++
		}
	}
	tr.restarts = tr.restarts[:valid]

	if len(tr.restarts) >= tr.limit {
		return false
	}

	tr.restarts = append(tr.restarts, now)
	return true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -run TestRestartTracker -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add actor/restarts.go actor/restarts_test.go
git commit -m "feat(actor): add RestartTracker with bounded time-window restart limits"
```

---

## Chunk 2: Runtime

### Task 4: Runtime — spawn, stop, send, supervision loop

**Files:**
- Create: `actor/runtime.go`
- Test: `actor/runtime_test.go`

- [ ] **Step 1: Write failing test for basic actor lifecycle**

Create `actor/runtime_test.go`:

```go
package actor

// Methodology: integration tests for the actor runtime. Tests verify
// actor lifecycle (spawn, receive, stop), supervision (restart on
// failure), and message delivery. All tests use bounded timeouts.

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// echoActor sends back any message it receives to the sender.
type echoActor struct {
	received atomic.Int32
}

func (a *echoActor) Receive(ctx *Context, msg Message) error {
	a.received.Add(1)
	if msg.From != (Address{}) {
		ctx.Send(msg.From, msg.Payload)
	}
	return nil
}

func TestRuntimeSpawnAndSend(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	echo := &echoActor{}
	addr := Address{Type: "test", ID: "echo-1"}

	err := rt.Spawn(addr, echo)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Positive: send a message
	err = rt.Send(addr, Message{Payload: "hello"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Wait for delivery with bounded timeout
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if echo.received.Load() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Positive: actor received the message
	if echo.received.Load() < 1 {
		t.Fatalf("expected at least 1 message, got %d",
			echo.received.Load())
	}

	// Negative: sending to unknown address fails
	err = rt.Send(Address{Type: "x", ID: "y"}, Message{})
	if err == nil {
		t.Fatalf("expected error sending to unknown address")
	}
}

func TestRuntimeStop(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	echo := &echoActor{}
	addr := Address{Type: "test", ID: "stop-1"}

	rt.Spawn(addr, echo)

	// Positive: stop succeeds
	err := rt.Stop(addr)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Negative: sending after stop fails
	err = rt.Send(addr, Message{Payload: "late"})
	if err == nil {
		t.Fatalf("expected error sending to stopped actor")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -run "TestRuntimeSpawnAndSend|TestRuntimeStop" -v -timeout 10s`
Expected: FAIL — `NewRuntime` undefined

- [ ] **Step 3: Implement Runtime core (spawn, send, stop)**

Create `actor/runtime.go`:

```go
package actor

import (
	"fmt"
	"sync"
)

// ErrActorNotFound is returned when sending to an unknown address.
var ErrActorNotFound = fmt.Errorf("actor: not found")

// ErrMailboxFull is returned when an actor's mailbox is at capacity.
var ErrMailboxFull = fmt.Errorf("actor: mailbox full")

// ErrAlreadyExists is returned when spawning at an occupied address.
var ErrAlreadyExists = fmt.Errorf("actor: already exists")

// Runtime manages actor lifecycle, message delivery, and supervision.
// All methods are safe for concurrent use.
type Runtime struct {
	mu     sync.RWMutex
	actors map[string]*actorCell
}

// actorCell is the internal bookkeeping for a running actor.
type actorCell struct {
	addr     Address
	actor    Actor
	mailbox  chan Message
	parent   Address        // zero value = root actor
	children []Address
	strategy SupervisionStrategy
	restarts *RestartTracker
	done     chan struct{}
}

// NewRuntime creates an empty actor runtime.
func NewRuntime() *Runtime {
	return &Runtime{
		actors: make(map[string]*actorCell),
	}
}

// Spawn starts a new root-level actor. Use Context.Spawn for
// supervised child actors. Returns ErrAlreadyExists if the address
// is taken.
func (r *Runtime) Spawn(
	addr Address, actor Actor, opts ...SpawnOption,
) error {
	return r.spawn(addr, actor, Address{}, spawnWithOpts(opts))
}

func spawnWithOpts(opts []SpawnOption) spawnOptions {
	so := spawnDefaults()
	for _, o := range opts {
		o(&so)
	}
	return so
}

// spawn is the internal spawn used by both Runtime.Spawn and
// Context.Spawn. parent is zero-value for root actors.
func (r *Runtime) spawn(
	addr Address,
	a Actor,
	parent Address,
	opts spawnOptions,
) error {
	if addr.Type == "" || addr.ID == "" {
		panic("actor: address Type and ID must not be empty")
	}
	if a == nil {
		panic("actor: actor must not be nil")
	}

	r.mu.Lock()
	key := addr.String()
	if _, exists := r.actors[key]; exists {
		r.mu.Unlock()
		return ErrAlreadyExists
	}

	cell := &actorCell{
		addr:     addr,
		actor:    a,
		mailbox:  make(chan Message, opts.mailboxSize),
		parent:   parent,
		strategy: opts.strategy,
		restarts: NewRestartTracker(5, 1*60e9),
		done:     make(chan struct{}),
	}
	r.actors[key] = cell

	// Register as child of parent
	if parent != (Address{}) {
		if pc, ok := r.actors[parent.String()]; ok {
			pc.children = append(pc.children, addr)
		}
	}
	r.mu.Unlock()

	// Run actor loop in goroutine
	go r.runActor(cell)
	return nil
}

// runActor is the main actor goroutine. It calls PreStart (if
// Lifecycle), then processes messages until the done channel closes.
func (r *Runtime) runActor(cell *actorCell) {
	ctx := &Context{self: cell.addr, runtime: r}

	// Call PreStart if actor implements Lifecycle
	if lc, ok := cell.actor.(Lifecycle); ok {
		if err := lc.PreStart(ctx); err != nil {
			r.handleFailure(cell, err)
			return
		}
	}

	// Message loop — sequential delivery, one at a time
	for {
		select {
		case msg := <-cell.mailbox:
			if err := cell.actor.Receive(ctx, msg); err != nil {
				r.handleFailure(cell, err)
				return
			}
		case <-cell.done:
			// Graceful shutdown
			if lc, ok := cell.actor.(Lifecycle); ok {
				lc.PostStop(ctx)
			}
			return
		}
	}
}

// handleFailure applies the parent's supervision strategy.
func (r *Runtime) handleFailure(cell *actorCell, err error) {
	r.mu.RLock()
	parent, hasParent := r.actors[cell.parent.String()]
	r.mu.RUnlock()

	directive := Stop // Default: stop if no supervisor
	if hasParent && parent.strategy != nil {
		directive = parent.strategy.Decide(err)
	}

	switch directive {
	case Restart:
		r.restartActor(cell)
	case Stop:
		r.stopActor(cell)
	case Escalate:
		r.stopActor(cell)
		if hasParent {
			r.handleFailure(parent, err)
		}
	case Resume:
		// Re-enter message loop
		go r.runActor(cell)
	}
}

// restartActor stops and relaunches the actor if within limits.
func (r *Runtime) restartActor(cell *actorCell) {
	if !cell.restarts.Allow() {
		// Exhausted restart budget — stop permanently
		r.stopActor(cell)
		return
	}

	// Call PostStop on the old instance
	if lc, ok := cell.actor.(Lifecycle); ok {
		ctx := &Context{self: cell.addr, runtime: r}
		lc.PostStop(ctx)
	}

	// Re-enter message loop with same actor instance
	cell.done = make(chan struct{})
	go r.runActor(cell)
}

// stopActor terminates an actor and removes it from the runtime.
func (r *Runtime) stopActor(cell *actorCell) {
	// Signal done (safe to close multiple times via select)
	select {
	case <-cell.done:
		// Already closed
	default:
		close(cell.done)
	}

	r.mu.Lock()
	delete(r.actors, cell.addr.String())

	// Remove from parent's children list
	if cell.parent != (Address{}) {
		if pc, ok := r.actors[cell.parent.String()]; ok {
			for i, child := range pc.children {
				if child == cell.addr {
					pc.children = append(
						pc.children[:i],
						pc.children[i+1:]...,
					)
					break
				}
			}
		}
	}
	r.mu.Unlock()

	// Stop children recursively (iterative)
	r.mu.RLock()
	children := make([]Address, len(cell.children))
	copy(children, cell.children)
	r.mu.RUnlock()

	for _, child := range children {
		r.Stop(child)
	}
}

// Send delivers a message to an actor's mailbox. Returns
// ErrActorNotFound if the address is unknown, ErrMailboxFull
// if the channel is at capacity.
func (r *Runtime) Send(
	to Address, msg Message,
) error {
	r.mu.RLock()
	cell, ok := r.actors[to.String()]
	r.mu.RUnlock()
	if !ok {
		return ErrActorNotFound
	}

	select {
	case cell.mailbox <- msg:
		return nil
	default:
		return ErrMailboxFull
	}
}

// Stop gracefully terminates an actor and its children.
func (r *Runtime) Stop(addr Address) error {
	r.mu.RLock()
	cell, ok := r.actors[addr.String()]
	r.mu.RUnlock()
	if !ok {
		return ErrActorNotFound
	}
	r.stopActor(cell)
	return nil
}

// StopAll terminates all actors. Used in defer for cleanup.
func (r *Runtime) StopAll() {
	r.mu.RLock()
	addrs := make([]Address, 0, len(r.actors))
	for _, cell := range r.actors {
		addrs = append(addrs, cell.addr)
	}
	r.mu.RUnlock()

	for _, addr := range addrs {
		r.Stop(addr)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -run "TestRuntimeSpawnAndSend|TestRuntimeStop" -v -timeout 10s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add actor/runtime.go actor/runtime_test.go
git commit -m "feat(actor): add Runtime with spawn, send, stop, and supervision"
```

---

### Task 5: Supervision integration tests

**Files:**
- Modify: `actor/runtime_test.go`

- [ ] **Step 1: Write failing test for actor restart on error**

Add to `actor/runtime_test.go`:

```go
// failOnceActor fails on the first message, succeeds after.
type failOnceActor struct {
	calls atomic.Int32
}

func (a *failOnceActor) Receive(ctx *Context, msg Message) error {
	n := a.calls.Add(1)
	if n == 1 {
		return errors.New("transient failure")
	}
	return nil
}

func TestRuntimeSupervisedRestart(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	// Supervisor with OneForOne strategy
	supervisor := &echoActor{}
	supAddr := Address{Type: "sup", ID: "s1"}
	err := rt.Spawn(supAddr, supervisor,
		WithSupervision(&OneForOne{}),
	)
	if err != nil {
		t.Fatalf("Spawn supervisor: %v", err)
	}

	// Supervised child that fails once
	child := &failOnceActor{}
	childAddr := Address{Type: "child", ID: "c1"}

	supCtx := &Context{self: supAddr, runtime: rt}
	err = supCtx.Spawn(childAddr, child)
	if err != nil {
		t.Fatalf("Spawn child: %v", err)
	}

	// Send message that triggers failure
	rt.Send(childAddr, Message{Payload: "trigger-fail"})

	// Wait for restart + redelivery window
	time.Sleep(100 * time.Millisecond)

	// Send second message (should succeed after restart)
	rt.Send(childAddr, Message{Payload: "after-restart"})

	// Wait for processing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if child.calls.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: actor was restarted and processed second message
	if child.calls.Load() < 2 {
		t.Fatalf("expected >= 2 calls, got %d", child.calls.Load())
	}
}

// lifecycleActor tracks PreStart and PostStop calls.
type lifecycleActor struct {
	started  atomic.Int32
	stopped  atomic.Int32
	received atomic.Int32
}

func (a *lifecycleActor) Receive(ctx *Context, msg Message) error {
	a.received.Add(1)
	return nil
}

func (a *lifecycleActor) PreStart(ctx *Context) error {
	a.started.Add(1)
	return nil
}

func (a *lifecycleActor) PostStop(ctx *Context) {
	a.stopped.Add(1)
}

func TestRuntimeLifecycleHooks(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	lc := &lifecycleActor{}
	addr := Address{Type: "test", ID: "lc-1"}

	rt.Spawn(addr, lc)

	// Wait for PreStart
	time.Sleep(50 * time.Millisecond)

	// Positive: PreStart called
	if lc.started.Load() != 1 {
		t.Fatalf("PreStart calls = %d, want 1", lc.started.Load())
	}

	// Stop the actor
	rt.Stop(addr)
	time.Sleep(50 * time.Millisecond)

	// Positive: PostStop called
	if lc.stopped.Load() < 1 {
		t.Fatalf("PostStop calls = %d, want >= 1", lc.stopped.Load())
	}
}

func TestRuntimeSpawnDuplicateReturnsError(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	addr := Address{Type: "test", ID: "dup-1"}
	rt.Spawn(addr, &echoActor{})

	// Negative: duplicate spawn fails
	err := rt.Spawn(addr, &echoActor{})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify supervision works**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -v -timeout 15s`
Expected: ALL PASS

- [ ] **Step 3: Fix any issues found during testing**

If supervision restart doesn't work correctly (e.g. the child actor is removed from the map before restart), fix the race in `handleFailure` / `restartActor`. The message that caused the failure is lost (expected behavior — NATS redelivery handles this at the workflow level).

- [ ] **Step 4: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add actor/runtime_test.go
git commit -m "test(actor): supervision restart, lifecycle hooks, duplicate spawn"
```

---

### Task 6: Message passing between actors

**Files:**
- Modify: `actor/runtime_test.go`

- [ ] **Step 1: Write test for actor-to-actor messaging**

Add to `actor/runtime_test.go`:

```go
// collectorActor stores all received payloads.
type collectorActor struct {
	mu       sync.Mutex
	payloads []interface{}
}

func (a *collectorActor) Receive(ctx *Context, msg Message) error {
	a.mu.Lock()
	a.payloads = append(a.payloads, msg.Payload)
	a.mu.Unlock()
	return nil
}

func (a *collectorActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.payloads)
}

func TestRuntimeActorToActorMessaging(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	collector := &collectorActor{}
	collAddr := Address{Type: "test", ID: "collector"}
	rt.Spawn(collAddr, collector)

	// forwarder sends to collector on receive
	forwarder := &forwarderActor{target: collAddr}
	fwdAddr := Address{Type: "test", ID: "forwarder"}
	rt.Spawn(fwdAddr, forwarder)

	// Send to forwarder
	rt.Send(fwdAddr, Message{Payload: "ping"})

	// Wait for forwarding
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if collector.count() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Positive: collector received forwarded message
	if collector.count() < 1 {
		t.Fatalf("collector got %d messages, want >= 1",
			collector.count())
	}
}

type forwarderActor struct {
	target Address
}

func (a *forwarderActor) Receive(ctx *Context, msg Message) error {
	return ctx.Send(a.target, msg.Payload)
}

func TestRuntimeMailboxFull(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	// Actor with tiny mailbox
	slow := &echoActor{}
	addr := Address{Type: "test", ID: "slow"}
	rt.Spawn(addr, slow, WithMailboxSize(1))

	// Fill the mailbox (actor might process some, but eventually full)
	var fullErr error
	for i := 0; i < 100; i++ {
		err := rt.Send(addr, Message{Payload: i})
		if err != nil {
			fullErr = err
			break
		}
	}

	// Positive: eventually got mailbox full error
	if fullErr == nil {
		t.Fatalf("expected ErrMailboxFull with tiny mailbox")
	}
	if !errors.Is(fullErr, ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull, got %v", fullErr)
	}
}
```

- [ ] **Step 2: Run all actor tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -v -timeout 15s`
Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add actor/runtime_test.go
git commit -m "test(actor): actor-to-actor messaging and mailbox backpressure"
```

---

### Task 7: Run full test suite — verify no regressions

**Files:** None (verification only)

- [ ] **Step 1: Run all actor tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./actor/ -v -count=1 -timeout 30s`
Expected: ALL PASS

- [ ] **Step 2: Run entire project test suite**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS — actor package is additive, no existing code modified

- [ ] **Step 3: Verify code quality**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go vet ./actor/`
Expected: No issues

- [ ] **Step 4: Check line counts (TigerStyle: functions ≤ 70 lines)**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && wc -l actor/*.go`
Expected: ~500 LOC total (excluding tests), no file > 200 lines

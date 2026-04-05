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

// collectorActor stores all received payloads.
type collectorActor struct {
	mu       sync.Mutex
	payloads []any
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

func TestRuntimeContextSelf(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	// selfActor records its own address via Context.Self()
	sa := &selfActor{}
	addr := Address{Type: "test", ID: "self-1"}
	rt.Spawn(addr, sa)

	rt.Send(addr, Message{Payload: "check"})

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if sa.selfAddr.Load() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Positive: Self() returns the spawned address
	got := sa.selfAddr.Load().(Address)
	if got != addr {
		t.Fatalf("Self() = %v, want %v", got, addr)
	}

	// Negative: Self() is not some other address
	other := Address{Type: "other", ID: "x"}
	if got == other {
		t.Fatalf("Self() should not equal %v", other)
	}
}

// selfActor stores the address returned by Context.Self().
type selfActor struct {
	selfAddr atomic.Value
}

func (a *selfActor) Receive(ctx *Context, msg Message) error {
	a.selfAddr.Store(ctx.Self())
	return nil
}

func TestRuntimeContextSpawnChild(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	parent := &spawnerActor{}
	parentAddr := Address{Type: "test", ID: "parent"}
	rt.Spawn(parentAddr, parent,
		WithSupervision(&OneForOne{}),
	)

	// Tell parent to spawn a child
	childAddr := Address{Type: "test", ID: "child"}
	rt.Send(parentAddr, Message{Payload: childAddr})

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if parent.spawned.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Positive: child was spawned
	if parent.spawned.Load() < 1 {
		t.Fatalf("expected child to be spawned")
	}

	// Positive: child can receive messages
	err := rt.Send(childAddr, Message{Payload: "hi"})
	if err != nil {
		t.Fatalf("Send to child: %v", err)
	}
}

// spawnerActor spawns a child echo actor on receiving a message.
type spawnerActor struct {
	spawned atomic.Int32
}

func (a *spawnerActor) Receive(
	ctx *Context, msg Message,
) error {
	addr := msg.Payload.(Address)
	err := ctx.Spawn(addr, &echoActor{})
	if err != nil {
		return err
	}
	a.spawned.Add(1)
	return nil
}

func TestRuntimeStopAllRemovesAll(t *testing.T) {
	rt := NewRuntime()

	addr1 := Address{Type: "test", ID: "a1"}
	addr2 := Address{Type: "test", ID: "a2"}
	rt.Spawn(addr1, &echoActor{})
	rt.Spawn(addr2, &echoActor{})

	rt.StopAll()

	// Negative: both actors gone
	err1 := rt.Send(addr1, Message{Payload: "gone"})
	if !errors.Is(err1, ErrActorNotFound) {
		t.Fatalf("expected ErrActorNotFound for a1, got %v", err1)
	}
	err2 := rt.Send(addr2, Message{Payload: "gone"})
	if !errors.Is(err2, ErrActorNotFound) {
		t.Fatalf("expected ErrActorNotFound for a2, got %v", err2)
	}
}

func TestRuntimeEscalateDirective(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	// Parent that escalates all child errors (no grandparent
	// means escalation defaults to Stop)
	parentAddr := Address{Type: "parent", ID: "esc-p1"}
	rt.Spawn(parentAddr, &echoActor{},
		WithSupervision(&OneForOne{
			Decider: func(err error) Directive {
				return Escalate
			},
		}),
	)

	// Child that always fails
	child := &failAlwaysActor{}
	childAddr := Address{Type: "child", ID: "esc-c1"}
	pCtx := &Context{self: parentAddr, runtime: rt}
	pCtx.Spawn(childAddr, child)

	// Trigger failure in child
	rt.Send(childAddr, Message{Payload: "fail"})
	time.Sleep(200 * time.Millisecond)

	// Positive: child stopped after escalation
	err := rt.Send(childAddr, Message{Payload: "check"})
	if !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("expected child gone, got %v", err)
	}

	// Positive: parent also stopped (escalation to root = Stop)
	err = rt.Send(parentAddr, Message{Payload: "check"})
	if !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("expected parent gone, got %v", err)
	}
}

// failAlwaysActor always returns an error from Receive.
type failAlwaysActor struct{}

func (a *failAlwaysActor) Receive(
	ctx *Context, msg Message,
) error {
	return errors.New("permanent failure")
}

func TestRuntimeResumeDirective(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	// Supervisor that resumes on all errors
	supAddr := Address{Type: "sup", ID: "resume-sup"}
	rt.Spawn(supAddr, &echoActor{},
		WithSupervision(&OneForOne{
			Decider: func(err error) Directive {
				return Resume
			},
		}),
	)

	// Child that fails once then succeeds
	child := &failOnceActor{}
	childAddr := Address{Type: "child", ID: "resume-c1"}
	supCtx := &Context{self: supAddr, runtime: rt}
	supCtx.Spawn(childAddr, child)

	// First message triggers failure + resume
	rt.Send(childAddr, Message{Payload: "fail"})
	time.Sleep(100 * time.Millisecond)

	// Second message should be processed (actor resumed)
	rt.Send(childAddr, Message{Payload: "ok"})

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if child.calls.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: actor processed both messages
	if child.calls.Load() < 2 {
		t.Fatalf("expected >= 2 calls, got %d", child.calls.Load())
	}

	// Positive: actor is still alive
	err := rt.Send(childAddr, Message{Payload: "still-here"})
	if err != nil {
		t.Fatalf("expected actor alive after resume, got %v", err)
	}
}

func TestRuntimeRestartBudgetExhausted(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	supAddr := Address{Type: "sup", ID: "budget-sup"}
	rt.Spawn(supAddr, &echoActor{},
		WithSupervision(&OneForOne{}),
	)

	// Child that always fails — will exhaust restart budget
	child := &failAlwaysActor{}
	childAddr := Address{Type: "child", ID: "budget-c1"}
	supCtx := &Context{self: supAddr, runtime: rt}
	supCtx.Spawn(childAddr, child)

	// Send messages to trigger repeated failures
	for i := 0; i < 10; i++ {
		rt.Send(childAddr, Message{Payload: i})
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)

	// Positive: actor stopped after exhausting restart budget
	err := rt.Send(childAddr, Message{Payload: "check"})
	if !errors.Is(err, ErrActorNotFound) {
		t.Fatalf(
			"expected actor stopped after budget exhaust, got %v",
			err,
		)
	}
}

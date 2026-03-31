package actor

// Methodology: integration tests for the actor runtime. Tests verify
// actor lifecycle (spawn, receive, stop), supervision (restart on
// failure), and message delivery. All tests use bounded timeouts.

import (
	"errors"
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

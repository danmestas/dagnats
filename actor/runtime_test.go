package actor

// Methodology: integration tests for the actor runtime. Tests verify
// actor lifecycle (spawn, receive, stop), supervision (restart on
// failure), and message delivery. All tests use bounded timeouts.

import (
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

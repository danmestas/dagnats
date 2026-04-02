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

func TestDirectiveStringPanicsOnUnknown(t *testing.T) {
	// Negative: unknown directive panics
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic for unknown Directive")
		}
	}()
	_ = Directive(99).String()
}

func TestWithMailboxSizePanicsOnZero(t *testing.T) {
	// Negative: zero mailbox size panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for zero mailbox size")
		}
	}()
	opt := WithMailboxSize(0)
	opt(&spawnOptions{})
}

func TestWithSupervisionPanicsOnNil(t *testing.T) {
	// Negative: nil strategy panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for nil strategy")
		}
	}()
	opt := WithSupervision(nil)
	opt(&spawnOptions{})
}

func TestSpawnPanicsOnEmptyAddress(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	// Negative: empty Type panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for empty address")
		}
	}()
	rt.Spawn(Address{Type: "", ID: "x"}, &testNopActor{})
}

func TestSpawnPanicsOnNilActor(t *testing.T) {
	rt := NewRuntime()
	defer rt.StopAll()

	// Negative: nil actor panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for nil actor")
		}
	}()
	rt.Spawn(Address{Type: "t", ID: "x"}, nil)
}

// testNopActor is a minimal actor that does nothing.
type testNopActor struct{}

func (a *testNopActor) Receive(
	ctx *Context, msg Message,
) error {
	return nil
}

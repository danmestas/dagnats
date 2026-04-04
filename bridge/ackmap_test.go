// ackmap_test.go
// Unit tests for AckMap — pure Go, no NATS dependency.
// Methodology: test store/load/delete lifecycle and panic assertions.
package bridge

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestAckMapStoreAndLoad(t *testing.T) {
	am := NewAckMap()
	msg := &nats.Msg{Subject: "task.echo.run1"}

	am.Store("run1.step1", msg)

	got, ok := am.Load("run1.step1")
	if !ok {
		t.Fatal("expected Load to return true for stored key")
	}
	if got != msg {
		t.Fatal("expected Load to return the same message")
	}

	// Negative: key not present
	_, ok = am.Load("run1.missing")
	if ok {
		t.Fatal("expected Load to return false for missing key")
	}
}

func TestAckMapDelete(t *testing.T) {
	am := NewAckMap()
	msg := &nats.Msg{Subject: "task.echo.run1"}

	am.Store("run1.step1", msg)
	am.Delete("run1.step1")

	_, ok := am.Load("run1.step1")
	if ok {
		t.Fatal("expected Load to return false after Delete")
	}

	// Count should be zero
	if am.Count() != 0 {
		t.Fatalf("expected count 0 after delete, got %d", am.Count())
	}
}

func TestAckMapCount(t *testing.T) {
	am := NewAckMap()
	msg1 := &nats.Msg{Subject: "task.echo.run1"}
	msg2 := &nats.Msg{Subject: "task.echo.run2"}

	if am.Count() != 0 {
		t.Fatalf("expected count 0, got %d", am.Count())
	}

	am.Store("run1.step1", msg1)
	am.Store("run2.step1", msg2)

	if am.Count() != 2 {
		t.Fatalf("expected count 2, got %d", am.Count())
	}
}

func TestAckMapDeleteNonExistent(t *testing.T) {
	am := NewAckMap()

	// Should not panic or go negative
	am.Delete("nonexistent")

	if am.Count() != 0 {
		t.Fatalf("expected count 0, got %d", am.Count())
	}
}

func TestAckMapStorePanicsEmptyID(t *testing.T) {
	am := NewAckMap()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty taskID")
		}
	}()
	am.Store("", &nats.Msg{})
}

func TestAckMapStorePanicsNilMsg(t *testing.T) {
	am := NewAckMap()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil msg")
		}
	}()
	am.Store("run1.step1", nil)
}

func TestAckMapLoadPanicsEmptyID(t *testing.T) {
	am := NewAckMap()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty taskID")
		}
	}()
	am.Load("")
}

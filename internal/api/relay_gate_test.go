// relay_gate_test.go
// The relay's concurrency bound: one program filling its share is
// refused while another still gets slots, and a release frees a slot.
// Methodology: pure, no NATS.
package api

import "testing"

func TestRelayGateBoundsEachProgram(t *testing.T) {
	var gate relayGate
	for i := 0; i < relayPerProgramMax; i++ {
		if !gate.acquire("notify.", relayClassOther) {
			t.Fatalf("slot %d refused below the bound", i)
		}
	}
	if gate.acquire("notify.", relayClassOther) {
		t.Fatal("a program got a slot past its bound")
	}
	if !gate.acquire("artifact.", relayClassOther) {
		t.Fatal("a flooding program starved another")
	}
	gate.release("notify.", relayClassOther)
	if !gate.acquire("notify.", relayClassOther) {
		t.Fatal("a released slot was not reusable")
	}
}

// TestRelayGateReservesStepEvents: a program whose journal replay holds
// every one of its other slots still gets step-event slots, up to their
// own smaller bound.
func TestRelayGateReservesStepEvents(t *testing.T) {
	var gate relayGate
	for i := 0; i < relayPerProgramMax; i++ {
		if !gate.acquire("notify.", relayClassOther) {
			t.Fatalf("other slot %d refused", i)
		}
	}
	for i := 0; i < relayEventsPerProgramMax; i++ {
		if !gate.acquire("notify.", relayClassEvents) {
			t.Fatalf("step-event slot %d refused while the journal was busy", i)
		}
	}
	if gate.acquire("notify.", relayClassEvents) {
		t.Fatal("step events got a slot past their own bound")
	}
}

func TestRelayGateBoundsTheNode(t *testing.T) {
	var gate relayGate
	held := 0
	for p := 0; held < relayTotalMax; p++ {
		prefix := "p" + string(rune('a'+p%26)) + string(rune('a'+p/26)) + "."
		for i := 0; i < relayPerProgramMax && held < relayTotalMax; i++ {
			if !gate.acquire(prefix, relayClassOther) {
				t.Fatalf("refused below the node bound at %d", held)
			}
			held++
		}
	}
	if gate.acquire("fresh.", relayClassEvents) {
		t.Fatal("a slot past the node bound was granted")
	}
}

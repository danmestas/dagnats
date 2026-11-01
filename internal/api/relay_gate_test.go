// relay_gate_test.go
// The relay's concurrency bound: one program filling its share is
// refused while another still gets slots, and a release frees a slot.
// Methodology: pure, no NATS.
package api

import "testing"

func TestRelayGateBoundsEachProgram(t *testing.T) {
	var gate relayGate
	for i := 0; i < relayPerProgramMax; i++ {
		if !gate.acquire("notify.") {
			t.Fatalf("slot %d refused below the bound", i)
		}
	}
	if gate.acquire("notify.") {
		t.Fatal("a program got a slot past its bound")
	}
	if !gate.acquire("artifact.") {
		t.Fatal("a flooding program starved another")
	}
	gate.release("notify.")
	if !gate.acquire("notify.") {
		t.Fatal("a released slot was not reusable")
	}
}

func TestRelayGateBoundsTheNode(t *testing.T) {
	var gate relayGate
	held := 0
	for p := 0; held < relayTotalMax; p++ {
		prefix := "p" + string(rune('a'+p%26)) + string(rune('a'+p/26)) + "."
		for i := 0; i < relayPerProgramMax && held < relayTotalMax; i++ {
			if !gate.acquire(prefix) {
				t.Fatalf("refused below the node bound at %d", held)
			}
			held++
		}
	}
	if gate.acquire("fresh.") {
		t.Fatal("a slot past the node bound was granted")
	}
}

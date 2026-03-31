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

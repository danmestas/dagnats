// e2e/harness/embedded_test.go
// Tests for the embedded topology provider. Methodology: create an
// embedded topology, connect, verify JetStream is available and KV
// buckets are created by Setup.
package harness

import (
	"testing"
)

func TestEmbeddedConnectAndSetup(t *testing.T) {
	topo := NewEmbedded()

	// Positive: name is correct.
	if topo.Name() != "embedded" {
		t.Fatalf("expected name 'embedded', got %q", topo.Name())
	}

	nc := topo.Connect(t)
	topo.Setup(t, nc)

	// Positive: JetStream is available.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Positive: KV bucket created by Setup.
	_, err = js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs bucket not found: %v", err)
	}

	// Negative: connection is alive.
	if !nc.IsConnected() {
		t.Fatal("expected connection to be alive")
	}
}

// e2e/harness/local_cluster_test.go
// Tests for the local cluster topology provider. Methodology: start
// a production-like single NATS server, verify JetStream and KV work.
package harness

import (
	"testing"
)

func TestLocalClusterConnectAndSetup(t *testing.T) {
	topo := NewLocalCluster()

	// Positive: name is correct.
	if topo.Name() != "local_cluster" {
		t.Fatalf("expected name 'local_cluster', got %q", topo.Name())
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

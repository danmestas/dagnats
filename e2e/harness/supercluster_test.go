// e2e/harness/supercluster_test.go
// Tests for the supercluster topology. Methodology: start 2 clusters
// + 1 leaf, verify JetStream and KV work through Cluster A, verify
// gateways and leaf formed, verify kill/restart resilience methods.
package harness

import (
	"testing"
	"time"
)

func TestSuperclusterConnectAndSetup(t *testing.T) {
	topo := NewSupercluster()

	// Positive: name is correct.
	if topo.Name() != "supercluster" {
		t.Fatalf("expected name 'supercluster', got %q", topo.Name())
	}

	nc := topo.Connect(t)
	topo.Setup(t, nc)

	// Positive: JetStream is available.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	kv, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs bucket not found: %v", err)
	}

	// Write and read through the cluster.
	_, err = kv.Put("test-key", []byte("test-value"))
	if err != nil {
		t.Fatalf("KV Put: %v", err)
	}
	entry, err := kv.Get("test-key")
	if err != nil {
		t.Fatalf("KV Get: %v", err)
	}
	if string(entry.Value()) != "test-value" {
		t.Fatalf("expected 'test-value', got %q", string(entry.Value()))
	}

	// Negative: connection is alive.
	if !nc.IsConnected() {
		t.Fatal("expected connection to be alive")
	}
}

func TestSuperclusterTopologyFormed(t *testing.T) {
	sc := NewSupercluster()
	_ = sc.Connect(t)

	// Positive: gateways formed between clusters.
	gw := sc.clusterB[0].NumOutboundGateways()
	if gw < 1 {
		t.Fatalf("expected >= 1 outbound gateway, got %d", gw)
	}

	// Positive: leaf connected to Cluster A.
	leafs := sc.clusterA[0].NumLeafNodes()
	if leafs < 1 {
		t.Fatalf("expected >= 1 leaf node, got %d", leafs)
	}
}

func TestSuperclusterKillAndRestart(t *testing.T) {
	sc := NewSupercluster()
	nc := sc.Connect(t)
	sc.Setup(t, nc)

	// Positive: JetStream works before disruption.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	kv, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}

	_, err = kv.Put("survive", []byte("data"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Kill a1 (not a0 — client is connected to a0).
	if err := sc.KillNode("a1"); err != nil {
		t.Fatalf("KillNode a1: %v", err)
	}

	// Allow failover.
	time.Sleep(500 * time.Millisecond)

	// Positive: data survives node failure.
	entry, err := kv.Get("survive")
	if err != nil {
		t.Fatalf("Get after kill: %v", err)
	}
	if string(entry.Value()) != "data" {
		t.Fatalf("expected 'data', got %q", string(entry.Value()))
	}

	// Restart the killed node.
	if err := sc.RestartNode("a1"); err != nil {
		t.Fatalf("RestartNode a1: %v", err)
	}

	// Negative: data still accessible after restart.
	entry, err = kv.Get("survive")
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if string(entry.Value()) != "data" {
		t.Fatalf("expected 'data', got %q", string(entry.Value()))
	}
}

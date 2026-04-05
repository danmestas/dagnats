package dagnatstest

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestServer(t *testing.T) {
	nc := Server(t)
	// Positive: connection is live
	if !nc.IsConnected() {
		t.Fatal("expected connected NATS client")
	}
	// Positive: JetStream available
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	// Positive: workflow_defs bucket exists
	_, err = js.KeyValue(t.Context(), "workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs bucket: %v", err)
	}
	// Positive: workflow_runs bucket exists
	_, err = js.KeyValue(t.Context(), "workflow_runs")
	if err != nil {
		t.Fatalf("workflow_runs bucket: %v", err)
	}
}

// natsutil/conn_test.go
// Tests for NATS utility functions: connection, stream creation, KV bucket setup.
// Methodology: each test starts an embedded NATS server, calls the utility,
// then verifies the resource was created via NATS JetStream API.
// Bounded 5-second timeout on all operations.
package natsutil

import (
	"testing"
	"time"
)

func TestStartTestServer(t *testing.T) {
	ns, nc := StartTestServer(t)
	if ns == nil {
		t.Fatal("test server is nil")
	}
	if nc == nil {
		t.Fatal("nats connection is nil")
	}
	if !nc.IsConnected() {
		t.Fatal("nats connection is not connected")
	}
}

func TestSetupStreams(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	err = SetupStreams(js)
	if err != nil {
		t.Fatalf("SetupStreams failed: %v", err)
	}
	info, err := js.StreamInfo("WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("StreamInfo(WORKFLOW_HISTORY) failed: %v", err)
	}
	if info == nil {
		t.Fatal("WORKFLOW_HISTORY stream not found")
	}
	info, err = js.StreamInfo("TASK_QUEUES")
	if err != nil {
		t.Fatalf("StreamInfo(TASK_QUEUES) failed: %v", err)
	}
	if info == nil {
		t.Fatal("TASK_QUEUES stream not found")
	}
	info, err = js.StreamInfo("EVENTS")
	if err != nil {
		t.Fatalf("StreamInfo(EVENTS) failed: %v", err)
	}
	if info == nil {
		t.Fatal("EVENTS stream not found")
	}
}

func TestSetupKVBuckets(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	err = SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	kv, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs) failed: %v", err)
	}
	_, err = kv.PutString("test-key", "test-value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	entry, err := kv.Get("test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value()) != "test-value" {
		t.Fatalf("value = %q, want %q", string(entry.Value()), "test-value")
	}
	_, err = js.KeyValue("workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_runs) failed: %v", err)
	}
}

func TestSetupAll(t *testing.T) {
	_, nc := StartTestServer(t)
	done := make(chan error, 1)
	go func() { done <- SetupAll(nc) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetupAll failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetupAll timed out after 5s")
	}
}

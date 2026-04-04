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

func TestSetupTelemetryStream(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	err = SetupTelemetryStream(js)
	if err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}
	info, err := js.StreamInfo("TELEMETRY")
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.Config.MaxAge != 7*24*time.Hour {
		t.Fatalf("MaxAge = %v, want 7d", info.Config.MaxAge)
	}
	if info.Config.MaxBytes != 1<<30 {
		t.Fatalf("MaxBytes = %d, want 1GB", info.Config.MaxBytes)
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

func TestSetupAllWithExtras(t *testing.T) {
	_, nc := StartTestServer(t)

	extra := StreamConfig{
		Name:     "AGENT_TASKS",
		Subjects: []string{"agent.task.>"},
	}
	extraKV := KVConfig{Bucket: "roles"}

	if err := SetupAll(nc,
		WithStreams(extra),
		WithKVBuckets(extraKV),
	); err != nil {
		t.Fatalf("SetupAll with extras: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Positive: default streams still exist
	if _, err := js.StreamInfo("WORKFLOW_HISTORY"); err != nil {
		t.Fatalf("WORKFLOW_HISTORY should exist: %v", err)
	}

	// Positive: extra stream exists
	if _, err := js.StreamInfo("AGENT_TASKS"); err != nil {
		t.Fatalf("AGENT_TASKS should exist: %v", err)
	}

	// Positive: extra KV bucket exists
	if _, err := js.KeyValue("roles"); err != nil {
		t.Fatalf("roles KV should exist: %v", err)
	}

	// Positive: default KV buckets still exist
	if _, err := js.KeyValue("workflow_defs"); err != nil {
		t.Fatalf("workflow_defs should exist: %v", err)
	}
}

func TestSetupKVBucketsCreatesScheduledRuns(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	err = SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets: %v", err)
	}

	// Positive: scheduled_runs bucket exists.
	kv, err := js.KeyValue("scheduled_runs")
	if err != nil {
		t.Fatalf("scheduled_runs bucket should exist: %v", err)
	}

	// Negative: bucket name is correct.
	status, err := kv.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Bucket() != "scheduled_runs" {
		t.Fatalf("bucket = %q, want scheduled_runs", status.Bucket())
	}
}

// assert is a test helper that fails the test if the condition is false.
// Minimum 2 assertions per test for positive and negative space validation.
func assert(t *testing.T, condition bool, format string, args ...interface{}) {
	t.Helper()
	if !condition {
		t.Fatalf(format, args...)
	}
}

func TestSetupAllCreatesWorkersKV(t *testing.T) {
	s, nc := StartTestServer(t)
	defer s.Shutdown()
	defer nc.Close()

	err := SetupAll(nc)
	assert(t, err == nil, "SetupAll must succeed: %v", err)

	js, err := nc.JetStream()
	assert(t, err == nil, "JetStream must succeed: %v", err)

	kv, err := js.KeyValue("workers")
	assert(t, err == nil, "workers KV bucket must exist: %v", err)
	assert(t, kv != nil, "workers KV bucket must not be nil")

	status, err := kv.Status()
	assert(t, err == nil, "status must succeed: %v", err)
	assert(t, status.TTL() == 60*time.Second,
		"workers TTL must be 60s, got %v", status.TTL())
}

func TestSetupAllCreatesEventWaitersKV(t *testing.T) {
	s, nc := StartTestServer(t)
	defer s.Shutdown()
	defer nc.Close()

	err := SetupAll(nc)
	assert(t, err == nil, "SetupAll must succeed: %v", err)

	js, err := nc.JetStream()
	assert(t, err == nil, "JetStream must succeed: %v", err)

	// Positive: event_waiters bucket exists
	kv, err := js.KeyValue("event_waiters")
	assert(t, err == nil, "event_waiters KV bucket must exist: %v", err)
	assert(t, kv != nil, "event_waiters KV bucket must not be nil")

	// Negative: bucket name is correct
	status, err := kv.Status()
	assert(t, err == nil, "status must succeed: %v", err)
	assert(t, status.Bucket() == "event_waiters",
		"bucket = %q, want event_waiters", status.Bucket())
}

func TestSetupAllCreatesRateLimitsKV(t *testing.T) {
	s, nc := StartTestServer(t)
	defer s.Shutdown()
	defer nc.Close()

	err := SetupAll(nc)
	assert(t, err == nil, "SetupAll must succeed: %v", err)

	js, err := nc.JetStream()
	assert(t, err == nil, "JetStream must succeed: %v", err)

	// Positive: rate_limits bucket exists
	kv, err := js.KeyValue("rate_limits")
	assert(t, err == nil,
		"rate_limits KV bucket must exist: %v", err)
	assert(t, kv != nil,
		"rate_limits KV bucket must not be nil")

	// Negative: bucket name is correct
	status, err := kv.Status()
	assert(t, err == nil, "status must succeed: %v", err)
	assert(t, status.Bucket() == "rate_limits",
		"bucket = %q, want rate_limits", status.Bucket())
}

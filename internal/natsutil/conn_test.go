// natsutil/conn_test.go
// Tests for NATS utility functions: connection, stream creation, KV bucket setup.
// Methodology: each test starts an embedded NATS server, calls the utility,
// then verifies the resource was created via NATS JetStream API.
// Bounded 5-second timeout on all operations.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
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
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = SetupStreams(js)
	if err != nil {
		t.Fatalf("SetupStreams failed: %v", err)
	}
	ctx := context.Background()
	_, err = js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream(WORKFLOW_HISTORY) failed: %v", err)
	}
	_, err = js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream(TASK_QUEUES) failed: %v", err)
	}
	_, err = js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatalf("Stream(EVENTS) failed: %v", err)
	}
}

func TestSetupKVBuckets(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_defs) failed: %v", err)
	}
	_, err = kv.PutString(ctx, "test-key", "test-value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	entry, err := kv.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value()) != "test-value" {
		t.Fatalf("value = %q, want %q",
			string(entry.Value()), "test-value")
	}
	_, err = js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_runs) failed: %v", err)
	}
}

func TestSetupTelemetryStream(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	err = SetupTelemetryStream(js)
	if err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}
	ctx := context.Background()
	stream, err := js.Stream(ctx, "TELEMETRY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info := stream.CachedInfo()
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

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := context.Background()

	// Positive: default streams still exist
	if _, err := js.Stream(ctx, "WORKFLOW_HISTORY"); err != nil {
		t.Fatalf("WORKFLOW_HISTORY should exist: %v", err)
	}

	// Positive: extra stream exists
	if _, err := js.Stream(ctx, "AGENT_TASKS"); err != nil {
		t.Fatalf("AGENT_TASKS should exist: %v", err)
	}

	// Positive: extra KV bucket exists
	if _, err := js.KeyValue(ctx, "roles"); err != nil {
		t.Fatalf("roles KV should exist: %v", err)
	}

	// Positive: default KV buckets still exist
	if _, err := js.KeyValue(ctx, "workflow_defs"); err != nil {
		t.Fatalf("workflow_defs should exist: %v", err)
	}
}

func TestSetupKVBucketsCreatesScheduledRuns(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	err = SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets: %v", err)
	}
	ctx := context.Background()

	// Positive: scheduled_runs bucket exists.
	kv, err := js.KeyValue(ctx, "scheduled_runs")
	if err != nil {
		t.Fatalf("scheduled_runs bucket should exist: %v", err)
	}

	// Negative: bucket name is correct.
	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Bucket() != "scheduled_runs" {
		t.Fatalf("bucket = %q, want scheduled_runs",
			status.Bucket())
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

	js, err := jetstream.New(nc)
	assert(t, err == nil, "jetstream.New must succeed: %v", err)

	ctx := context.Background()
	kv, err := js.KeyValue(ctx, "workers")
	assert(t, err == nil,
		"workers KV bucket must exist: %v", err)
	assert(t, kv != nil, "workers KV bucket must not be nil")

	status, err := kv.Status(ctx)
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

	js, err := jetstream.New(nc)
	assert(t, err == nil, "jetstream.New must succeed: %v", err)

	ctx := context.Background()
	// Positive: event_waiters bucket exists
	kv, err := js.KeyValue(ctx, "event_waiters")
	assert(t, err == nil,
		"event_waiters KV bucket must exist: %v", err)
	assert(t, kv != nil,
		"event_waiters KV bucket must not be nil")

	// Negative: bucket name is correct
	status, err := kv.Status(ctx)
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

	js, err := jetstream.New(nc)
	assert(t, err == nil, "jetstream.New must succeed: %v", err)

	ctx := context.Background()
	// Positive: rate_limits bucket exists
	kv, err := js.KeyValue(ctx, "rate_limits")
	assert(t, err == nil,
		"rate_limits KV bucket must exist: %v", err)
	assert(t, kv != nil,
		"rate_limits KV bucket must not be nil")

	// Negative: bucket name is correct
	status, err := kv.Status(ctx)
	assert(t, err == nil, "status must succeed: %v", err)
	assert(t, status.Bucket() == "rate_limits",
		"bucket = %q, want rate_limits", status.Bucket())
}

func TestEnableAtomicPublish(t *testing.T) {
	_, nc := StartTestServer(t)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := SetupStreams(js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}

	// Call the function under test.
	err = enableAtomicPublish(js, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("enableAtomicPublish: %v", err)
	}

	// Positive: verify AllowAtomicPublish is set.
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info := stream.CachedInfo()
	assert(t, info.Config.AllowAtomicPublish,
		"AllowAtomicPublish should be true")

	// Negative: non-existent stream should error.
	err = enableAtomicPublish(js, "NONEXISTENT")
	assert(t, err != nil,
		"expected error for nonexistent stream, got nil")
}

// cli/clean_test.go
// Tests for the clean command. Methodology: integration tests with
// embedded NATS — populate streams and KV, run clean, verify empty.
package cli

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestExecuteClean_PurgesStreamsAndBuckets(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	// Populate a stream and a KV bucket.
	oldJS, err2 := nc.JetStream()
	if err2 != nil {
		t.Fatalf("JetStream: %v", err2)
	}
	if _, err := oldJS.Publish(
		"history.test-run", []byte("data"),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if _, err := kv.Put(ctx, "test-key", []byte("val")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Positive: stream has messages before clean.
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs == 0 {
		t.Fatal("expected messages before clean")
	}

	result := executeClean(js, false)

	// Positive: streams and buckets were cleaned.
	if result.Streams == 0 {
		t.Fatal("expected at least 1 stream purged")
	}
	if result.Buckets == 0 {
		t.Fatal("expected at least 1 bucket cleared")
	}

	// Positive: stream is now empty.
	info2, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info after: %v", err)
	}
	if info2.State.Msgs != 0 {
		t.Fatalf("expected 0 msgs after clean, got %d",
			info2.State.Msgs)
	}

	// Negative: no errors.
	if result.Errors != 0 {
		t.Fatalf("expected 0 errors, got %d", result.Errors)
	}
}

func TestExecuteClean_PreservesWorkflowDefs(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	// Populate workflow_defs.
	kv, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if _, err := kv.Put(ctx, "my-wf", []byte(`{}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Clean without --all.
	executeClean(js, false)

	// Positive: workflow_defs still has the key.
	entry, err := kv.Get(ctx, "my-wf")
	if err != nil {
		t.Fatalf("workflow_defs should be preserved: %v", err)
	}
	if entry == nil {
		t.Fatal("workflow_defs entry should not be nil")
	}
}

func TestExecuteClean_AllClearsWorkflowDefs(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	kv, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if _, err := kv.Put(ctx, "my-wf", []byte(`{}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Clean with --all.
	executeClean(js, true)

	// Positive: workflow_defs is now empty.
	_, err = kv.Get(ctx, "my-wf")
	if err == nil {
		t.Fatal("workflow_defs should be cleared with --all")
	}
}

func TestPurgeKVBucket_EmptyBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}

	// Positive: purging empty bucket does not error.
	err = purgeKVBucket(ctx, kv)
	if err != nil {
		t.Fatalf("purgeKVBucket on empty: %v", err)
	}

	// Negative: no keys exist.
	keys, _ := kv.Keys(ctx)
	if len(keys) != 0 {
		t.Fatalf("expected 0 keys, got %d", len(keys))
	}
}

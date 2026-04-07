// cli/clean_test.go
// Tests for the clean command. Methodology: integration tests with
// embedded NATS — populate streams and KV, run clean, verify empty.
// Unit tests for flag parsing and category resolution.
package cli

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// --- Unit tests: parseDuration ---

func TestParseDuration_Days(t *testing.T) {
	dur, err := parseDuration("7d")
	if err != nil {
		t.Fatalf("parseDuration(7d): %v", err)
	}
	want := 7 * 24 * time.Hour
	if dur != want {
		t.Errorf("got %v, want %v", dur, want)
	}

	// Negative: zero days is invalid.
	_, err = parseDuration("0d")
	if err == nil {
		t.Error("expected error for 0d")
	}
}

func TestParseDuration_Hours(t *testing.T) {
	dur, err := parseDuration("24h")
	if err != nil {
		t.Fatalf("parseDuration(24h): %v", err)
	}
	if dur != 24*time.Hour {
		t.Errorf("got %v, want 24h", dur)
	}

	// Negative: empty string.
	_, err = parseDuration("")
	if err == nil {
		t.Error("expected error for empty duration")
	}
}

// --- Unit tests: resolveCategories ---

func TestResolveCategories_Default(t *testing.T) {
	f := cleanFlags{}
	cats := resolveCategories(f)

	// Positive: default is runs + dlq.
	if len(cats) != 2 {
		t.Fatalf("want 2 categories, got %d", len(cats))
	}
	if cats[0] != "runs" || cats[1] != "dlq" {
		t.Errorf("want [runs dlq], got %v", cats)
	}
}

func TestResolveCategories_All(t *testing.T) {
	f := cleanFlags{all: true}
	cats := resolveCategories(f)

	// Positive: all categories returned.
	if len(cats) != len(allCategories) {
		t.Fatalf("want %d categories, got %d",
			len(allCategories), len(cats))
	}

	// Negative: no duplicates.
	seen := make(map[string]bool)
	for _, c := range cats {
		if seen[c] {
			t.Errorf("duplicate category: %s", c)
		}
		seen[c] = true
	}
}

func TestResolveCategories_TypeOverridesAll(t *testing.T) {
	f := cleanFlags{all: true, types: []string{"otel"}}
	cats := resolveCategories(f)

	// Positive: --type takes precedence over --all.
	if len(cats) != 1 || cats[0] != "otel" {
		t.Errorf("want [otel], got %v", cats)
	}
}

// --- Unit tests: collectTargets ---

func TestCollectTargets_Runs(t *testing.T) {
	streams, buckets := collectTargets([]string{"runs"})

	// Positive: runs has streams and buckets.
	if len(streams) == 0 {
		t.Fatal("runs category should have streams")
	}
	if len(buckets) == 0 {
		t.Fatal("runs category should have buckets")
	}

	// Negative: no TELEMETRY in runs.
	for _, s := range streams {
		if s == "TELEMETRY" {
			t.Error("runs should not include TELEMETRY")
		}
	}
}

func TestCollectTargets_NoDuplicates(t *testing.T) {
	// Request the same category twice.
	streams, buckets := collectTargets(
		[]string{"runs", "runs"})

	seen := make(map[string]bool)
	for _, s := range streams {
		if seen[s] {
			t.Errorf("duplicate stream: %s", s)
		}
		seen[s] = true
	}
	for _, b := range buckets {
		if seen[b] {
			t.Errorf("duplicate bucket: %s", b)
		}
		seen[b] = true
	}
}

// --- Integration tests ---

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

	streams, buckets := collectTargets(defaultCategories)
	result := executeClean(ctx, js, streams, buckets, 0)

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

	// Clean default categories (no defs).
	streams, buckets := collectTargets(defaultCategories)
	executeClean(ctx, js, streams, buckets, 0)

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

	// Clean all categories (includes defs).
	streams, buckets := collectTargets(allCategories)
	executeClean(ctx, js, streams, buckets, 0)

	// Positive: workflow_defs is now empty.
	_, err = kv.Get(ctx, "my-wf")
	if err == nil {
		t.Fatal("workflow_defs should be cleared with --all")
	}
}

func TestExecuteClean_TypeOtelOnly(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	// Populate both WORKFLOW_HISTORY and TELEMETRY.
	oldJS, _ := nc.JetStream()
	oldJS.Publish("history.test", []byte("data"))
	oldJS.Publish("telemetry.span", []byte("span"))

	// Clean only otel.
	streams, buckets := collectTargets([]string{"otel"})
	result := executeClean(ctx, js, streams, buckets, 0)

	// Positive: TELEMETRY was purged.
	if result.Streams != 1 {
		t.Fatalf("expected 1 stream purged, got %d",
			result.Streams)
	}

	// Negative: WORKFLOW_HISTORY should be untouched.
	hist, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := hist.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs == 0 {
		t.Fatal("WORKFLOW_HISTORY should not have been purged")
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

func TestDryRunReport_ShowsStreamInfo(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	// Populate a stream.
	oldJS, _ := nc.JetStream()
	oldJS.Publish("history.test", []byte("data"))

	streams, buckets := collectTargets(defaultCategories)
	report := dryRunReport(ctx, js, streams, buckets, 0)

	// Positive: report has entries.
	if len(report.Entries) == 0 {
		t.Fatal("expected dry-run entries")
	}

	// Positive: total messages > 0.
	if report.TotalMsgs == 0 {
		t.Fatal("expected total messages > 0")
	}

	// Negative: empty streams should not appear.
	for _, e := range report.Entries {
		if e.Messages == 0 {
			t.Errorf("entry %s has 0 messages", e.Name)
		}
	}
}

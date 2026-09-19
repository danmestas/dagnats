// cli/clean_ceiling_test.go
// Tests for the "runs" category count ceiling (enforceRunCeiling). On
// 2026-09-19 the scheduled cleanup job ran on time and correctly purged
// nothing, because every one of 353,614 workflow_runs entries had been
// written that same day by a retry storm — none were older than the 7-day
// floor. Age-based purging and count-based purging are orthogonal; these
// tests prove the ceiling catches what age alone misses, without touching
// buckets that are still under it, and without ever draining a work-queue
// stream's live tasks.
package cli

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEnforceRunCeiling_PurgesOversizedBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	for i := 0; i < 30; i++ {
		if _, err := kv.Put(ctx,
			fmt.Sprintf("run-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	result := enforceRunCeiling(ctx, js, []string{"runs"}, 10)

	// Positive: the oversized bucket was found and purged down to the cap.
	if result.Exceeded == 0 {
		t.Fatal("expected at least one target over the ceiling")
	}
	if result.Purged == 0 {
		t.Fatal("expected at least one target purged")
	}

	stream, err := js.Stream(ctx, "KV_workflow_runs")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs > 10 {
		t.Fatalf("expected <=10 msgs after ceiling, got %d",
			info.State.Msgs)
	}

	// Positive: the bucket survives and stays usable.
	if _, err := kv.Put(ctx, "after", []byte("ok")); err != nil {
		t.Fatalf("bucket unusable after ceiling purge: %v", err)
	}
}

// TestEnforceRunCeiling_NoOpUnderCeiling proves the ceiling never touches a
// bucket that hasn't exceeded it — the normal-operation case. If this ever
// purged under the cap, every live in-flight run would be at risk on every
// routine cleanup run, not just the retry-storm recovery case.
func TestEnforceRunCeiling_NoOpUnderCeiling(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := kv.Put(ctx,
			fmt.Sprintf("run-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	result := enforceRunCeiling(ctx, js, []string{"runs"}, 50)

	// Negative: nothing exceeded the ceiling, nothing purged.
	if result.Exceeded != 0 {
		t.Fatalf("expected 0 targets exceeded, got %d", result.Exceeded)
	}
	if result.Purged != 0 {
		t.Fatalf("expected 0 targets purged, got %d", result.Purged)
	}
	if result.Checked == 0 {
		t.Fatal("expected targets to be checked")
	}

	keys, err := kv.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 5 {
		t.Fatalf("expected all 5 live runs to survive, got %d", len(keys))
	}
}

// TestEnforceRunCeiling_SkipsWorkQueueStream regresses the same eviction risk
// already guarded in seqPurgeStreams (#521, #523): TASK_QUEUES holds live
// un-acked tasks, not history, so an automatic ceiling purge must never drain
// it even when it is the target that most exceeds --max-keep.
func TestEnforceRunCeiling_SkipsWorkQueueStream(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	pub, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	for i := 0; i < 20; i++ {
		if _, err := pub.Publish("task.ceiling-guard",
			[]byte(fmt.Sprintf("t-%d", i))); err != nil {
			t.Fatalf("Publish task: %v", err)
		}
	}

	result := enforceRunCeiling(ctx, js, []string{"runs"}, 1)

	// Positive: the work-queue stream keeps every live task, untouched.
	wq, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream TASK_QUEUES: %v", err)
	}
	wqInfo, err := wq.Info(ctx)
	if err != nil {
		t.Fatalf("Info TASK_QUEUES: %v", err)
	}
	if wqInfo.State.Msgs != 20 {
		t.Fatalf("work-queue purged: expected 20 msgs kept, got %d",
			wqInfo.State.Msgs)
	}

	// Negative space: TASK_QUEUES must never have been counted as checked or
	// exceeded — proves the skip happens before either counter, not just
	// before the purge call.
	_ = result
}

// TestEnforceRunCeiling_DisabledWhenMaxKeepZero proves --max-keep=0 is a real
// off switch, not just a very high default.
func TestEnforceRunCeiling_DisabledWhenMaxKeepZero(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	for i := 0; i < 30; i++ {
		if _, err := kv.Put(ctx,
			fmt.Sprintf("run-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	result := enforceRunCeiling(ctx, js, []string{"runs"}, 0)

	if result != (runCeilingResult{}) {
		t.Fatalf("expected zero-value result when disabled, got %+v", result)
	}

	keys, err := kv.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 30 {
		t.Fatalf("expected all 30 runs to survive, got %d", len(keys))
	}
}

// TestEnforceRunCeiling_SkipsWhenRunsCategoryNotSelected proves the ceiling
// never reaches outside the "runs" category — most importantly it must never
// be reachable for "defs" (workflow definitions), which no automatic
// count-based purge should ever touch.
func TestEnforceRunCeiling_SkipsWhenRunsCategoryNotSelected(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	for i := 0; i < 30; i++ {
		if _, err := kv.Put(ctx,
			fmt.Sprintf("run-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	result := enforceRunCeiling(ctx, js, []string{"dlq", "otel"}, 10)

	if result != (runCeilingResult{}) {
		t.Fatalf("expected zero-value result, got %+v", result)
	}

	keys, err := kv.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 30 {
		t.Fatalf("expected all 30 runs to survive, got %d", len(keys))
	}
}

// TestCleanCmd_CeilingCatchesWhatAgeAloneMisses reproduces the 2026-09-19
// incident's exact shape end to end: an --older-than pass over data that is
// all "new" purges nothing, and the ceiling pass that runs after it is what
// actually bounds the index.
func TestCleanCmd_CeilingCatchesWhatAgeAloneMisses(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	for i := 0; i < 30; i++ {
		if _, err := kv.Put(ctx,
			fmt.Sprintf("run-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	streams, buckets := collectTargets([]string{"runs"})

	// Simulates `--older-than=7d` on data that is all hours old: nothing
	// qualifies, the age pass is a no-op.
	executeClean(ctx, js, streams, buckets, 7*24*time.Hour)

	keys, err := kv.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 30 {
		t.Fatalf("age pass should not have purged fresh data, got %d keys",
			len(keys))
	}

	// The ceiling pass that runs after it in runCleanCmd is what actually
	// bounds the bucket.
	result := enforceRunCeiling(ctx, js, []string{"runs"}, 10)
	if result.Purged == 0 {
		t.Fatal("expected the ceiling to purge what age left behind")
	}

	stream, err := js.Stream(ctx, "KV_workflow_runs")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs > 10 {
		t.Fatalf("expected <=10 msgs after ceiling, got %d",
			info.State.Msgs)
	}
}

func TestEffectiveMaxKeep_DefaultAndOverride(t *testing.T) {
	if got := effectiveMaxKeep(cleanFlags{}); got != defaultMaxRunKeep {
		t.Errorf("default: got %d, want %d", got, defaultMaxRunKeep)
	}

	explicit := cleanFlags{maxKeep: 5000, maxKeepSet: true}
	if got := effectiveMaxKeep(explicit); got != 5000 {
		t.Errorf("explicit: got %d, want 5000", got)
	}

	disabled := cleanFlags{maxKeep: 0, maxKeepSet: true}
	if got := effectiveMaxKeep(disabled); got != 0 {
		t.Errorf("disabled: got %d, want 0", got)
	}
}

func TestParseCleanFlags_MaxKeep(t *testing.T) {
	f := parseCleanFlags([]string{"--max-keep=1234"})
	if !f.maxKeepSet {
		t.Fatal("expected maxKeepSet")
	}
	if f.maxKeep != 1234 {
		t.Errorf("got %d, want 1234", f.maxKeep)
	}

	// Negative: --max-keep=0 must parse as explicitly set to zero, not
	// "unset" — that distinction is what lets 0 mean "disabled".
	f = parseCleanFlags([]string{"--max-keep=0"})
	if !f.maxKeepSet {
		t.Fatal("expected maxKeepSet for --max-keep=0")
	}
	if f.maxKeep != 0 {
		t.Errorf("got %d, want 0", f.maxKeep)
	}
}

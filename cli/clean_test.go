// cli/clean_test.go
// Tests for the clean command. Methodology: integration tests with
// embedded NATS — populate streams and KV, run clean, verify empty.
// Unit tests for flag parsing and category resolution.
package cli

import (
	"context"
	"fmt"
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
	err = purgeKVBucket(ctx, js, "workflow_runs")
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

func TestParseCleanFlags_AllFlags(t *testing.T) {
	args := []string{
		"--all", "--force", "--json", "--dry-run",
		"--older-than=7d", "--type=otel,dlq",
	}
	f := parseCleanFlags(args)

	if !f.all {
		t.Error("expected all=true")
	}
	if !f.force {
		t.Error("expected force=true")
	}
	if !f.json {
		t.Error("expected json=true")
	}
	if !f.dryRun {
		t.Error("expected dryRun=true")
	}
	if f.olderThan != 7*24*time.Hour {
		t.Errorf("olderThan = %v, want 168h", f.olderThan)
	}
	if len(f.types) != 2 ||
		f.types[0] != "otel" || f.types[1] != "dlq" {
		t.Errorf("types = %v, want [otel dlq]", f.types)
	}
}

func TestParseCleanFlags_Empty(t *testing.T) {
	f := parseCleanFlags([]string{})

	if f.all || f.force || f.json || f.dryRun {
		t.Error("expected all flags false for empty args")
	}
	if f.olderThan != 0 {
		t.Errorf("olderThan = %v, want 0", f.olderThan)
	}
	if len(f.types) != 0 {
		t.Errorf("types = %v, want empty", f.types)
	}
}

func TestFormatCleanBytes(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{1073741824, "1.0 GiB"},
		{1610612736, "1.5 GiB"},
	}
	for _, tt := range tests {
		got := formatCleanBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatCleanBytes(%d) = %q, want %q",
				tt.input, got, tt.want)
		}
	}

	if formatCleanBytes(1023) != "1023 B" {
		t.Error("1023 should be bytes, not KiB")
	}
}

func TestPurgeStreamBefore_PartialPurge(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	oldJS, _ := nc.JetStream()
	for i := 0; i < 5; i++ {
		oldJS.Publish("history.purge-test",
			[]byte(fmt.Sprintf("msg-%d", i)))
	}

	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	info, _ := stream.Info(ctx)
	if info.State.Msgs != 5 {
		t.Fatalf("expected 5 msgs, got %d",
			info.State.Msgs)
	}

	purged := purgeStreamBefore(ctx, stream, 24*time.Hour)
	if purged {
		t.Error("expected no purge for fresh messages")
	}

	info2, _ := stream.Info(ctx)
	if info2.State.Msgs != 5 {
		t.Fatalf("expected 5 msgs after no-op purge, got %d",
			info2.State.Msgs)
	}
}

func TestPurgeStreamBefore_AllOld(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	oldJS, _ := nc.JetStream()
	oldJS.Publish("history.old-test", []byte("old"))

	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	purged := purgeStreamBefore(ctx, stream, time.Millisecond)
	if !purged {
		t.Error("expected purge for old message")
	}

	info, _ := stream.Info(ctx)
	if info.State.Msgs != 0 {
		t.Errorf("expected 0 msgs after purge, got %d",
			info.State.Msgs)
	}
}

// --- Unit tests: bulk prune flags (--keep / --before-seq) ---

func TestParseCleanFlags_KeepAndBeforeSeq(t *testing.T) {
	f := parseCleanFlags([]string{"--keep=30000"})

	// Positive: --keep parses into keep, before-seq untouched.
	if f.keep != 30000 {
		t.Errorf("keep = %d, want 30000", f.keep)
	}
	if f.beforeSeq != 0 {
		t.Errorf("beforeSeq = %d, want 0", f.beforeSeq)
	}

	f2 := parseCleanFlags([]string{"--before-seq=42"})

	// Positive: --before-seq parses into beforeSeq, keep untouched.
	if f2.beforeSeq != 42 {
		t.Errorf("beforeSeq = %d, want 42", f2.beforeSeq)
	}
	if f2.keep != 0 {
		t.Errorf("keep = %d, want 0", f2.keep)
	}
}

func TestValidateCleanFlags_MutualExclusion(t *testing.T) {
	// Negative: --keep and --before-seq cannot combine (nats.go forbids it).
	if err := validateCleanFlags(cleanFlags{
		keep: 10, beforeSeq: 5, force: true,
	}); err == nil {
		t.Error("keep + before-seq should be rejected")
	}

	// Negative: --keep and --older-than are different strategies.
	if err := validateCleanFlags(cleanFlags{
		keep: 10, olderThan: time.Hour, force: true,
	}); err == nil {
		t.Error("keep + older-than should be rejected")
	}

	// Negative: --before-seq and --older-than are different strategies.
	if err := validateCleanFlags(cleanFlags{
		beforeSeq: 10, olderThan: time.Hour, force: true,
	}); err == nil {
		t.Error("before-seq + older-than should be rejected")
	}

	// Positive: --keep alone with --force is valid.
	if err := validateCleanFlags(cleanFlags{
		keep: 10, force: true,
	}); err != nil {
		t.Errorf("keep + force should be valid: %v", err)
	}
}

func TestValidateCleanFlags_ForceRequired(t *testing.T) {
	// Negative: bulk prune without --force or --dry-run is refused.
	if err := validateCleanFlags(cleanFlags{keep: 10}); err == nil {
		t.Error("keep without force/dry-run should be rejected")
	}
	if err := validateCleanFlags(cleanFlags{beforeSeq: 10}); err == nil {
		t.Error("before-seq without force/dry-run should be rejected")
	}

	// Positive: --dry-run is an allowed preview without --force.
	if err := validateCleanFlags(cleanFlags{
		keep: 10, dryRun: true,
	}); err != nil {
		t.Errorf("keep + dry-run should be valid: %v", err)
	}

	// Positive: age-based clean without force is unaffected by the gate.
	if err := validateCleanFlags(cleanFlags{
		olderThan: time.Hour,
	}); err != nil {
		t.Errorf("older-than without force should be valid: %v", err)
	}
}

func TestSeqPurgeWarningText(t *testing.T) {
	// Positive: seq mode without --json emits the safety warning.
	if seqPurgeWarningText(cleanFlags{keep: 10}) == "" {
		t.Error("expected warning for --keep")
	}
	if seqPurgeWarningText(cleanFlags{beforeSeq: 10}) == "" {
		t.Error("expected warning for --before-seq")
	}

	// Negative: --json suppresses human warning; non-seq mode has none.
	if seqPurgeWarningText(cleanFlags{keep: 10, json: true}) != "" {
		t.Error("--json should suppress the warning")
	}
	if seqPurgeWarningText(cleanFlags{}) != "" {
		t.Error("non-seq mode should have no warning")
	}
}

// --- Integration tests: bulk prune (--keep / --before-seq) ---

func TestExecuteSeqPurge_KeepBucketDrainsToLimit(t *testing.T) {
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
	for i := 0; i < 50; i++ {
		if _, err := kv.Put(ctx,
			fmt.Sprintf("run-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	streams, buckets := collectTargets([]string{"runs"})

	// Tight deadline: the O(1) stream purge must beat a per-key loop.
	purgeCtx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()
	result := executeSeqPurge(purgeCtx, js, streams, buckets, 10, 0)

	// Positive: backing stream drained to <= keep.
	stream, err := js.Stream(ctx, "KV_workflow_runs")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs > 10 {
		t.Fatalf("expected <=10 msgs after keep, got %d",
			info.State.Msgs)
	}
	if result.Buckets == 0 {
		t.Fatal("expected at least 1 bucket purged")
	}

	// Positive: bucket survives and stays usable.
	if _, err := kv.Put(ctx, "after", []byte("ok")); err != nil {
		t.Fatalf("bucket unusable after keep-purge: %v", err)
	}

	// Negative: no errors.
	if result.Errors != 0 {
		t.Fatalf("expected 0 errors, got %d", result.Errors)
	}
}

func TestExecuteSeqPurge_BeforeSeqPurgesBelow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	oldJS, _ := nc.JetStream()
	for i := 0; i < 5; i++ {
		if _, err := oldJS.Publish("history.seq-test",
			[]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Purge messages below sequence 3 (keeps seqs 3,4,5).
	result := executeSeqPurge(ctx, js,
		[]string{"WORKFLOW_HISTORY"}, nil, 0, 3)

	// Positive: one stream purged, three messages remain.
	if result.Streams != 1 {
		t.Fatalf("expected 1 stream purged, got %d", result.Streams)
	}
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs != 3 {
		t.Fatalf("expected 3 msgs after before-seq, got %d",
			info.State.Msgs)
	}

	// Negative: first surviving message is seq 3, not below.
	if info.State.FirstSeq != 3 {
		t.Fatalf("expected first seq 3, got %d",
			info.State.FirstSeq)
	}
}

// TestExecuteSeqPurge_SkipsWorkQueueStream regresses the #523 review finding
// that a bulk sequence purge would drain TASK_QUEUES — a work-queue stream of
// live un-acked tasks. The recipe `clean --type=runs --keep=N` selects
// TASK_QUEUES, so an unguarded seq purge silently drops queued work. The purge
// must skip work-queue streams (as the age path does) while still draining the
// history stream, proving the skip is a real guard and not a no-op.
func TestExecuteSeqPurge_SkipsWorkQueueStream(t *testing.T) {
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
	for i := 0; i < 5; i++ {
		if _, err := pub.Publish("task.seq-guard",
			[]byte(fmt.Sprintf("t-%d", i))); err != nil {
			t.Fatalf("Publish task: %v", err)
		}
		if _, err := pub.Publish("history.seq-guard",
			[]byte(fmt.Sprintf("h-%d", i))); err != nil {
			t.Fatalf("Publish history: %v", err)
		}
	}

	// Keep only the newest 1 across all runs targets.
	streams, buckets := collectTargets([]string{"runs"})
	result := executeSeqPurge(ctx, js, streams, buckets, 1, 0)

	// Positive: the work-queue stream retains ALL its live messages, untouched.
	wq, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream TASK_QUEUES: %v", err)
	}
	wqInfo, err := wq.Info(ctx)
	if err != nil {
		t.Fatalf("Info TASK_QUEUES: %v", err)
	}
	if wqInfo.State.Msgs != 5 {
		t.Fatalf("work-queue purged: expected 5 msgs kept, got %d",
			wqInfo.State.Msgs)
	}

	// Negative space: the ordinary history stream WAS drained to keep=1, so the
	// purge genuinely ran — the work-queue skip is a guard, not a dead path.
	hist, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream WORKFLOW_HISTORY: %v", err)
	}
	histInfo, err := hist.Info(ctx)
	if err != nil {
		t.Fatalf("Info WORKFLOW_HISTORY: %v", err)
	}
	if histInfo.State.Msgs != 1 {
		t.Fatalf("expected history drained to 1, got %d",
			histInfo.State.Msgs)
	}
	if result.Errors != 0 {
		t.Fatalf("expected 0 errors, got %d", result.Errors)
	}
}

func TestSeqDryRunReport_DoesNotExecute(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx := context.Background()

	oldJS, _ := nc.JetStream()
	for i := 0; i < 5; i++ {
		if _, err := oldJS.Publish("history.dry-seq",
			[]byte(fmt.Sprintf("m-%d", i))); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	report := seqDryRunReport(ctx, js,
		[]string{"WORKFLOW_HISTORY"}, nil, 2, 0)

	// Positive: report estimates 3 purged (5 msgs, keep 2).
	if report.TotalMsgs != 3 {
		t.Fatalf("expected estimate 3, got %d", report.TotalMsgs)
	}

	// Negative: dry-run did not touch the stream.
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs != 5 {
		t.Fatalf("dry-run should not execute; got %d msgs",
			info.State.Msgs)
	}
}

func TestPurgeKVBucketBefore_SelectiveDelete(t *testing.T) {
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

	kv.Put(ctx, "old-key", []byte("old"))
	time.Sleep(50 * time.Millisecond)
	kv.Put(ctx, "new-key", []byte("new"))

	err = purgeKVBucketBefore(ctx, kv, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("purgeKVBucketBefore: %v", err)
	}

	_, err = kv.Get(ctx, "old-key")
	if err == nil {
		t.Error("old-key should have been deleted")
	}

	entry, err := kv.Get(ctx, "new-key")
	if err != nil {
		t.Fatalf("new-key should survive: %v", err)
	}
	if string(entry.Value()) != "new" {
		t.Errorf("value = %q, want new",
			string(entry.Value()))
	}
}

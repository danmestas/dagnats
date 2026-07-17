// cli/clean.go
// Purge run data from streams and runtime KV buckets. Preserves
// workflow definitions and telemetry by default. Supports filtering
// by category (--type) and age (--older-than).
package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// cleanCategory groups related streams and KV buckets.
type cleanCategory struct {
	Streams []string
	Buckets []string
}

// categoryMap defines the named data categories available for cleanup.
var categoryMap = map[string]cleanCategory{
	"runs": {
		Streams: []string{
			"WORKFLOW_HISTORY",
			"TASK_QUEUES",
			"EVENTS",
			"SLEEP_TIMERS",
		},
		Buckets: []string{
			"workflow_runs",
			"scheduled_runs",
			"event_waiters",
			"rate_limits",
			"concurrency_tasks",
			"approval_tokens",
			"debounce_state",
			"idempotency_keys",
			"sticky_bindings",
			"singleton_locks",
		},
	},
	"dlq": {
		Streams: []string{"DEAD_LETTERS"},
	},
	"otel": {
		Streams: []string{"TELEMETRY"},
	},
	"defs": {
		Buckets: []string{"workflow_defs"},
	},
}

// defaultCategories are cleaned when no --type is specified.
var defaultCategories = []string{"runs", "dlq"}

// allCategories lists every category name.
var allCategories = []string{"runs", "dlq", "otel", "defs"}

// cleanFlags holds parsed command-line options for clean.
type cleanFlags struct {
	all       bool
	force     bool
	json      bool
	dryRun    bool
	olderThan time.Duration
	types     []string
}

// parseCleanFlags extracts flags from args.
func parseCleanFlags(args []string) cleanFlags {
	if args == nil {
		panic("parseCleanFlags: args must not be nil")
	}
	if len(args) > 100 {
		panic("parseCleanFlags: args exceeds max bound")
	}

	var f cleanFlags
	for _, arg := range args {
		switch {
		case arg == "--all":
			f.all = true
		case arg == "--force":
			f.force = true
		case arg == "--json":
			f.json = true
		case arg == "--dry-run":
			f.dryRun = true
		case strings.HasPrefix(arg, "--older-than="):
			val := strings.TrimPrefix(arg, "--older-than=")
			dur, err := parseDuration(val)
			if err != nil {
				fmt.Fprintf(os.Stderr,
					"invalid --older-than: %v\n", err)
				os.Exit(1)
			}
			f.olderThan = dur
		case strings.HasPrefix(arg, "--type="):
			val := strings.TrimPrefix(arg, "--type=")
			f.types = strings.Split(val, ",")
			for _, t := range f.types {
				if _, ok := categoryMap[t]; !ok {
					fmt.Fprintf(os.Stderr,
						"unknown type: %q (valid: %s)\n",
						t, strings.Join(allCategories, ", "))
					os.Exit(1)
				}
			}
		}
	}
	return f
}

// parseDuration parses durations with day support: "7d", "24h", "30m".
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	// Handle day suffix: convert to hours for time.ParseDuration.
	if strings.HasSuffix(s, "d") {
		numStr := strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(numStr)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf(
				"%q: must be a positive integer of days", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	return time.ParseDuration(s)
}

// resolveCategories returns the category names to clean based on flags.
func resolveCategories(f cleanFlags) []string {
	if len(f.types) > 0 {
		return f.types
	}
	if f.all {
		return allCategories
	}
	return defaultCategories
}

// collectTargets gathers all streams and buckets for the given categories.
func collectTargets(
	categories []string,
) (streams, buckets []string) {
	if categories == nil {
		panic("collectTargets: categories must not be nil")
	}
	const maxCategories = 20
	if len(categories) > maxCategories {
		panic("collectTargets: categories exceeds max bound")
	}

	seen := make(map[string]bool)
	for _, name := range categories {
		cat, ok := categoryMap[name]
		if !ok {
			continue
		}
		for _, s := range cat.Streams {
			if !seen[s] {
				seen[s] = true
				streams = append(streams, s)
			}
		}
		for _, b := range cat.Buckets {
			if !seen[b] {
				seen[b] = true
				buckets = append(buckets, b)
			}
		}
	}
	return streams, buckets
}

// runCleanCmd purges run data from streams and KV buckets.
func runCleanCmd(args []string) {
	if args == nil {
		panic("runCleanCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runCleanCmd: args exceeds max bound")
	}

	if HasHelpFlag(args) {
		printCleanUsage()
		return
	}

	f := parseCleanFlags(args)
	categories := resolveCategories(f)
	streams, buckets := collectTargets(categories)

	if !f.force && !f.json {
		label := "all"
		if f.olderThan > 0 {
			label = fmt.Sprintf("older than %s", f.olderThan)
		}
		fmt.Printf(
			"This will purge %s data (%s). Continue? [y/N] ",
			strings.Join(categories, ", "), label)
		var answer string
		fmt.Scanln(&answer)
		if answer != "y" && answer != "Y" {
			fmt.Println("Aborted.")
			return
		}
	}

	_, nc := connectService()
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	// The full-clear path is O(1) (KV backing-stream purge), so this ceiling
	// only bounds the --older-than per-key loop, which iterates up to maxKeys
	// (100k) entries, each under its own perKeyTimeout parented on this ctx.
	// A tight 30s budget spuriously failed a large but healthy bucket, so the
	// deadline is generous while still guaranteeing the command terminates
	// and stays cancellable (#521).
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Minute,
	)
	defer cancel()

	if f.dryRun {
		report := dryRunReport(ctx, js, streams, buckets, f.olderThan)
		if f.json {
			if err := FormatJSON(os.Stdout, report); err != nil {
				fmt.Fprintf(os.Stderr, "format json: %v\n", err)
				os.Exit(1)
			}
			return
		}
		printDryRunReport(report)
		return
	}

	result := executeClean(ctx, js, streams, buckets, f.olderThan)

	if f.json {
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	printCleanResult(result)
}

// cleanResult holds the outcome for display.
type cleanResult struct {
	Streams int `json:"streams_purged"`
	Buckets int `json:"buckets_cleared"`
	Errors  int `json:"errors"`
}

// executeClean purges streams and clears KV buckets.
func executeClean(
	ctx context.Context,
	js jetstream.JetStream,
	streams, buckets []string,
	olderThan time.Duration,
) cleanResult {
	if js == nil {
		panic("executeClean: js must not be nil")
	}

	var result cleanResult
	result.Streams = purgeStreams(ctx, js, streams, olderThan)
	cleared, errs := clearBuckets(ctx, js, buckets, olderThan)
	result.Buckets = cleared
	result.Errors = errs
	return result
}

// purgeStreams purges each named stream. When olderThan > 0, only
// purges messages older than the cutoff using WithPurgeSequence.
// Returns count of successfully purged streams.
func purgeStreams(
	ctx context.Context,
	js jetstream.JetStream,
	names []string,
	olderThan time.Duration,
) int {
	if js == nil {
		panic("purgeStreams: js must not be nil")
	}
	const maxStreams = 50
	if len(names) > maxStreams {
		panic("purgeStreams: names exceeds max bound")
	}

	purged := 0
	for _, name := range names {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			continue
		}

		if olderThan > 0 {
			if purgeStreamBefore(ctx, stream, olderThan) {
				purged++
			}
			continue
		}

		if err := stream.Purge(ctx); err != nil {
			fmt.Fprintf(os.Stderr,
				"warn: purge %s: %v\n", name, err)
			continue
		}
		purged++
	}
	return purged
}

// purgeStreamBefore purges messages older than olderThan from a
// stream. Uses an ordered consumer starting at the cutoff time to
// find the first message to keep, then purges everything before it.
func purgeStreamBefore(
	ctx context.Context,
	stream jetstream.Stream,
	olderThan time.Duration,
) bool {
	if stream == nil {
		panic("purgeStreamBefore: stream must not be nil")
	}
	if olderThan <= 0 {
		panic("purgeStreamBefore: olderThan must be positive")
	}

	cutoff := time.Now().Add(-olderThan)

	// Check if any messages are older than cutoff.
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs == 0 {
		return false
	}

	// Age-based purge does not apply to work-queue streams: un-acked
	// messages are live pending tasks, not history — trimming them by age
	// silently drops work. The ordered consumer used below to find the age
	// boundary is also rejected on a work-queue (err 10084, explicit ack
	// required). A work-queue self-drains as workers ack, so skip it (#521).
	if info.Config.Retention == jetstream.WorkQueuePolicy {
		fmt.Fprintf(os.Stderr,
			"info: skip age-based purge of %s: work-queue stream self-drains\n",
			info.Config.Name)
		return false
	}

	// If the newest message is older than cutoff, purge everything.
	if info.State.LastTime.Before(cutoff) {
		if err := stream.Purge(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "warn: purge %s: %v\n",
				info.Config.Name, err)
			return false
		}
		return true
	}

	// If the oldest message is newer than cutoff, nothing to purge.
	if info.State.FirstTime.After(cutoff) ||
		info.State.FirstTime.Equal(cutoff) {
		return false
	}

	// Find the first message at or after the cutoff using an
	// ordered consumer. Its sequence becomes the purge boundary.
	cons, err := stream.OrderedConsumer(ctx,
		jetstream.OrderedConsumerConfig{
			OptStartTime: &cutoff,
		})
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"warn: consumer for %s: %v\n",
			info.Config.Name, err)
		return false
	}

	msg, err := cons.Next(
		jetstream.FetchMaxWait(5 * time.Second),
	)
	if err != nil {
		// No messages at or after cutoff — purge all.
		if err := stream.Purge(ctx); err != nil {
			return false
		}
		return true
	}

	meta, err := msg.Metadata()
	if err != nil {
		return false
	}

	// Purge everything before this sequence.
	if err := stream.Purge(ctx,
		jetstream.WithPurgeSequence(meta.Sequence.Stream),
	); err != nil {
		fmt.Fprintf(os.Stderr,
			"warn: purge %s before seq %d: %v\n",
			info.Config.Name, meta.Sequence.Stream, err)
		return false
	}
	return true
}

// clearBuckets deletes keys from each KV bucket. When olderThan > 0,
// only deletes entries with revision timestamps older than cutoff.
func clearBuckets(
	ctx context.Context,
	js jetstream.JetStream,
	names []string,
	olderThan time.Duration,
) (int, int) {
	if js == nil {
		panic("clearBuckets: js must not be nil")
	}
	const maxBuckets = 50
	if len(names) > maxBuckets {
		panic("clearBuckets: names exceeds max bound")
	}

	cleared := 0
	errors := 0
	for _, name := range names {
		kv, err := js.KeyValue(ctx, name)
		if err != nil {
			continue
		}
		var cleanErr error
		if olderThan > 0 {
			cleanErr = purgeKVBucketBefore(ctx, kv, olderThan)
		} else {
			cleanErr = purgeKVBucket(ctx, js, name)
		}
		if cleanErr != nil {
			fmt.Fprintf(os.Stderr,
				"warn: clear %s: %v\n", name, cleanErr)
			errors++
			continue
		}
		cleared++
	}
	return cleared, errors
}

// purgeKVBucket empties a KV bucket in one round trip by purging its backing
// JetStream stream (KV_<bucket>). A per-key Delete loop over a large bucket
// (~222k run keys) blew the shared clean deadline (#521); the backing-stream
// purge is O(1) — the same mechanism already used to purge streams in this
// file. The bucket itself is preserved (KV streams allow purge: DenyDelete is
// set but DenyPurge is not).
func purgeKVBucket(
	ctx context.Context, js jetstream.JetStream, bucket string,
) error {
	if js == nil {
		panic("purgeKVBucket: js must not be nil")
	}
	if bucket == "" {
		panic("purgeKVBucket: bucket must not be empty")
	}

	stream, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return fmt.Errorf("kv backing stream %s: %w", bucket, err)
	}
	if err := stream.Purge(ctx); err != nil {
		return fmt.Errorf("purge kv %s: %w", bucket, err)
	}
	return nil
}

// purgeKVBucketBefore deletes keys with revision timestamps older
// than the cutoff.
func purgeKVBucketBefore(
	ctx context.Context,
	kv jetstream.KeyValue,
	olderThan time.Duration,
) error {
	if kv == nil {
		panic("purgeKVBucketBefore: kv must not be nil")
	}
	if olderThan <= 0 {
		panic("purgeKVBucketBefore: olderThan must be positive")
	}

	cutoff := time.Now().Add(-olderThan)

	keys, err := kv.Keys(ctx)
	if err != nil {
		return nil
	}

	const maxKeys = 100_000
	const perKeyTimeout = 5 * time.Second
	for i, key := range keys {
		if i >= maxKeys {
			break
		}
		// Per-key bounded context so one slow Get/Delete cannot exhaust a
		// deadline shared across the whole bucket (#521). Parented on the
		// caller's ctx (not Background) so an interrupted clean cancels the
		// loop; the generous clean-wide deadline keeps a large but healthy
		// bucket from being starved before it finishes.
		opCtx, cancel := context.WithTimeout(
			ctx, perKeyTimeout,
		)
		entry, err := kv.Get(opCtx, key)
		if err != nil {
			cancel()
			continue
		}
		if entry.Created().Before(cutoff) {
			if err := kv.Delete(opCtx, key); err != nil {
				cancel()
				return fmt.Errorf("delete key %s: %w", key, err)
			}
		}
		cancel()
	}
	return nil
}

// dryRunEntry describes one stream or bucket for dry-run output.
type dryRunEntry struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Messages uint64 `json:"messages"`
	Bytes    uint64 `json:"bytes"`
}

// dryRunResult holds the full dry-run report.
type dryRunResult struct {
	Entries    []dryRunEntry `json:"entries"`
	TotalBytes uint64        `json:"total_bytes"`
	TotalMsgs  uint64        `json:"total_messages"`
}

// dryRunReport builds a report of what would be cleaned.
func dryRunReport(
	ctx context.Context,
	js jetstream.JetStream,
	streams, buckets []string,
	olderThan time.Duration,
) dryRunResult {
	if js == nil {
		panic("dryRunReport: js must not be nil")
	}

	var result dryRunResult

	for _, name := range streams {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			continue
		}
		info, err := stream.Info(ctx)
		if err != nil {
			continue
		}
		msgs := info.State.Msgs
		bytes := info.State.Bytes
		// For time-filtered purge, this is an estimate — we
		// report total stream size since per-message byte
		// accounting would require iterating every message.
		if olderThan > 0 && msgs > 0 {
			cutoff := time.Now().Add(-olderThan)
			if info.State.FirstTime.After(cutoff) {
				msgs = 0
				bytes = 0
			}
		}
		if msgs > 0 {
			result.Entries = append(result.Entries, dryRunEntry{
				Name:     name,
				Kind:     "stream",
				Messages: msgs,
				Bytes:    bytes,
			})
			result.TotalBytes += bytes
			result.TotalMsgs += msgs
		}
	}

	for _, name := range buckets {
		kv, err := js.KeyValue(ctx, name)
		if err != nil {
			continue
		}
		keys, err := kv.Keys(ctx)
		if err != nil {
			continue
		}
		count := uint64(len(keys))
		if olderThan > 0 {
			cutoff := time.Now().Add(-olderThan)
			old := uint64(0)
			for _, key := range keys {
				entry, err := kv.Get(ctx, key)
				if err != nil {
					continue
				}
				if entry.Created().Before(cutoff) {
					old++
				}
			}
			count = old
		}
		if count > 0 {
			result.Entries = append(result.Entries, dryRunEntry{
				Name:     name,
				Kind:     "kv",
				Messages: count,
			})
			result.TotalMsgs += count
		}
	}

	return result
}

// printDryRunReport displays the dry-run report.
func printDryRunReport(r dryRunResult) {
	if len(r.Entries) == 0 {
		fmt.Println("Nothing to clean.")
		return
	}

	fmt.Println("Would clean:")
	for _, e := range r.Entries {
		if e.Bytes > 0 {
			fmt.Printf("  %-25s %s  %d msgs  %s\n",
				e.Name, e.Kind, e.Messages,
				formatCleanBytes(e.Bytes))
		} else {
			fmt.Printf("  %-25s %s  %d keys\n",
				e.Name, e.Kind, e.Messages)
		}
	}
	fmt.Printf("\nTotal: %d messages", r.TotalMsgs)
	if r.TotalBytes > 0 {
		fmt.Printf(", %s", formatCleanBytes(r.TotalBytes))
	}
	fmt.Println()
}

// formatCleanBytes returns a human-readable byte string.
func formatCleanBytes(b uint64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case b >= gib:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(gib))
	case b >= mib:
		return fmt.Sprintf("%.1f MiB", float64(b)/float64(mib))
	case b >= kib:
		return fmt.Sprintf("%.1f KiB", float64(b)/float64(kib))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// printCleanResult displays the clean outcome.
func printCleanResult(r cleanResult) {
	fmt.Printf("Purged %d streams, cleared %d KV buckets",
		r.Streams, r.Buckets)
	if r.Errors > 0 {
		fmt.Printf(" (%d errors)", r.Errors)
	}
	fmt.Println()
}

// printCleanUsage prints clean command help.
func printCleanUsage() {
	fmt.Println("Usage: dagnats clean [flags]")
	fmt.Println()
	fmt.Println("Purge data from streams and KV buckets.")
	fmt.Println(
		"Cleans runs and dlq by default." +
			" Use --type or --all to target more.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println(
		"  --type=<categories>  " +
			"comma-separated: runs, dlq, otel, defs")
	fmt.Println(
		"  --older-than=<dur>   " +
			"only clean data older than duration (7d, 24h)")
	fmt.Println(
		"  --dry-run            " +
			"show what would be cleaned without doing it")
	fmt.Println(
		"  --all                " +
			"clean all categories (runs, dlq, otel, defs)")
	fmt.Println(
		"  --force              skip confirmation prompt")
	fmt.Println(
		"  --json               output result as JSON")
}

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
	// keep and beforeSeq drive the bulk stream-sequence prune (WithPurgeKeep /
	// WithPurgeSequence). Zero means unset; both must be positive when used.
	keep      uint64
	beforeSeq uint64
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
		case strings.HasPrefix(arg, "--keep="):
			f.keep = parseUintFlag(arg, "--keep=")
		case strings.HasPrefix(arg, "--before-seq="):
			f.beforeSeq = parseUintFlag(arg, "--before-seq=")
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

// parseUintFlag parses a positive uint value from a "--flag=N" argument. Zero
// is rejected: a zero keep/before-seq purges everything, which the plain full
// clean already does — surfacing it as an error avoids a surprising blunt wipe.
func parseUintFlag(arg, prefix string) uint64 {
	if !strings.HasPrefix(arg, prefix) {
		panic("parseUintFlag: arg missing prefix")
	}
	if prefix == "" {
		panic("parseUintFlag: prefix must not be empty")
	}

	val := strings.TrimPrefix(arg, prefix)
	n, err := strconv.ParseUint(val, 10, 64)
	if err != nil || n == 0 {
		fmt.Fprintf(os.Stderr,
			"invalid %sN: must be a positive integer\n", prefix)
		os.Exit(1)
	}
	return n
}

// validateCleanFlags rejects mutually-exclusive or unsafe flag combinations.
// --keep and --before-seq map to nats.go WithPurgeKeep/WithPurgeSequence, which
// the client refuses to combine; both are whole-stream sequence prunes and must
// not silently mix with the age-based --older-than strategy. Because a sequence
// prune purges by write order (not run age or terminal status) it can evict a
// live in-flight run, so it is gated behind --force unless previewed via
// --dry-run.
func validateCleanFlags(f cleanFlags) error {
	if f.keep > 0 && f.beforeSeq > 0 {
		return fmt.Errorf(
			"--keep and --before-seq are mutually exclusive")
	}
	if (f.keep > 0 || f.beforeSeq > 0) && f.olderThan > 0 {
		return fmt.Errorf(
			"--keep/--before-seq cannot combine with --older-than")
	}
	if (f.keep > 0 || f.beforeSeq > 0) && !f.force && !f.dryRun {
		return fmt.Errorf(
			"--keep/--before-seq require --force " +
				"(blunt recovery tool); use --dry-run to preview")
	}
	return nil
}

// seqPurgeWarning explains why a sequence-based prune is dangerous: it purges by
// global stream sequence, not run age or terminal status, so a long-pending live
// run whose snapshot has an old last-write sequence can be evicted while newer
// terminal runs survive. KV delete-tombstones also count as messages, so fewer
// than N keys may remain.
const seqPurgeWarning = "warning: --keep/--before-seq purge by stream " +
	"sequence, not age — they can evict live in-flight runs whose last " +
	"write is old, and KV tombstones count toward the total so fewer than " +
	"N keys may remain; --older-than is the safe default, --keep is a blunt " +
	"recovery tool for a bucket that cannot be drained otherwise."

// seqPurgeWarningText returns the stderr warning for a bulk prune, or "" when
// none is needed (not a sequence prune, or --json suppresses human output).
func seqPurgeWarningText(f cleanFlags) string {
	if f.json {
		return ""
	}
	if f.keep == 0 && f.beforeSeq == 0 {
		return ""
	}
	return seqPurgeWarning
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
	if err := validateCleanFlags(f); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	categories := resolveCategories(f)
	streams, buckets := collectTargets(categories)
	seqMode := f.keep > 0 || f.beforeSeq > 0

	// The confirmation gates mutation only: --dry-run previews without touching
	// data, so it must never prompt (a sequence prune is otherwise force-gated
	// by validateCleanFlags and skips this too). Only the mutating age/full path
	// reaches the prompt.
	if !f.force && !f.json && !f.dryRun {
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

	if w := seqPurgeWarningText(f); w != "" {
		fmt.Fprintln(os.Stderr, w)
	}

	if f.dryRun {
		var report dryRunResult
		if seqMode {
			report = seqDryRunReport(
				ctx, js, streams, buckets, f.keep, f.beforeSeq)
		} else {
			report = dryRunReport(
				ctx, js, streams, buckets, f.olderThan)
		}
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

	// A bulk prune replaces the age/full path entirely — a single server-side
	// sequence purge, never the per-key/age loop.
	var result cleanResult
	if seqMode {
		result = executeSeqPurge(
			ctx, js, streams, buckets, f.keep, f.beforeSeq)
	} else {
		result = executeClean(ctx, js, streams, buckets, f.olderThan)
	}

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

// purgeSeqOpt maps the keep/beforeSeq flags to the matching single-round-trip
// nats.go purge option. Exactly one must be set — the caller guarantees this
// via validateCleanFlags.
func purgeSeqOpt(keep, beforeSeq uint64) jetstream.StreamPurgeOpt {
	if keep == 0 && beforeSeq == 0 {
		panic("purgeSeqOpt: keep or beforeSeq must be set")
	}
	if keep > 0 && beforeSeq > 0 {
		panic("purgeSeqOpt: keep and beforeSeq are exclusive")
	}
	if keep > 0 {
		return jetstream.WithPurgeKeep(keep)
	}
	return jetstream.WithPurgeSequence(beforeSeq)
}

// executeSeqPurge drains selected streams and KV backing-streams by global
// sequence using server-side WithPurgeKeep/WithPurgeSequence. Unlike the age
// path's per-key loop, this is one round trip per target and drains a bucket of
// any size — the recovery tool for a KV_workflow_runs that has already cliffed
// past what the per-key loop can iterate within the deadline (#523).
func executeSeqPurge(
	ctx context.Context,
	js jetstream.JetStream,
	streams, buckets []string,
	keep, beforeSeq uint64,
) cleanResult {
	if js == nil {
		panic("executeSeqPurge: js must not be nil")
	}
	if keep == 0 && beforeSeq == 0 {
		panic("executeSeqPurge: keep or beforeSeq must be set")
	}

	opt := purgeSeqOpt(keep, beforeSeq)
	var result cleanResult
	result.Streams = seqPurgeStreams(ctx, js, streams, opt)
	cleared, errs := seqPurgeBuckets(ctx, js, buckets, opt)
	result.Buckets = cleared
	result.Errors = errs
	return result
}

// seqPurgeStreams purges each named stream with opt. Returns the count purged.
func seqPurgeStreams(
	ctx context.Context,
	js jetstream.JetStream,
	names []string,
	opt jetstream.StreamPurgeOpt,
) int {
	if js == nil {
		panic("seqPurgeStreams: js must not be nil")
	}
	const maxStreams = 50
	if len(names) > maxStreams {
		panic("seqPurgeStreams: names exceeds max bound")
	}

	purged := 0
	for _, name := range names {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			continue
		}
		// A sequence purge does not apply to work-queue streams (e.g.
		// TASK_QUEUES): un-acked messages are live pending tasks, not history.
		// Keeping the newest N by sequence — or dropping below a boundary —
		// would silently discard un-started queued work. A work-queue
		// self-drains as workers ack, so skip it, mirroring the age-path guard
		// (#521, #523).
		info, err := stream.Info(ctx)
		if err != nil {
			continue
		}
		if info.Config.Retention == jetstream.WorkQueuePolicy {
			fmt.Fprintf(os.Stderr,
				"info: skip sequence purge of %s: work-queue stream self-drains\n",
				name)
			continue
		}
		if err := stream.Purge(ctx, opt); err != nil {
			fmt.Fprintf(os.Stderr,
				"warn: purge %s: %v\n", name, err)
			continue
		}
		purged++
	}
	return purged
}

// seqPurgeBuckets purges each KV bucket's backing stream (KV_<bucket>) with opt.
// The bucket is preserved (only messages are purged). Returns (cleared, errors).
func seqPurgeBuckets(
	ctx context.Context,
	js jetstream.JetStream,
	names []string,
	opt jetstream.StreamPurgeOpt,
) (int, int) {
	if js == nil {
		panic("seqPurgeBuckets: js must not be nil")
	}
	const maxBuckets = 50
	if len(names) > maxBuckets {
		panic("seqPurgeBuckets: names exceeds max bound")
	}

	cleared := 0
	errors := 0
	for _, name := range names {
		stream, err := js.Stream(ctx, "KV_"+name)
		if err != nil {
			continue
		}
		if err := stream.Purge(ctx, opt); err != nil {
			fmt.Fprintf(os.Stderr,
				"warn: purge kv %s: %v\n", name, err)
			errors++
			continue
		}
		cleared++
	}
	return cleared, errors
}

// seqPurgeEstimate estimates how many messages a keep/beforeSeq purge removes
// from a stream in the given state. It is an upper bound: server-side sequence
// gaps and tombstones make the exact count unknowable without iterating.
func seqPurgeEstimate(
	msgs, firstSeq, lastSeq, keep, beforeSeq uint64,
) uint64 {
	if keep > 0 {
		if msgs > keep {
			return msgs - keep
		}
		return 0
	}
	if beforeSeq <= firstSeq {
		return 0
	}
	if beforeSeq > lastSeq {
		return msgs
	}
	span := beforeSeq - firstSeq
	if span > msgs {
		return msgs
	}
	return span
}

// seqDryRunReport reports what a keep/beforeSeq purge would remove without
// executing it. Backing-stream (KV_<bucket>) sizes are read directly so the
// estimate reflects tombstones the KV key count would hide.
func seqDryRunReport(
	ctx context.Context,
	js jetstream.JetStream,
	streams, buckets []string,
	keep, beforeSeq uint64,
) dryRunResult {
	if js == nil {
		panic("seqDryRunReport: js must not be nil")
	}
	if keep == 0 && beforeSeq == 0 {
		panic("seqDryRunReport: keep or beforeSeq must be set")
	}

	var result dryRunResult
	for _, name := range streams {
		addSeqDryRunEntry(ctx, js, name, name, "stream",
			keep, beforeSeq, &result)
	}
	for _, name := range buckets {
		addSeqDryRunEntry(ctx, js, "KV_"+name, name, "kv",
			keep, beforeSeq, &result)
	}
	return result
}

// addSeqDryRunEntry appends one estimated purge entry for a target stream.
func addSeqDryRunEntry(
	ctx context.Context,
	js jetstream.JetStream,
	streamName, displayName, kind string,
	keep, beforeSeq uint64,
	result *dryRunResult,
) {
	if js == nil {
		panic("addSeqDryRunEntry: js must not be nil")
	}
	if result == nil {
		panic("addSeqDryRunEntry: result must not be nil")
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return
	}
	// Execute skips work-queue streams (seqPurgeStreams), so the preview must
	// not report them as purgeable — otherwise the dry-run overstates what a
	// real run would remove.
	if info.Config.Retention == jetstream.WorkQueuePolicy {
		return
	}
	st := info.State
	est := seqPurgeEstimate(
		st.Msgs, st.FirstSeq, st.LastSeq, keep, beforeSeq)
	if est == 0 {
		return
	}
	result.Entries = append(result.Entries, dryRunEntry{
		Name:     displayName,
		Kind:     kind,
		Messages: est,
	})
	result.TotalMsgs += est
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
		"  --keep=<n>           " +
			"bulk-prune each target to the newest n messages")
	fmt.Println(
		"  --before-seq=<n>     " +
			"bulk-prune messages below stream sequence n")
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
	fmt.Println()
	fmt.Println("Retention & bulk prune:")
	fmt.Println(
		"  Bucket size grows with run rate x retention. --older-than is")
	fmt.Println(
		"  the safe default: it prunes by age and skips live work. If a")
	fmt.Println(
		"  bucket (e.g. KV_workflow_runs) has already cliffed and cannot")
	fmt.Println(
		"  be drained per-key, --keep/--before-seq purge server-side by")
	fmt.Println(
		"  stream sequence in one round trip, regardless of size.")
	fmt.Println(
		"  Recovery recipe:  dagnats clean --type=runs --keep=30000 --force")
	fmt.Println(
		"  Caution: sequence prunes ignore run age/status and can evict")
	fmt.Println(
		"  live in-flight runs; prefer --older-than for routine cleanup.")
}

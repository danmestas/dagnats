// cli/clean.go
// Purge run data from streams and runtime KV buckets. Preserves
// workflow definitions and telemetry by default. Intended for
// development and testing — not production use.
package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// cleanableStreams are purged by default. These hold run-scoped
// data that accumulates during testing.
var cleanableStreams = []string{
	"WORKFLOW_HISTORY",
	"TASK_QUEUES",
	"EVENTS",
	"DEAD_LETTERS",
	"SLEEP_TIMERS",
}

// cleanableBuckets are cleared by default. These hold runtime
// state that should be reset between test runs.
var cleanableBuckets = []string{
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
}

// extraStreams are only purged with --all.
var extraStreams = []string{
	"TELEMETRY",
}

// extraBuckets are only cleared with --all.
var extraBuckets = []string{
	"workflow_defs",
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

	all := hasFlag(args, "--all")
	force := hasFlag(args, "--force")
	jsonOutput := HasJSONFlag(args)

	if !force && !jsonOutput {
		fmt.Print("This will purge all run data. Continue? [y/N] ")
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

	result := executeClean(js, all)

	if jsonOutput {
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
	js jetstream.JetStream, all bool,
) cleanResult {
	if js == nil {
		panic("executeClean: js must not be nil")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	var result cleanResult

	streams := cleanableStreams
	if all {
		streams = append(streams, extraStreams...)
	}
	result.Streams = purgeStreams(ctx, js, streams)

	buckets := cleanableBuckets
	if all {
		buckets = append(buckets, extraBuckets...)
	}
	cleared, errs := clearBuckets(ctx, js, buckets)
	result.Buckets = cleared
	result.Errors = errs

	return result
}

// purgeStreams purges each named stream. Returns count of
// successfully purged streams. Skips missing streams silently.
func purgeStreams(
	ctx context.Context,
	js jetstream.JetStream,
	names []string,
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
		if err := stream.Purge(ctx); err != nil {
			fmt.Fprintf(os.Stderr,
				"warn: purge %s: %v\n", name, err)
			continue
		}
		purged++
	}
	return purged
}

// clearBuckets deletes all keys from each KV bucket. Returns
// count of cleared buckets and count of errors.
func clearBuckets(
	ctx context.Context,
	js jetstream.JetStream,
	names []string,
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
		if err := purgeKVBucket(ctx, kv); err != nil {
			fmt.Fprintf(os.Stderr,
				"warn: clear %s: %v\n", name, err)
			errors++
			continue
		}
		cleared++
	}
	return cleared, errors
}

// purgeKVBucket deletes all keys in a KV bucket.
func purgeKVBucket(
	ctx context.Context, kv jetstream.KeyValue,
) error {
	if kv == nil {
		panic("purgeKVBucket: kv must not be nil")
	}

	keys, err := kv.Keys(ctx)
	if err != nil {
		return nil
	}

	const maxKeys = 100_000
	for i, key := range keys {
		if i >= maxKeys {
			break
		}
		if err := kv.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete key %s: %w", key, err)
		}
	}
	return nil
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
	fmt.Println("Usage: dagnats clean [--all] [--force] [--json]")
	fmt.Println()
	fmt.Println("Purge all run data from streams and KV buckets.")
	fmt.Println("Preserves workflow definitions and telemetry by default.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --all    also clear workflow definitions and telemetry")
	fmt.Println("  --force  skip confirmation prompt")
	fmt.Println("  --json   output result as JSON")
}

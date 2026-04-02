// cli/dlq_watch.go
// Continuous DLQ watcher that auto-replays dead-letter messages on a
// configurable interval with bounded retry tracking.
package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/api"
)

// maxTrackedSequences bounds the in-memory replay tracker map.
// Exceeding this indicates an operational problem (DLQ backlog too large).
const maxTrackedSequences = 10000

// replayTracker tracks how many times each DLQ entry has been replayed.
// Bounded to maxTrackedSequences entries to prevent unbounded growth.
type replayTracker struct {
	counts     map[uint64]int
	maxReplays int
}

// newReplayTracker creates a tracker with the given max replays per entry.
func newReplayTracker(maxReplays int) *replayTracker {
	if maxReplays <= 0 {
		panic("newReplayTracker: maxReplays must be positive")
	}
	if maxReplays > 1000 {
		panic("newReplayTracker: maxReplays exceeds upper bound")
	}
	return &replayTracker{
		counts:     make(map[uint64]int),
		maxReplays: maxReplays,
	}
}

// shouldReplay returns true if the entry hasn't hit the replay limit.
func (t *replayTracker) shouldReplay(seq uint64) bool {
	if t.counts == nil {
		panic("replayTracker.shouldReplay: counts map is nil")
	}
	if seq == 0 {
		panic("replayTracker.shouldReplay: seq must be > 0")
	}
	return t.counts[seq] < t.maxReplays
}

// record increments the replay count for a sequence. Panics if the
// tracker would exceed its bounded capacity.
func (t *replayTracker) record(seq uint64) {
	if t.counts == nil {
		panic("replayTracker.record: counts map is nil")
	}
	if _, exists := t.counts[seq]; !exists {
		if len(t.counts) >= maxTrackedSequences {
			panic(fmt.Sprintf(
				"replayTracker.record: exceeded %d tracked sequences",
				maxTrackedSequences))
		}
	}
	t.counts[seq]++
}

// exhausted returns how many entries have hit the max replay limit.
func (t *replayTracker) exhausted() int {
	if t.counts == nil {
		panic("replayTracker.exhausted: counts map is nil")
	}
	const maxIter = maxTrackedSequences + 1
	count := 0
	iter := 0
	for _, c := range t.counts {
		iter++
		if iter > maxIter {
			panic("replayTracker.exhausted: iteration exceeded bound")
		}
		if c >= t.maxReplays {
			count++
		}
	}
	return count
}

// FormatDLQWatchAction writes a human-readable replay action line.
func FormatDLQWatchAction(
	w io.Writer, letter api.DeadLetter,
	attempt int, maxReplays int,
) {
	if w == nil {
		panic("FormatDLQWatchAction: writer must not be nil")
	}
	if attempt <= 0 {
		panic("FormatDLQWatchAction: attempt must be positive")
	}
	fmt.Fprintf(w, "[replay %d/%d] seq=%d task=%s run=%s err=%s\n",
		attempt, maxReplays, letter.Sequence,
		letter.Task, letter.RunID, letter.Error)
}

// FormatDLQWatchActionSkipped writes a skip line for exhausted entries.
func FormatDLQWatchActionSkipped(
	w io.Writer, letter api.DeadLetter, maxReplays int,
) {
	if w == nil {
		panic("FormatDLQWatchActionSkipped: writer must not be nil")
	}
	if maxReplays <= 0 {
		panic(
			"FormatDLQWatchActionSkipped: maxReplays must be positive",
		)
	}
	fmt.Fprintf(w,
		"[skip exhausted %d/%d] seq=%d task=%s run=%s\n",
		maxReplays, maxReplays, letter.Sequence,
		letter.Task, letter.RunID)
}

// FormatDLQWatchSummary writes the final summary on shutdown.
func FormatDLQWatchSummary(
	w io.Writer, totalReplayed int, totalExhausted int,
) {
	if w == nil {
		panic("FormatDLQWatchSummary: writer must not be nil")
	}
	if totalReplayed < 0 {
		panic("FormatDLQWatchSummary: totalReplayed must be >= 0")
	}
	fmt.Fprintf(w,
		"\nWatch summary: %d replayed, %d exhausted\n",
		totalReplayed, totalExhausted)
}

// dlqWatchActionJSON is the JSON envelope for a single watch action.
type dlqWatchActionJSON struct {
	Action   string `json:"action"`
	Sequence uint64 `json:"sequence"`
	Task     string `json:"task"`
	RunID    string `json:"run_id"`
	Attempt  int    `json:"attempt,omitempty"`
}

// FormatDLQWatchActionJSON writes a JSON line for a watch action.
func FormatDLQWatchActionJSON(
	w io.Writer, letter api.DeadLetter,
	action string, attempt int,
) {
	if w == nil {
		panic("FormatDLQWatchActionJSON: writer must not be nil")
	}
	if action == "" {
		panic("FormatDLQWatchActionJSON: action must not be empty")
	}
	entry := dlqWatchActionJSON{
		Action:   action,
		Sequence: letter.Sequence,
		Task:     letter.Task,
		RunID:    letter.RunID,
		Attempt:  attempt,
	}
	// Error handled: FormatJSON panics on nil writer/value.
	if err := FormatJSON(w, entry); err != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", err)
	}
}

// dlqWatchSummaryJSON is the JSON envelope for the final summary.
type dlqWatchSummaryJSON struct {
	TotalReplayed  int `json:"total_replayed"`
	TotalExhausted int `json:"total_exhausted"`
}

// FormatDLQWatchSummaryJSON writes the shutdown summary as JSON.
func FormatDLQWatchSummaryJSON(
	w io.Writer, totalReplayed int, totalExhausted int,
) {
	if w == nil {
		panic("FormatDLQWatchSummaryJSON: writer must not be nil")
	}
	if totalReplayed < 0 {
		panic(
			"FormatDLQWatchSummaryJSON: totalReplayed must be >= 0",
		)
	}
	entry := dlqWatchSummaryJSON{
		TotalReplayed:  totalReplayed,
		TotalExhausted: totalExhausted,
	}
	if err := FormatJSON(w, entry); err != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", err)
	}
}

// runDLQWatchCmd parses flags and starts the watch loop.
func runDLQWatchCmd(args []string) {
	if args == nil {
		panic("runDLQWatchCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runDLQWatchCmd: args exceeds max bound")
	}

	if HasHelpFlag(args) {
		printDLQWatchHelp()
		return
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	interval, maxReplays, runFilter := parseDLQWatchFlags(args)
	tracker := newReplayTracker(maxReplays)

	svc, nc := connectService()
	defer nc.Close()

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)
	defer stop()

	if !jsonOutput {
		fmt.Printf(
			"Watching DLQ every %s (max %d replays per message)\n",
			interval, maxReplays)
		if runFilter != "" {
			fmt.Printf("Filtering: run=%s\n", runFilter)
		}
	}

	totalReplayed := runDLQWatchLoop(
		ctx, svc, tracker, interval, runFilter, jsonOutput,
	)

	if jsonOutput {
		FormatDLQWatchSummaryJSON(
			os.Stdout, totalReplayed, tracker.exhausted(),
		)
	} else {
		FormatDLQWatchSummary(
			os.Stdout, totalReplayed, tracker.exhausted(),
		)
	}
}

// printDLQWatchHelp prints usage for the watch subcommand.
func printDLQWatchHelp() {
	fmt.Println("Usage: dagnats dlq watch [flags]")
	fmt.Println("Flags:")
	fmt.Println("  --interval=<duration>  poll interval (default 30s)")
	fmt.Println("  --max-replays=<n>      max replays per message" +
		" (default 3)")
	fmt.Println("  --run=<run-id>         filter by run ID")
	fmt.Println("  --json                 JSON output")
}

// parseDLQWatchFlags extracts watch-specific flags from args.
func parseDLQWatchFlags(
	args []string,
) (time.Duration, int, string) {
	if args == nil {
		panic("parseDLQWatchFlags: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("parseDLQWatchFlags: args exceeds max bound")
	}

	interval := 30 * time.Second
	maxReplays := 3
	var runFilter string

	for _, arg := range args {
		if strings.HasPrefix(arg, "--interval=") {
			val := strings.TrimPrefix(arg, "--interval=")
			parsed, err := time.ParseDuration(val)
			if err != nil {
				fmt.Fprintf(os.Stderr,
					"invalid --interval: %v\n", err)
				exitFunc(1)
				return 0, 0, ""
			}
			interval = parsed
		}
		if strings.HasPrefix(arg, "--max-replays=") {
			val := strings.TrimPrefix(arg, "--max-replays=")
			parsed, err := strconv.Atoi(val)
			if err != nil {
				fmt.Fprintf(os.Stderr,
					"invalid --max-replays: %v\n", err)
				exitFunc(1)
				return 0, 0, ""
			}
			if parsed <= 0 {
				fmt.Fprintln(os.Stderr,
					"--max-replays must be positive")
				exitFunc(1)
				return 0, 0, ""
			}
			maxReplays = parsed
		}
		if strings.HasPrefix(arg, "--run=") {
			runFilter = strings.TrimPrefix(arg, "--run=")
		}
	}

	return interval, maxReplays, runFilter
}

// runDLQWatchLoop polls the DLQ and replays eligible entries.
// Returns total number of successful replays.
func runDLQWatchLoop(
	ctx context.Context, svc *api.Service,
	tracker *replayTracker, interval time.Duration,
	runFilter string, jsonOutput bool,
) int {
	if svc == nil {
		panic("runDLQWatchLoop: svc must not be nil")
	}
	if tracker == nil {
		panic("runDLQWatchLoop: tracker must not be nil")
	}

	totalReplayed := 0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run once immediately before first tick.
	totalReplayed += pollAndReplay(
		ctx, svc, tracker, runFilter, jsonOutput,
	)

	const maxTicks = 1_000_000
	for tick := 0; tick < maxTicks; tick++ {
		select {
		case <-ctx.Done():
			return totalReplayed
		case <-ticker.C:
			totalReplayed += pollAndReplay(
				ctx, svc, tracker, runFilter, jsonOutput,
			)
		}
	}
	return totalReplayed
}

// pollAndReplay fetches DLQ entries and replays eligible ones.
// Returns the number of entries replayed in this poll cycle.
func pollAndReplay(
	ctx context.Context, svc *api.Service,
	tracker *replayTracker, runFilter string,
	jsonOutput bool,
) int {
	if svc == nil {
		panic("pollAndReplay: svc must not be nil")
	}
	if tracker == nil {
		panic("pollAndReplay: tracker must not be nil")
	}

	const maxFetch = 100
	letters, err := svc.ListDeadLetters(ctx, maxFetch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list dead letters: %v\n", err)
		return 0
	}

	if runFilter != "" {
		letters = filterByRun(letters, runFilter)
	}

	replayed := 0
	for _, letter := range letters {
		if !tracker.shouldReplay(letter.Sequence) {
			if jsonOutput {
				FormatDLQWatchActionJSON(
					os.Stdout, letter, "exhausted", 0)
			} else {
				FormatDLQWatchActionSkipped(
					os.Stdout, letter, tracker.maxReplays)
			}
			continue
		}

		replayErr := svc.ReplayDeadLetter(ctx, letter.Sequence)
		if replayErr != nil {
			fmt.Fprintf(os.Stderr,
				"replay seq %d: %v\n", letter.Sequence, replayErr)
			continue
		}

		tracker.record(letter.Sequence)
		replayed++
		attempt := tracker.counts[letter.Sequence]

		if jsonOutput {
			FormatDLQWatchActionJSON(
				os.Stdout, letter, "replayed", attempt)
		} else {
			FormatDLQWatchAction(
				os.Stdout, letter, attempt, tracker.maxReplays)
		}
	}
	return replayed
}

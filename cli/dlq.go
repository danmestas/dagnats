// cli/dlq.go
// Commands for managing dead-letter queue: list, replay.
package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/danmestas/dagnats/internal/api"
)

// dlqListSafetyCap caps the default `dagnats dlq list` output so a
// runaway DLQ doesn't dump megabytes to the operator's terminal.
// Operators bypass with `--limit N` or `--all`. The cap is a CLI
// concern only — the service layer accepts any positive limit.
const dlqListSafetyCap = 500

// runDLQCmd dispatches DLQ subcommands.
func runDLQCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats dlq <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  list     list dead-letter messages " +
			"(default up to 500; --all for everything)")
		fmt.Println("  replay   replay a dead-letter message")
		fmt.Println("  watch    auto-replay dead letters on interval")
		return
	}
	if len(args) == 0 {
		fmt.Println("Usage: dagnats dlq <list|replay|watch> [--json]")
		return
	}
	switch args[0] {
	case "list":
		runDLQListCmd(args[1:])
	case "replay":
		runDLQReplayCmd(args[1:])
	case "watch":
		runDLQWatchCmd(args[1:])
	default:
		fmt.Printf("unknown dlq subcommand: %s\n", args[0])
	}
}

// dlqListFlags is the parsed shape of `dagnats dlq list` flags.
// `limitSet` distinguishes "operator passed --limit" from "default":
// the truncation footer fires only when the *effective* result is
// smaller than the stream-total, so the default safety cap and an
// explicit smaller --limit both produce a footer when appropriate.
type dlqListFlags struct {
	runFilter string
	limit     int  // effective limit fed to the service layer
	limitSet  bool // operator supplied --limit explicitly
	all       bool // operator supplied --all
}

// runDLQListCmd lists dead-letter messages with optional filters.
// Default returns up to dlqListSafetyCap entries; `--all` returns
// everything; `--limit N` overrides the default. When the result
// is smaller than the stream-total, a footer is written to stderr
// (not stdout — keeps JSON parseable).
func runDLQListCmd(args []string) {
	if args == nil {
		panic("runDLQListCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runDLQListCmd: args exceeds max bound")
	}
	if HasHelpFlag(args) {
		printDLQListHelp()
		return
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	flags := parseDLQListFlags(args)

	svc, nc := connectService()
	defer nc.Close()

	ctx := context.Background()
	streamTotal, countErr := svc.CountDeadLetters(ctx)
	if countErr != nil {
		fmt.Fprintf(os.Stderr,
			"count dead letters: %v\n", countErr)
		os.Exit(1)
	}

	effLimit := resolveDLQListLimit(flags, streamTotal)
	letters, err := fetchLetters(svc, effLimit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list dead letters: %v\n", err)
		os.Exit(1)
	}
	if flags.runFilter != "" {
		letters = filterByRun(letters, flags.runFilter)
	}

	renderDLQList(letters, jsonOutput)
	if len(letters) < streamTotal {
		fmt.Fprintf(os.Stderr,
			"# truncated: %d entries in stream, %d shown — "+
				"use --limit N or --all to see more\n",
			streamTotal, len(letters))
	}
}

// resolveDLQListLimit converts parsed flags + stream-total into the
// effective limit to send to the service. The service rejects limit<=0,
// so when the stream is empty we return 1 — the service still returns
// no rows (no entries to read), but the call stays valid. Otherwise
// `--all` returns stream-total verbatim; `--limit N` overrides; default
// returns min(total, dlqListSafetyCap).
func resolveDLQListLimit(f dlqListFlags, streamTotal int) int {
	if streamTotal <= 0 {
		return 1
	}
	if f.all {
		return streamTotal
	}
	if f.limitSet {
		return f.limit
	}
	if streamTotal < dlqListSafetyCap {
		return streamTotal
	}
	return dlqListSafetyCap
}

// fetchLetters wraps svc.ListDeadLetters so runDLQListCmd stays under
// the 70-line ceiling.
func fetchLetters(
	svc dlqService, effLimit int,
) ([]api.DeadLetterView, error) {
	if effLimit <= 0 {
		panic("fetchLetters: effLimit must be positive")
	}
	return svc.ListDeadLetters(context.Background(), effLimit)
}

// dlqService is the slice of api.Service the dlq command actually uses.
// Declared so the fetchLetters split stays test-friendly without
// reaching for the full Service surface.
type dlqService interface {
	ListDeadLetters(
		ctx context.Context, limit int,
	) ([]api.DeadLetterView, error)
}

// renderDLQList prints the rows to stdout in the chosen format. Empty
// result paths are handled here so the caller stays linear.
func renderDLQList(letters []api.DeadLetterView, jsonOutput bool) {
	if jsonOutput {
		if err := FormatJSON(os.Stdout, letters); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(letters) == 0 {
		fmt.Println("No dead letters found.")
		return
	}
	printDLQTable(letters)
}

// printDLQListHelp documents the `dagnats dlq list` flags and columns.
func printDLQListHelp() {
	fmt.Println("Usage: dagnats dlq list [flags]")
	fmt.Println("Flags:")
	fmt.Println("  --limit N    show at most N entries " +
		"(default: up to 500; rejects --limit 0)")
	fmt.Println("  --all        return every entry " +
		"(bypasses the safety cap)")
	fmt.Println("  --run=ID     filter by run id")
	fmt.Println("  --json       emit a JSON array on stdout")
	fmt.Println()
	fmt.Println("Truncation: if the stream holds more entries than " +
		"shown, a footer is written to stderr.")
	fmt.Println("Columns: SEQ, SUBJECT, RUN_ID, STEP_ID, TASK, BODY, " +
		"DELIVERY, CONSUMER, ERROR, TIMESTAMP.")
	fmt.Println("JSON fields include delivery_count and consumer for " +
		"max-deliver-only DLQ writes.")
}

// parseDLQListFlags extracts --run, --limit, and --all from args.
// `--limit 0` is rejected explicitly so `--all` stays the only
// no-limit idiom — matches the behavior contract in the #203 brief.
func parseDLQListFlags(args []string) dlqListFlags {
	if args == nil {
		panic("parseDLQListFlags: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("parseDLQListFlags: args exceeds max bound")
	}

	var f dlqListFlags
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--all":
			f.all = true
		case strings.HasPrefix(arg, "--run="):
			f.runFilter = strings.TrimPrefix(arg, "--run=")
		case strings.HasPrefix(arg, "--limit="):
			f.limit, f.limitSet =
				parseLimitValue(strings.TrimPrefix(arg, "--limit=")),
				true
		case arg == "--limit":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr,
					"--limit requires a value")
				os.Exit(1)
			}
			i++
			f.limit, f.limitSet = parseLimitValue(args[i]), true
		}
	}
	if f.all && f.limitSet {
		fmt.Fprintln(os.Stderr,
			"--all and --limit are mutually exclusive")
		os.Exit(1)
	}
	return f
}

// parseLimitValue parses a --limit value, rejecting non-positive
// inputs. `--limit 0` is explicitly rejected so `--all` is the only
// "no limit" idiom.
func parseLimitValue(raw string) int {
	val, parseErr := strconv.Atoi(raw)
	if parseErr != nil {
		fmt.Fprintf(os.Stderr,
			"invalid --limit value: %v\n", parseErr)
		os.Exit(1)
	}
	if val <= 0 {
		fmt.Fprintln(os.Stderr,
			"--limit must be positive; use --all for no limit")
		os.Exit(1)
	}
	return val
}

// filterByRun returns only letters matching the given run ID.
func filterByRun(
	letters []api.DeadLetterView, runID string,
) []api.DeadLetterView {
	if runID == "" {
		panic("filterByRun: runID must not be empty")
	}
	const maxLetters = 10000
	if len(letters) > maxLetters {
		panic("filterByRun: letters exceeds max bound")
	}

	filtered := make([]api.DeadLetterView, 0, len(letters))
	for _, l := range letters {
		if l.RunID == runID {
			filtered = append(filtered, l)
		}
	}
	return filtered
}

// printDLQTable renders dead letters as a tab-aligned table.
// BODY column surfaces whether the entry's stored body is preserved
// (Y) — only such entries are replayable; legacy entries show "-".
// DELIVERY shows the JetStream redelivery count at the moment of DLQ
// publish; CONSUMER names the consumer that delivered the message.
// Both are empty ("-") for legacy entries that pre-date the metadata
// capture path.
func printDLQTable(letters []api.DeadLetterView) {
	if letters == nil {
		panic("printDLQTable: letters must not be nil")
	}
	const maxLetters = 10000
	if len(letters) > maxLetters {
		panic("printDLQTable: letters exceeds max bound")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w,
		"SEQ\tSUBJECT\tRUN_ID\tSTEP_ID\tTASK\tBODY\t"+
			"DELIVERY\tCONSUMER\tERROR\tTIMESTAMP")

	for _, letter := range letters {
		ts := letter.Timestamp.Format("2006-01-02 15:04:05")
		body := "-"
		if letter.BodyPreserved {
			body = "Y"
		}
		delivery := "-"
		if letter.DeliveryCount > 0 {
			delivery = strconv.Itoa(letter.DeliveryCount)
		}
		consumer := "-"
		if letter.Consumer != "" {
			consumer = letter.Consumer
		}
		fmt.Fprintf(w,
			"%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			letter.Sequence, letter.Subject, letter.RunID,
			letter.StepID, letter.Task, body,
			delivery, consumer, letter.Error, ts)
	}

	w.Flush()
}

// runDLQReplayCmd replays dead-letter messages by sequence or run.
func runDLQReplayCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats dlq replay "+
				"<sequence-number> | --run=<run-id> [--json]")
		os.Exit(1)
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	// Batch replay by run ID
	var runFilter string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--run=") {
			runFilter = strings.TrimPrefix(arg, "--run=")
		}
	}
	if runFilter != "" {
		replayByRun(runFilter, jsonOutput)
		return
	}

	replayBySequence(args[0], jsonOutput)
}

// dlqReplayResult is the JSON response for single replay.
type dlqReplayResult struct {
	Sequence uint64 `json:"sequence"`
	Replayed bool   `json:"replayed"`
}

// dlqBatchResult is the JSON response for batch replay.
type dlqBatchResult struct {
	RunID     string   `json:"run_id"`
	Replayed  int      `json:"replayed"`
	Sequences []uint64 `json:"sequences"`
}

// replayBySequence replays a single dead letter by sequence number.
func replayBySequence(seqStr string, jsonOutput bool) {
	if seqStr == "" {
		panic("replayBySequence: seqStr must not be empty")
	}

	seqNum, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"invalid sequence number: %v\n", err)
		os.Exit(1)
	}
	if seqNum == 0 {
		panic("replayBySequence: sequence must be > 0")
	}

	svc, nc := connectService()
	defer nc.Close()

	err = svc.ReplayDeadLetter(context.Background(), seqNum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay dead letter: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		result := dlqReplayResult{
			Sequence: seqNum, Replayed: true,
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Replayed dead letter %d\n", seqNum)
}

// replayByRun replays all dead letters matching a run ID.
func replayByRun(runID string, jsonOutput bool) {
	if runID == "" {
		panic("replayByRun: runID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	const maxFetch = 100
	ctx := context.Background()
	letters, err := svc.ListDeadLetters(ctx, maxFetch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list dead letters: %v\n", err)
		os.Exit(1)
	}

	replayed := 0
	sequences := make([]uint64, 0, len(letters))
	for _, dl := range letters {
		if dl.RunID != runID {
			continue
		}
		replayErr := svc.ReplayDeadLetter(ctx, dl.Sequence)
		if replayErr != nil {
			fmt.Fprintf(os.Stderr,
				"replay seq %d: %v\n", dl.Sequence, replayErr)
			continue
		}
		replayed++
		sequences = append(sequences, dl.Sequence)
		if !jsonOutput {
			fmt.Printf("Replayed dead letter %d (%s)\n",
				dl.Sequence, dl.Task)
		}
	}

	if jsonOutput {
		result := dlqBatchResult{
			RunID:     runID,
			Replayed:  replayed,
			Sequences: sequences,
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if replayed == 0 {
		fmt.Println("No dead letters found for run.")
	} else {
		fmt.Printf("Replayed %d dead letters for run %s\n",
			replayed, runID)
	}
}

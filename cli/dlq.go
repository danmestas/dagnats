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

// runDLQCmd dispatches DLQ subcommands.
func runDLQCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats dlq <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  list     list dead-letter messages")
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

// runDLQListCmd lists dead-letter messages with optional filters.
func runDLQListCmd(args []string) {
	if args == nil {
		panic("runDLQListCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runDLQListCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	runFilter, limit := parseDLQListFlags(args)

	svc, nc := connectService()
	defer nc.Close()

	letters, err := svc.ListDeadLetters(
		context.Background(), limit,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list dead letters: %v\n", err)
		os.Exit(1)
	}

	if runFilter != "" {
		letters = filterByRun(letters, runFilter)
	}

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

// parseDLQListFlags extracts --run and --limit from args.
func parseDLQListFlags(args []string) (string, int) {
	if args == nil {
		panic("parseDLQListFlags: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("parseDLQListFlags: args exceeds max bound")
	}

	var runFilter string
	limit := 50
	for _, arg := range args {
		if strings.HasPrefix(arg, "--run=") {
			runFilter = strings.TrimPrefix(arg, "--run=")
		}
		if strings.HasPrefix(arg, "--limit=") {
			val, parseErr := strconv.Atoi(
				strings.TrimPrefix(arg, "--limit="),
			)
			if parseErr != nil {
				fmt.Fprintf(os.Stderr,
					"invalid --limit value: %v\n", parseErr)
				os.Exit(1)
			}
			if val <= 0 {
				fmt.Fprintln(os.Stderr,
					"--limit must be positive")
				os.Exit(1)
			}
			limit = val
		}
	}
	return runFilter, limit
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
		"SEQ\tSUBJECT\tRUN_ID\tSTEP_ID\tTASK\tBODY\tERROR\tTIMESTAMP")

	for _, letter := range letters {
		ts := letter.Timestamp.Format("2006-01-02 15:04:05")
		body := "-"
		if letter.BodyPreserved {
			body = "Y"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			letter.Sequence, letter.Subject, letter.RunID,
			letter.StepID, letter.Task, body, letter.Error, ts)
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

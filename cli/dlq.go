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
)

// runDLQCmd dispatches DLQ subcommands.
func runDLQCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats dlq <command>")
		fmt.Println("Commands:")
		fmt.Println("  list     list dead-letter messages")
		fmt.Println("  replay   replay a dead-letter message")
		return
	}
	if len(args) == 0 {
		fmt.Println("Usage: dagnats dlq <list|replay>")
		return
	}
	switch args[0] {
	case "list":
		runDLQListCmd(args[1:])
	case "replay":
		runDLQReplayCmd(args[1:])
	default:
		fmt.Printf("unknown dlq subcommand: %s\n", args[0])
	}
}

// runDLQListCmd lists dead-letter messages with optional filters.
func runDLQListCmd(args []string) {
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

	svc, nc := connectService()
	defer nc.Close()

	letters, err := svc.ListDeadLetters(
		context.Background(), limit,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list dead letters: %v\n", err)
		os.Exit(1)
	}

	// Apply run filter client-side
	if runFilter != "" {
		filtered := letters[:0]
		for _, l := range letters {
			if l.RunID == runFilter {
				filtered = append(filtered, l)
			}
		}
		letters = filtered
	}

	if len(letters) == 0 {
		fmt.Println("No dead letters found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w,
		"SEQ\tSUBJECT\tRUN_ID\tSTEP_ID\tTASK\tERROR\tTIMESTAMP")

	for _, letter := range letters {
		ts := letter.Timestamp.Format("2006-01-02 15:04:05")
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			letter.Sequence, letter.Subject, letter.RunID,
			letter.StepID, letter.Task, letter.Error, ts)
	}

	w.Flush()
}

// runDLQReplayCmd replays dead-letter messages by sequence or run.
func runDLQReplayCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats dlq replay "+
				"<sequence-number> | --run=<run-id>")
		os.Exit(1)
	}

	// Batch replay by run ID
	var runFilter string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--run=") {
			runFilter = strings.TrimPrefix(arg, "--run=")
		}
	}
	if runFilter != "" {
		replayByRun(runFilter)
		return
	}

	replayBySequence(args[0])
}

// replayBySequence replays a single dead letter by sequence number.
func replayBySequence(seqStr string) {
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

	fmt.Printf("Replayed dead letter %d\n", seqNum)
}

// replayByRun replays all dead letters matching a run ID.
func replayByRun(runID string) {
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
	for _, dl := range letters {
		if dl.RunID != runID {
			continue
		}
		if err := svc.ReplayDeadLetter(ctx, dl.Sequence); err != nil {
			fmt.Fprintf(os.Stderr,
				"replay seq %d: %v\n", dl.Sequence, err)
			continue
		}
		replayed++
		fmt.Printf("Replayed dead letter %d (%s)\n",
			dl.Sequence, dl.Task)
	}

	if replayed == 0 {
		fmt.Println("No dead letters found for run.")
	} else {
		fmt.Printf("Replayed %d dead letters for run %s\n",
			replayed, runID)
	}
}

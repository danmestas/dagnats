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

// runDLQListCmd lists dead-letter messages with optional --run filter.
func runDLQListCmd(args []string) {
	var runFilter string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--run=") {
			runFilter = strings.TrimPrefix(arg, "--run=")
		}
	}

	svc, nc := connectService()
	defer nc.Close()

	letters, err := svc.ListDeadLetters(context.Background(), 50)
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

// runDLQReplayCmd replays a dead-letter message via api.Service.
func runDLQReplayCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats dlq replay <sequence-number>")
		os.Exit(1)
	}

	seqNum, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid sequence number: %v\n", err)
		os.Exit(1)
	}
	if seqNum == 0 {
		panic("runDLQReplayCmd: sequence number must be > 0")
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

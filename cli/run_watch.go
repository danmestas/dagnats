// cli/run_watch.go
// Attaches to an existing workflow run and tails its events in real time.
// Delegates to watchRun which handles polling and terminal status detection.
package cli

import (
	"context"
	"fmt"
	"os"
)

// runWatchCmd attaches to an existing run and tails its events.
func runWatchCmd(args []string) {
	if args == nil {
		panic("runWatchCmd: args must not be nil")
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run watch <run-id>")
		os.Exit(1)
	}

	runID := args[0]
	if runID == "" {
		panic("runWatchCmd: runID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	// Verify the run exists before starting the watch loop.
	_, err := svc.GetRun(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		os.Exit(1)
	}

	watchRun(svc, runID)
}

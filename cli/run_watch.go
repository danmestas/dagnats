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

	hasLast := HasLastFlag(args)
	args = StripLastFlag(args)

	var rawID string
	if len(args) == 1 {
		rawID = args[0]
	} else if !hasLast {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run watch"+
				" <run-id> [--last]")
		os.Exit(1)
	}

	svc, nc := connectService()
	defer nc.Close()

	runID, err := ResolveRunID(svc, rawID, hasLast)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve run: %v\n", err)
		os.Exit(1)
	}

	// Verify the run exists before starting the watch loop.
	_, err = svc.GetRun(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		os.Exit(1)
	}

	watchRun(svc, runID)
}

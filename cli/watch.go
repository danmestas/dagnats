// cli/watch.go
// Polls a workflow run until it reaches a terminal state, printing
// new events as they arrive. Used by `run start --watch`.
package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
)

const (
	watchPollInterval = 1 * time.Second
	watchTimeout      = 30 * time.Minute
)

// watchRun polls for events and status until the run completes, fails,
// or is cancelled. Prints each new event as it arrives.
func watchRun(svc *api.Service, runID string) {
	if svc == nil {
		panic("watchRun: svc must not be nil")
	}
	if runID == "" {
		panic("watchRun: runID must not be empty")
	}

	fmt.Println()
	seen := 0
	deadline := time.Now().Add(watchTimeout)

	for time.Now().Before(deadline) {
		events, status := pollRunState(svc, runID, seen)
		for _, evt := range events {
			printWatchEvent(evt)
		}
		seen += len(events)

		if isTerminalStatus(status) {
			fmt.Printf("\nRun %s: %s\n", status.String(), runID)
			return
		}

		time.Sleep(watchPollInterval)
	}

	fmt.Fprintln(os.Stderr, "watch: timed out after 30m")
}

// pollRunState fetches new events and the current run status.
func pollRunState(
	svc *api.Service, runID string, seen int,
) ([]api.RunEvent, dag.RunStatus) {
	if svc == nil {
		panic("pollRunState: svc must not be nil")
	}
	if seen < 0 {
		panic("pollRunState: seen must not be negative")
	}

	ctx := context.Background()
	events, err := svc.ListRunEvents(ctx, runID, false)
	if err != nil {
		return nil, dag.RunStatusPending
	}

	// Only return events we haven't printed yet.
	var newEvents []api.RunEvent
	if len(events) > seen {
		newEvents = events[seen:]
	}

	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		return newEvents, dag.RunStatusRunning
	}
	return newEvents, run.Status
}

// printWatchEvent prints a single event line to stdout.
func printWatchEvent(evt api.RunEvent) {
	step := evt.StepID
	if step == "" {
		step = "-"
	}
	fmt.Printf("  %s  %-24s %s\n",
		evt.Timestamp.Format("15:04:05"), evt.Type, step)
}

// isTerminalStatus returns true for completed, failed, or cancelled.
func isTerminalStatus(status dag.RunStatus) bool {
	return status == dag.RunStatusCompleted ||
		status == dag.RunStatusFailed ||
		status == dag.RunStatusCancelled
}

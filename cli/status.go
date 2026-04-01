// cli/status.go
// Command for checking DagNats system health from the CLI.
// Connects to NATS and reports connection, JetStream, and run status.
package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/danmestas/dagnats/dag"
)

// runSystemStatusCmd checks system health and prints a summary. It verifies
// NATS connectivity, JetStream availability, and counts active workflow runs.
func runSystemStatusCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats status")
		fmt.Println(
			"Shows system health: NATS, JetStream, active runs.")
		return
	}
	if len(args) > 0 {
		fmt.Println("Usage: dagnats status")
		fmt.Println(
			"Shows system health: NATS, JetStream, active runs.")
		return
	}

	svc, nc := connectService()
	defer nc.Close()

	// Connection health — if connectService succeeded nc is non-nil,
	// but verify the connection is still alive.
	if !nc.IsConnected() {
		fmt.Fprintln(os.Stderr, "NATS:        disconnected")
		os.Exit(1)
	}
	fmt.Println("NATS:        connected")

	// JetStream availability and stream count.
	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream:   unavailable (%v)\n", err)
		os.Exit(1)
	}
	info, err := js.AccountInfo()
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream:   unavailable (%v)\n", err)
		os.Exit(1)
	}
	if info == nil {
		panic("runSystemStatusCmd: AccountInfo returned nil without error")
	}
	fmt.Printf("JetStream:   available (%d streams)\n", info.Streams)

	// Count active runs (pending + running).
	runs, err := svc.ListRuns(context.Background(), "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Active runs: error (%v)\n", err)
		os.Exit(1)
	}

	activeCount := countActiveRuns(runs)
	fmt.Printf("Active runs: %d\n", activeCount)

	// Detailed stream and run breakdown for richer status output.
	printStreamDetails(js)
	printRunBreakdown(svc)
}

// countActiveRuns counts runs that are pending or running.
func countActiveRuns(runs []dag.WorkflowRun) int {
	if runs == nil {
		panic("countActiveRuns: runs must not be nil")
	}

	const maxRuns = 10000
	if len(runs) > maxRuns {
		panic("countActiveRuns: runs exceeds max bound")
	}

	count := 0
	for _, r := range runs {
		if r.Status == dag.RunStatusRunning ||
			r.Status == dag.RunStatusPending {
			count++
		}
	}
	return count
}

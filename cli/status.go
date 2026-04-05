// cli/status.go
// Command for checking DagNats system health from the CLI.
// Connects to NATS and reports connection, JetStream, and run status.
package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// systemStatus holds all health data for JSON serialization.
type systemStatus struct {
	NATS       string           `json:"nats"`
	JetStream  string           `json:"jetstream"`
	Streams    int              `json:"stream_count"`
	ActiveRuns int              `json:"active_runs"`
	StreamInfo []streamInfo     `json:"streams,omitempty"`
	Runs       map[string]int   `json:"runs,omitempty"`
	Workflows  []workflowMetric `json:"workflows,omitempty"`
	Queues     []queueHealth    `json:"queues,omitempty"`
	DLQ        *dlqSummary      `json:"dlq,omitempty"`
	Engine     *engineLag       `json:"engine,omitempty"`
}

// hasDetailFlag returns true if args contains "--detail".
func hasDetailFlag(args []string) bool {
	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("hasDetailFlag: args exceeds max bound")
	}
	for _, arg := range args {
		if arg == "--detail" {
			return true
		}
	}
	return false
}

// stripDetailFlag returns a copy of args without "--detail".
func stripDetailFlag(args []string) []string {
	if args == nil {
		panic("stripDetailFlag: args must not be nil")
	}
	result := make([]string, 0, len(args))
	for _, arg := range args {
		if arg != "--detail" {
			result = append(result, arg)
		}
	}
	return result
}

// runSystemStatusCmd checks system health and prints a summary.
// Supports --json and --detail for machine-readable or enriched output.
func runSystemStatusCmd(args []string) {
	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	detail := hasDetailFlag(args)
	args = stripDetailFlag(args)

	if HasHelpFlag(args) {
		fmt.Println(
			"Usage: dagnats status [--json] [--detail]")
		fmt.Println(
			"Shows system health: NATS, JetStream, active runs.")
		return
	}
	if len(args) > 0 {
		fmt.Println(
			"Usage: dagnats status [--json] [--detail]")
		fmt.Println(
			"Shows system health: NATS, JetStream, active runs.")
		return
	}

	svc, nc := connectService()
	defer nc.Close()

	if jsonOutput {
		status := collectSystemStatus(nc, svc)
		if detail {
			appendDetailToStatus(&status, nc)
		}
		err := FormatJSON(os.Stdout, status)
		if err != nil {
			fmt.Fprintf(os.Stderr, "json error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	printSystemStatus(nc, svc)
	if detail {
		printDetailSections(nc)
	}
}

// collectSystemStatus gathers all health data into a struct.
func collectSystemStatus(
	nc *nats.Conn, svc *api.Service,
) systemStatus {
	if nc == nil {
		panic("collectSystemStatus: nc must not be nil")
	}
	if svc == nil {
		panic("collectSystemStatus: svc must not be nil")
	}

	status := systemStatus{NATS: "disconnected"}
	if !nc.IsConnected() {
		return status
	}
	status.NATS = "connected"

	js, err := jetstream.New(nc)
	if err != nil {
		status.JetStream = "unavailable"
		return status
	}
	info, err := js.AccountInfo(context.Background())
	if err != nil {
		status.JetStream = "unavailable"
		return status
	}
	if info == nil {
		panic("collectSystemStatus: AccountInfo nil without error")
	}
	status.JetStream = "available"
	status.Streams = info.Streams
	status.StreamInfo = collectStreamInfo(js)

	runs, err := svc.ListRuns(context.Background(), "")
	if err != nil {
		return status
	}
	status.ActiveRuns = countActiveRuns(runs)

	runCounts, err := collectRunCountMap(svc)
	if err == nil {
		status.Runs = runCounts
	}

	wfMetrics, err := collectWorkflowMetrics(svc)
	if err == nil {
		status.Workflows = wfMetrics
	}
	return status
}

// printSystemStatus outputs human-readable health information.
func printSystemStatus(nc *nats.Conn, svc *api.Service) {
	if nc == nil {
		panic("printSystemStatus: nc must not be nil")
	}
	if svc == nil {
		panic("printSystemStatus: svc must not be nil")
	}

	if !nc.IsConnected() {
		fmt.Fprintln(os.Stderr, "NATS:        disconnected")
		os.Exit(1)
	}
	fmt.Println("NATS:        connected")

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"JetStream:   unavailable (%v)\n", err)
		os.Exit(1)
	}
	info, err := js.AccountInfo(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"JetStream:   unavailable (%v)\n", err)
		os.Exit(1)
	}
	if info == nil {
		panic("printSystemStatus: AccountInfo nil without error")
	}
	fmt.Printf("JetStream:   available (%d streams)\n",
		info.Streams)

	runs, err := svc.ListRuns(context.Background(), "")
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Active runs: error (%v)\n", err)
		os.Exit(1)
	}

	activeCount := countActiveRuns(runs)
	fmt.Printf("Active runs: %d\n", activeCount)

	printStreamDetails(js)
	printRunBreakdown(svc)
	printWorkflowMetrics(svc)
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

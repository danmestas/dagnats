// cli/status_detail.go
// Helpers for enriched status output: stream details table and run breakdown.
// Separated from status.go to keep each function short and testable.
package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
)

// streamInfo holds per-stream statistics for both human and JSON output.
type streamInfo struct {
	Name      string `json:"name"`
	Messages  uint64 `json:"messages"`
	Bytes     uint64 `json:"bytes"`
	Consumers int    `json:"consumers"`
}

// collectStreamInfo gathers stream statistics from JetStream.
// Returns a slice of streamInfo for all discoverable streams.
func collectStreamInfo(
	js nats.JetStreamContext,
) []streamInfo {
	if js == nil {
		panic("collectStreamInfo: js must not be nil")
	}

	const maxStreams = 200
	names := collectStreamNames(js, maxStreams)

	if len(names) == 0 {
		panic("collectStreamInfo: expected at least one stream")
	}

	result := make([]streamInfo, 0, len(names))
	for _, name := range names {
		info, err := js.StreamInfo(name)
		if err != nil {
			continue
		}
		result = append(result, streamInfo{
			Name:      name,
			Messages:  info.State.Msgs,
			Bytes:     info.State.Bytes,
			Consumers: info.State.Consumers,
		})
	}
	return result
}

// printStreamDetails prints a table of JetStream stream statistics.
// Thin wrapper over collectStreamInfo for human-readable output.
func printStreamDetails(js nats.JetStreamContext) {
	if js == nil {
		panic("printStreamDetails: js must not be nil")
	}

	streams := collectStreamInfo(js)
	if len(streams) == 0 {
		return
	}

	fmt.Println("\nStreams:")
	w := tabwriter.NewWriter(os.Stdout, 2, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  STREAM\tMESSAGES\tBYTES\tCONSUMERS\n")

	for _, s := range streams {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%d\n",
			s.Name,
			formatCount(s.Messages),
			formatBytes(s.Bytes),
			s.Consumers,
		)
	}
	w.Flush()
}

// collectStreamNames reads up to limit stream names from JetStream.
func collectStreamNames(
	js nats.JetStreamContext, limit int,
) []string {
	if js == nil {
		panic("collectStreamNames: js must not be nil")
	}
	if limit <= 0 {
		panic("collectStreamNames: limit must be positive")
	}

	names := make([]string, 0, limit)
	ch := js.StreamNames()
	for name := range ch {
		names = append(names, name)
		if len(names) >= limit {
			break
		}
	}
	return names
}

// collectRunCountMap queries all runs and returns a status-to-count map.
// Keys are human-readable status strings; suitable for JSON output.
func collectRunCountMap(
	svc *api.Service,
) (map[string]int, error) {
	if svc == nil {
		panic("collectRunCountMap: svc must not be nil")
	}

	runs, err := svc.ListRuns(context.Background(), "")
	if err != nil {
		return nil, err
	}

	if runs == nil {
		panic("collectRunCountMap: ListRuns returned nil")
	}

	counts := countRunsByStatus(runs)
	result := map[string]int{
		"pending":   counts[dag.RunStatusPending],
		"running":   counts[dag.RunStatusRunning],
		"completed": counts[dag.RunStatusCompleted],
		"failed":    counts[dag.RunStatusFailed],
		"cancelled": counts[dag.RunStatusCancelled],
	}
	return result, nil
}

// printRunBreakdown prints a one-line summary of runs grouped by status.
// Thin wrapper over collectRunCountMap for human-readable output.
func printRunBreakdown(svc *api.Service) {
	if svc == nil {
		panic("printRunBreakdown: svc must not be nil")
	}

	runs, err := svc.ListRuns(context.Background(), "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Runs:  error (%v)\n", err)
		return
	}

	if runs == nil {
		panic("printRunBreakdown: ListRuns returned nil")
	}

	counts := countRunsByStatus(runs)
	printRunCounts(counts)
}

// countRunsByStatus tallies runs into a fixed-size array indexed by status.
func countRunsByStatus(
	runs []dag.WorkflowRun,
) [5]int {
	if runs == nil {
		panic("countRunsByStatus: runs must not be nil")
	}

	const maxRuns = 10000
	if len(runs) > maxRuns {
		panic("countRunsByStatus: runs exceeds max bound")
	}

	var counts [5]int
	for _, r := range runs {
		idx := int(r.Status)
		if idx >= 0 && idx < len(counts) {
			counts[idx]++
		}
	}
	return counts
}

// printRunCounts formats and prints the status breakdown line.
func printRunCounts(counts [5]int) {
	for _, c := range counts {
		if c < 0 {
			panic("printRunCounts: negative count is impossible")
		}
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	if total < 0 {
		panic("printRunCounts: negative total is impossible")
	}

	if total == 0 {
		fmt.Println("Runs:  none")
		return
	}

	// Order: completed, failed, running, pending, cancelled
	// — most interesting statuses first.
	fmt.Printf("Runs:  %d completed, %d failed, "+
		"%d running, %d pending, %d cancelled\n",
		counts[dag.RunStatusCompleted],
		counts[dag.RunStatusFailed],
		counts[dag.RunStatusRunning],
		counts[dag.RunStatusPending],
		counts[dag.RunStatusCancelled],
	)
}

// formatBytes converts a byte count to a human-readable string with
// appropriate unit suffix (B, KB, MB, GB).
func formatBytes(bytes uint64) string {
	if bytes > 1<<50 {
		panic("formatBytes: unreasonably large byte count")
	}

	const (
		kb uint64 = 1024
		mb        = 1024 * kb
		gb        = 1024 * mb
	)
	if kb == 0 || mb == 0 || gb == 0 {
		panic("formatBytes: unit constants must be non-zero")
	}

	switch {
	case bytes == 0:
		return "0 B"
	case bytes < kb:
		return fmt.Sprintf("%d B", bytes)
	case bytes < mb:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(kb))
	case bytes < gb:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(mb))
	default:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(gb))
	}
}

// formatCount formats an integer with comma separators for readability.
func formatCount(n uint64) string {
	if n > 1<<50 {
		panic("formatCount: unreasonably large number")
	}
	const maxGroups = 7
	// Assert the group count can represent the max bounded value.
	// 1<<50 ~ 1.1e15, which needs 6 groups of 3 digits.
	if maxGroups < 6 {
		panic("formatCount: maxGroups too small for bounded range")
	}

	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	// Build digit groups iteratively from least significant.
	groups := [maxGroups]uint64{}
	groupIndex := 0
	remaining := n
	for remaining > 0 && groupIndex < maxGroups {
		groups[groupIndex] = remaining % 1000
		remaining = remaining / 1000
		groupIndex++
	}

	result := fmt.Sprintf("%d", groups[groupIndex-1])
	for i := groupIndex - 2; i >= 0; i-- {
		result += fmt.Sprintf(",%03d", groups[i])
	}
	return result
}

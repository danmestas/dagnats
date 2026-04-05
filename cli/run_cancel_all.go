// cli/run_cancel_all.go
// Bulk cancellation CLI command. Wraps api.BulkCancelRuns.
package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/danmestas/dagnats/api"
)

// runCancelAllCmd cancels all matching runs for a workflow.
func runCancelAllCmd(args []string) {
	if args == nil {
		panic("runCancelAllCmd: args must not be nil")
	}

	fs := flag.NewFlagSet("cancel-all", flag.ExitOnError)
	workflow := fs.String(
		"workflow", "", "workflow ID (required)",
	)
	status := fs.String(
		"status", "all", "running|pending|all",
	)
	after := fs.String("after", "", "RFC3339 timestamp")
	before := fs.String("before", "", "RFC3339 timestamp")
	dryRun := fs.Bool(
		"dry-run", false, "preview without cancelling",
	)
	jsonOut := fs.Bool("json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *workflow == "" {
		fmt.Fprintln(os.Stderr,
			"--workflow is required")
		fs.Usage()
		os.Exit(1)
	}

	req := api.BulkCancelRequest{
		WorkflowID: *workflow,
		Status:     *status,
		DryRun:     *dryRun,
	}
	if err := parseCancelAllTimes(
		&req, *after, *before,
	); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	svc, nc := connectService()
	defer nc.Close()

	resp, err := svc.BulkCancelRuns(
		context.Background(), req,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"bulk cancel: %v\n", err)
		os.Exit(1)
	}

	if *jsonOut {
		FormatJSON(os.Stdout, resp)
		return
	}
	printCancelAllResult(resp)
}

// parseCancelAllTimes parses after/before RFC3339 strings.
func parseCancelAllTimes(
	req *api.BulkCancelRequest,
	after, before string,
) error {
	if req == nil {
		panic("parseCancelAllTimes: req must not be nil")
	}
	if after != "" {
		t, err := time.Parse(time.RFC3339, after)
		if err != nil {
			return fmt.Errorf("--after: %w", err)
		}
		req.After = t
	}
	if before != "" {
		t, err := time.Parse(time.RFC3339, before)
		if err != nil {
			return fmt.Errorf("--before: %w", err)
		}
		req.Before = t
	}
	return nil
}

// printCancelAllResult prints human-readable output.
func printCancelAllResult(resp api.BulkCancelResponse) {
	if resp.DryRun {
		fmt.Printf("[dry-run] Would cancel %d runs\n",
			resp.Total)
		for _, id := range resp.Cancelled {
			fmt.Printf("  %s\n", id)
		}
		return
	}
	skipped := len(resp.Skipped)
	if skipped > 0 {
		fmt.Printf("Cancelled %d runs (%d skipped,"+
			" already terminal)\n",
			len(resp.Cancelled), skipped)
	} else {
		fmt.Printf("Cancelled %d runs\n",
			len(resp.Cancelled))
	}
}

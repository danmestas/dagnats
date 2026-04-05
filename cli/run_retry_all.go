// cli/run_retry_all.go
// Bulk retry CLI command. Wraps api.BulkRetryRuns.
package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/danmestas/dagnats/internal/api"
)

func runRetryAllCmd(args []string) {
	if args == nil {
		panic("runRetryAllCmd: args must not be nil")
	}
	fs := flag.NewFlagSet("retry-all", flag.ExitOnError)
	workflow := fs.String("workflow", "", "workflow ID (required)")
	mode := fs.String("mode", "", "rerun or replay (required)")
	after := fs.String("after", "", "RFC3339 timestamp")
	before := fs.String("before", "", "RFC3339 timestamp")
	dryRun := fs.Bool("dry-run", false, "preview without retrying")
	jsonOut := fs.Bool("json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	if *workflow == "" {
		fmt.Fprintln(os.Stderr, "--workflow is required")
		fs.Usage()
		os.Exit(1)
	}
	if *mode == "" {
		fmt.Fprintln(os.Stderr,
			"--mode is required (rerun or replay)")
		fs.Usage()
		os.Exit(1)
	}

	req := api.BulkRetryRequest{
		WorkflowID: *workflow,
		Mode:       *mode,
		DryRun:     *dryRun,
	}
	if err := parseRetryAllTimes(
		&req, *after, *before,
	); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	svc, nc := connectService()
	defer nc.Close()
	resp, err := svc.BulkRetryRuns(
		context.Background(), req,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bulk retry: %v\n", err)
		os.Exit(1)
	}
	if *jsonOut {
		FormatJSON(os.Stdout, resp)
		return
	}
	printRetryAllResult(resp)
}

func parseRetryAllTimes(
	req *api.BulkRetryRequest,
	after, before string,
) error {
	if req == nil {
		panic("parseRetryAllTimes: req must not be nil")
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

func printRetryAllResult(resp api.BulkRetryResponse) {
	if resp.DryRun {
		fmt.Printf("[dry-run] Would retry %d runs\n",
			resp.Total)
		for _, item := range resp.Retried {
			fmt.Printf("  %s\n", item.OriginalRunID)
		}
		return
	}
	skipped := len(resp.Skipped)
	if skipped > 0 {
		fmt.Printf("Retried %d runs (%d skipped)\n",
			len(resp.Retried), skipped)
	} else {
		fmt.Printf("Retried %d runs\n",
			len(resp.Retried))
	}
	for _, item := range resp.Retried {
		if item.NewRunID != "" {
			fmt.Printf("  %s -> %s\n",
				item.OriginalRunID, item.NewRunID)
		} else {
			fmt.Printf("  %s (replayed)\n",
				item.OriginalRunID)
		}
	}
}

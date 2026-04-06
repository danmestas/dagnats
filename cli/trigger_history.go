// cli/trigger_history.go
// Command for viewing trigger fire history. Connects to the
// TRIGGER_HISTORY stream and displays recent fire events.
package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/nats-io/nats.go"
)

// runTriggerHistoryCmd displays fire history for a trigger.
func runTriggerHistoryCmd(args []string) {
	runTriggerHistoryCmdWithWriter(args, os.Stdout, "")
}

// runTriggerHistoryCmdWithWriter is the testable version
// that accepts a writer and optional NATS URL override.
func runTriggerHistoryCmdWithWriter(
	args []string, w io.Writer, natsURL string,
) {
	if w == nil {
		panic(
			"runTriggerHistoryCmdWithWriter: w must not be nil",
		)
	}
	if args == nil {
		panic(
			"runTriggerHistoryCmdWithWriter: args must not be nil",
		)
	}
	triggerID, limit, jsonOutput := parseHistoryFlags(args)
	if triggerID == "" {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger history "+
				"<trigger-id> [--limit=N] [--json]")
		exitFunc(1)
		return
	}
	svc, nc := connectHistoryService(natsURL)
	defer nc.Close()
	fires := fetchTriggerFires(svc, triggerID, limit)
	if jsonOutput {
		FormatJSON(w, fires)
		return
	}
	printFireTable(w, fires)
}

// parseHistoryFlags extracts trigger-id, --limit, and --json
// from the argument list.
func parseHistoryFlags(
	args []string,
) (string, int, bool) {
	if args == nil {
		panic("parseHistoryFlags: args must not be nil")
	}
	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	fs := flag.NewFlagSet("trigger history", flag.ExitOnError)
	limit := fs.Int("limit", 10, "Max fire records")
	fs.Parse(args)

	if *limit <= 0 {
		*limit = 10
	}
	const maxLimit = 500
	if *limit > maxLimit {
		*limit = maxLimit
	}

	triggerID := ""
	if fs.NArg() > 0 {
		triggerID = fs.Arg(0)
	}
	return triggerID, *limit, jsonOutput
}

// connectHistoryService creates a NATS connection and API
// service for reading trigger history. Uses the override URL
// if provided (for tests), otherwise resolves from env.
func connectHistoryService(
	natsURL string,
) (*api.Service, *nats.Conn) {
	if natsURL == "" {
		natsURL = GetEnvWithFallback(
			"DAGNATS_NATS_URL", "NATS_URL",
			nats.DefaultURL,
		)
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: cannot connect to NATS at %s\n"+
				"Hint: run 'dagnats serve' first\n",
			natsURL)
		exitFunc(1)
		return nil, nil
	}
	svc, initErr := tryNewService(nc)
	if initErr != "" {
		nc.Close()
		fmt.Fprintf(os.Stderr,
			"Error: %s\n"+
				"Hint: run 'dagnats serve' first\n",
			initErr)
		exitFunc(1)
		return nil, nil
	}
	return svc, nc
}

// fetchTriggerFires calls the API service and handles errors.
func fetchTriggerFires(
	svc *api.Service, triggerID string, limit int,
) []api.TriggerFireEntry {
	if svc == nil {
		panic("fetchTriggerFires: svc must not be nil")
	}
	if triggerID == "" {
		panic(
			"fetchTriggerFires: triggerID must not be empty",
		)
	}
	fires, err := svc.ListTriggerFires(
		context.Background(), triggerID, limit,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: %v\n", err)
		exitFunc(1)
		return nil
	}
	return fires
}

// printFireTable writes a formatted table of fire records.
func printFireTable(
	w io.Writer, fires []api.TriggerFireEntry,
) {
	if w == nil {
		panic("printFireTable: w must not be nil")
	}
	if fires == nil {
		panic("printFireTable: fires must not be nil")
	}
	if len(fires) == 0 {
		fmt.Fprintln(w, "No fire history found.")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TIME\tSTATUS\tRUN ID\tDURATION")
	const maxFires = 500
	for i, fire := range fires {
		if i >= maxFires {
			break
		}
		printFireRow(tw, fire)
	}
	tw.Flush()
}

// printFireRow writes a single fire record row to the writer.
func printFireRow(
	w io.Writer, fire api.TriggerFireEntry,
) {
	if w == nil {
		panic("printFireRow: w must not be nil")
	}
	status := fire.Status
	if status == "" {
		status = "-"
	} else if colorEnabled() {
		status = ColorStatus(status)
	}
	runID := fire.RunID
	if runID == "" {
		runID = "-"
	}
	durStr := "-"
	if fire.Duration > 0 {
		durStr = fire.Duration.Truncate(
			time.Millisecond,
		).String()
	}
	timeStr := fire.FiredAt.Format(
		"2006-01-02 15:04:05",
	)
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
		timeStr, status, runID, durStr,
	)
}

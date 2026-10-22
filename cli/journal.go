// cli/journal.go
// `dagnats journal check <run>`: audit a run's agent journals (ADR-026)
// against its history and snapshot with journal.Check, and exit 1 on
// any violation so the command gates a chaos soak or a CI run.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
)

// journalCheckTimeout bounds one audit: a journal read per step and
// one history read, each bounded by its own fetch windows.
const journalCheckTimeout = 2 * time.Minute

// runJournalCmd dispatches journal subcommands.
func runJournalCmd(args []string) {
	if args == nil {
		panic("runJournalCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runJournalCmd: args exceeds max bound")
	}
	if HasHelpFlag(args) || len(args) == 0 {
		printJournalUsage()
		return
	}
	switch args[0] {
	case "check":
		runJournalCheckCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown journal subcommand: %s\n", args[0])
		printJournalUsage()
		exitFunc(1)
	}
}

func printJournalUsage() {
	fmt.Println("Usage: dagnats journal <command> [args]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  check <run-id> [--json]   audit the run's agent journals")
}

// runJournalCheckCmd connects and runs the audit for one run.
func runJournalCheckCmd(args []string) {
	if args == nil {
		panic("runJournalCheckCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runJournalCheckCmd: args exceeds max bound")
	}
	runID := ""
	asJSON := false
	for _, arg := range args {
		if arg == "--json" {
			asJSON = true
			continue
		}
		runID = arg
	}
	if runID == "" {
		fmt.Fprintln(os.Stderr, "usage: dagnats journal check <run-id> [--json]")
		exitFunc(1)
		return
	}
	_, nc := connectService()
	if nc == nil {
		return
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), journalCheckTimeout)
	defer cancel()
	exitFunc(checkJournal(ctx, nc, runID, asJSON, os.Stdout))
}

// checkJournal runs the audit and writes the report; the result is
// the exit code: 0 clean, 1 violations or a failed audit.
func checkJournal(
	ctx context.Context, nc *nats.Conn, runID string, asJSON bool, out io.Writer,
) int {
	if nc == nil {
		panic("checkJournal: nc must not be nil")
	}
	if runID == "" {
		panic("checkJournal: runID must not be empty")
	}
	report, err := journal.Check(ctx, nc, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fmt.Fprintf(os.Stderr, "Error: encode report: %v\n", err)
			return 1
		}
	} else {
		printJournalReport(out, report)
	}
	if report.OK() {
		return 0
	}
	return 1
}

// printJournalReport writes the human-readable report.
func printJournalReport(out io.Writer, report journal.Report) {
	if out == nil {
		panic("printJournalReport: out must not be nil")
	}
	if report.RunID == "" {
		panic("printJournalReport: report must name a run")
	}
	fmt.Fprintf(out, "run %s: %d journal step(s)\n", report.RunID, len(report.Steps))
	for _, step := range report.Steps {
		status := "ok"
		if len(step.Violations) > 0 {
			status = fmt.Sprintf("%d violation(s)", len(step.Violations))
		}
		fmt.Fprintf(out, "  %s: %d records, %d commits, %s\n",
			step.StepID, step.Records, step.Commits, status)
		for _, v := range step.Violations {
			fmt.Fprintf(out, "    - %s\n", v)
		}
	}
	for _, skipped := range report.Skipped {
		fmt.Fprintf(out, "  skipped: %s\n", skipped)
	}
	if report.OK() {
		fmt.Fprintln(out, "OK")
		return
	}
	fmt.Fprintln(out, "FAIL")
}

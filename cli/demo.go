// cli/demo.go
// `dagnats demo` command — operator-facing entry point for
// seed-and-drive demo data. Today the only subcommand is `seed`,
// which spins an in-process noop worker, registers a single-step
// demo workflow, starts N runs with predetermined outcomes drawn
// from a 70 / 20 / 10 completed / failed / cancelled distribution,
// and waits for every run to reach a terminal state before exiting.
//
// The noop worker runs ONLY during the lifetime of this command —
// it is not registered when `dagnats serve` boots. This keeps demo
// machinery out of the production hot path while still exercising
// the real engine completion path (vs. synthesising history events,
// which would bypass the wiring the demo is meant to verify).
package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// runDemoCmd is the entry point for `dagnats demo <subcommand>`.
func runDemoCmd(args []string) {
	if args == nil {
		panic("runDemoCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runDemoCmd: args exceeds max bound")
	}
	if HasHelpFlag(args) || len(args) == 0 {
		printDemoUsage()
		return
	}
	switch args[0] {
	case "seed":
		runDemoSeedCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown demo subcommand: %s\n", args[0])
		printDemoUsage()
		exitFunc(1)
	}
}

// demoSeedFlags holds parsed flags for `dagnats demo seed`.
type demoSeedFlags struct {
	count         int
	includeFailed bool
	timeout       time.Duration
	json          bool
}

// parseDemoSeedFlags extracts flags from args.
func parseDemoSeedFlags(args []string) (demoSeedFlags, error) {
	if args == nil {
		panic("parseDemoSeedFlags: args must not be nil")
	}
	if len(args) > 100 {
		panic("parseDemoSeedFlags: args exceeds max bound")
	}

	f := demoSeedFlags{
		count:   10,
		timeout: 5 * time.Second,
	}
	for _, arg := range args {
		switch {
		case arg == "--include-failed":
			f.includeFailed = true
		case arg == "--json":
			f.json = true
		case strings.HasPrefix(arg, "--count="):
			val := strings.TrimPrefix(arg, "--count=")
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 || n > 1000 {
				return f, fmt.Errorf(
					"invalid --count=%q: must be 1..1000", val,
				)
			}
			f.count = n
		case strings.HasPrefix(arg, "--timeout="):
			val := strings.TrimPrefix(arg, "--timeout=")
			d, err := time.ParseDuration(val)
			if err != nil || d <= 0 || d > 60*time.Second {
				return f, fmt.Errorf(
					"invalid --timeout=%q: must be 1ms..60s", val,
				)
			}
			f.timeout = d
		default:
			return f, fmt.Errorf("unknown flag: %s", arg)
		}
	}
	return f, nil
}

// runDemoSeedCmd is the `dagnats demo seed` handler.
func runDemoSeedCmd(args []string) {
	if HasHelpFlag(args) {
		printDemoSeedUsage()
		return
	}
	f, err := parseDemoSeedFlags(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exitFunc(1)
		return
	}

	svc, nc := connectService()
	defer nc.Close()

	result, err := runDemoSeed(svc, nc, demoSeedOptions{
		count:         f.count,
		includeFailed: f.includeFailed,
		waitTimeout:   f.timeout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exitFunc(1)
		return
	}

	if f.json {
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			exitFunc(1)
			return
		}
		return
	}

	printDemoSeedResult(result, f.count)
}

// printDemoSeedResult writes a friendly summary of seed outcomes.
func printDemoSeedResult(r demoSeedResult, requested int) {
	fmt.Printf(
		"Seeded %d runs: %d completed, %d failed, %d cancelled",
		requested, r.Completed, r.Failed, r.Cancelled,
	)
	if r.Stuck > 0 {
		fmt.Printf(", %d stuck (did not reach terminal in timeout)",
			r.Stuck)
	}
	fmt.Println()
}

// printDemoUsage prints the help text for `dagnats demo`.
func printDemoUsage() {
	fmt.Println("Usage: dagnats demo <subcommand>")
	fmt.Println()
	fmt.Println("Subcommands:")
	fmt.Println("  seed  seed runs and drive them to terminal" +
		" states via an in-process noop worker")
}

// printDemoSeedUsage prints the help text for `dagnats demo seed`.
func printDemoSeedUsage() {
	fmt.Println("Usage: dagnats demo seed [flags]")
	fmt.Println()
	fmt.Println("Seeds N runs and drives each to a terminal state" +
		" (completed / failed / cancelled)")
	fmt.Println("using an in-process noop worker. Default" +
		" distribution: 70% completed, 20% failed, 10% cancelled.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --count=N           number of runs to seed" +
		" (default 10, max 1000)")
	fmt.Println("  --include-failed    force at least one failed" +
		" and one cancelled run")
	fmt.Println("  --timeout=DUR       max wait for all runs to" +
		" terminate (default 5s, max 60s)")
	fmt.Println("  --json              emit terminal counts as JSON")
}

package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/server"
)

func runServeCmd(args []string) {
	if HasHelpFlag(args) {
		printServeHelp()
		return
	}

	if hasDryRunFlag(args) {
		runServeDryRun()
		return
	}

	cfg := server.ConfigFromEnv()
	srv := server.New(cfg)

	if len(cfg.Workers) > 0 {
		w := server.EmbeddedWorker(srv)
		for _, wc := range cfg.Workers {
			w.Handle(wc.Task, buildHandler(wc))
		}
	}

	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// printServeHelp prints usage for the serve command.
func printServeHelp() {
	fmt.Println("Usage: dagnats serve [--dry-run]")
	fmt.Println("Starts embedded NATS server with" +
		" DagNats engine and API.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --dry-run  validate config" +
		" without starting the server")
	fmt.Println()
	fmt.Println("Config: dagnats.yaml" +
		" (optional, in current directory)")
	fmt.Println("Env:    DAGNATS_DATA_DIR," +
		" DAGNATS_HTTP_ADDR, DAGNATS_NATS_PORT")
	fmt.Println()
	fmt.Println("Run 'dagnats config show'" +
		" to see effective configuration.")
}

// hasDryRunFlag checks if --dry-run is present in args.
func hasDryRunFlag(args []string) bool {
	if args == nil {
		panic("hasDryRunFlag: args is nil")
	}
	if len(args) > 1000 {
		panic("hasDryRunFlag: args exceeds max bound")
	}
	for _, a := range args {
		if a == "--dry-run" {
			return true
		}
	}
	return false
}

// runServeDryRun validates config and prints a report.
// Exits 0 on success, 1 on validation failure.
func runServeDryRun() {
	rc := server.ResolveConfig()
	passed := server.PrintDryRun(os.Stdout, rc)
	if !passed {
		os.Exit(1)
	}
}

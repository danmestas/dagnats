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

	configPath := extractConfigFlag(args)

	cfg, loadedPath, err := server.ConfigWithPath(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	printConfigSource(os.Stderr, loadedPath)

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
	fmt.Println("Usage: dagnats serve [--config=PATH]" +
		" [--dry-run]")
	fmt.Println("Starts embedded NATS server with" +
		" DagNats engine and API.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --config=PATH  path to config file")
	fmt.Println("  --dry-run      validate config" +
		" without starting the server")
	fmt.Println()
	fmt.Println("Config search order (when --config" +
		" not specified):")
	fmt.Println("  1. ./dagnats.yaml")
	fmt.Println("  2. $XDG_CONFIG_HOME/dagnats/" +
		"dagnats.yaml")
	fmt.Println("  3. /etc/dagnats/dagnats.yaml" +
		" (Linux only)")
	fmt.Println()
	fmt.Println("Env: DAGNATS_DATA_DIR," +
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

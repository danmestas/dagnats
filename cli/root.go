package cli

import (
	"fmt"
	"os"
)

// Run is the main entry point for the CLI. It dispatches to subcommand handlers
// based on the first argument. Exits with code 1 on usage errors so the shell
// can detect failure without inspecting stderr.
func Run(args []string) {
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch args[1] {
	case "workflow":
		runWorkflowCmd(args[2:])
	case "run":
		runRunCmd(args[2:])
	case "trigger":
		runTriggerCmd(args[2:])
	case "dlq":
		runDLQCmd(args[2:])
	case "serve":
		runServeCmd(args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: dagnats <command> [args]")
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  workflow  list, register workflows")
	fmt.Fprintln(os.Stderr, "  run       start, status, history, retry, cancel, signal runs")
	fmt.Fprintln(os.Stderr, "  trigger   create, list, delete triggers")
	fmt.Fprintln(os.Stderr, "  dlq       list, replay dead-letter messages")
	fmt.Fprintln(os.Stderr, "  serve     start embedded server")
}

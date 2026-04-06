package cli

import (
	"fmt"
	"os"
)

// Run is the main entry point for the CLI. It dispatches to subcommand handlers
// based on the first argument. Exits with code 1 on usage errors so the shell
// can detect failure without inspecting stderr.
func Run(args []string) {
	if args == nil {
		panic("Run: args must not be nil")
	}
	if len(args) > 1000 {
		panic("Run: args exceeds max bound")
	}
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch args[1] {
	case "--help", "-h":
		printUsage()
		return
	case "--version", "-v":
		printVersion()
		return
	case "workflow":
		runWorkflowCmd(args[2:])
	case "run":
		runRunCmd(args[2:])
	case "trigger":
		runTriggerCmd(args[2:])
	case "dlq":
		runDLQCmd(args[2:])
	case "workers":
		runWorkersCmd(args[2:])
	case "serve":
		runServeCmd(args[2:])
	case "status":
		runSystemStatusCmd(args[2:])
	case "init":
		runInitCmd(args[2:])
	case "config":
		runConfigCmd(args[2:])
	case "logs":
		runLogsCmd(args[2:])
	case "dev":
		runDevCmd(args[2:])
	case "trace":
		runTraceCmd(args[2:])
	case "metrics":
		runMetricsCmd(args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: dagnats <command> [args]")
	fmt.Println("Commands:")
	fmt.Println("  workflow  list, register workflows")
	fmt.Println(
		"  run       start, status, list, events, cancel, signal runs")
	fmt.Println(
		"  trigger   create, list, delete, enable, disable triggers")
	fmt.Println("  dlq       list, replay dead-letter messages")
	fmt.Println("  workers   list registered workers")
	fmt.Println("  serve     start embedded server")
	fmt.Println("  init      scaffold a new workflow project")
	fmt.Println("  config    show effective configuration")
	fmt.Println("  status    show system health")
	fmt.Println("  logs      tail telemetry log stream")
	fmt.Println("  dev       watch mode: build and restart on changes")
	fmt.Println("  trace     view and search trace spans")
	fmt.Println("  metrics   view metric snapshots")
	fmt.Println("\nGlobal flags:")
	fmt.Println("  --json    output in JSON format")
}

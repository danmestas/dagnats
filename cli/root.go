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
	case "observe":
		runObserveCmd(args[2:])
	case "sidecar":
		runSidecarCmd(args[2:])
	case "singleton":
		runSingletonCmd(args[2:])
	case "service":
		runServiceCmd(args[2:])
	case "trigger-type":
		runTriggerTypeCmd(args[2:])
	case "clean":
		runCleanCmd(args[2:])
	case "journal":
		runJournalCmd(args[2:])
	case "demo":
		runDemoCmd(args[2:])
	case "completion":
		runCompletionCmd(args[2:])
	case "__complete":
		handleCompleteCmd(args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[1])
		printUsage()
		os.Exit(1)
	}
}

// usageEntry is one line of top-level help: command name plus summary.
type usageEntry struct {
	name    string
	summary string
}

// usageCommands lists every dispatched top-level command (hidden
// __complete excluded). root_test.go fails when Run gains a case that
// is missing here.
var usageCommands = []usageEntry{
	{"workflow", "list, register, show, validate, delete workflows"},
	{"run", "start, status, list, count, events, inspect, watch, output, " +
		"retry, cancel, signal runs"},
	{"trigger", "create, list, update, delete, enable, disable, fire, " +
		"test, history triggers"},
	{"trigger-type", "list, describe external trigger types"},
	{"dlq", "list, replay, watch dead-letter messages"},
	{"workers", "list registered workers"},
	{"serve", "start embedded server"},
	{"init", "scaffold a new workflow project"},
	{"config", "show effective configuration"},
	{"status", "show system health"},
	{"logs", "tail or search telemetry log stream"},
	{"dev", "watch mode: build and restart on changes"},
	{"trace", "view and search trace spans"},
	{"metrics", "view metric snapshots"},
	{"observe", "observability pipeline health"},
	{"sidecar", "local observability sidecar"},
	{"singleton", "list and release singleton locks"},
	{"service", "list registered services"},
	{"clean", "purge run data (for testing)"},
	{"journal", "check a run's agent journals"},
	{"demo", "seed runs and drive them to terminal states"},
	{"completion", "generate shell completion scripts"},
}

func printUsage() {
	fmt.Println("Usage: dagnats <command> [args]")
	fmt.Println("Commands:")
	for _, entry := range usageCommands {
		fmt.Printf("  %-13s %s\n", entry.name, entry.summary)
	}
	fmt.Println("\nGlobal flags:")
	fmt.Println("  --json    output in JSON format")
}

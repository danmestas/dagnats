// cli/workers.go
// Commands for observing worker status: list registered workers.
package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/danmestas/dagnats/worker"
)

// runWorkersCmd dispatches workers subcommands.
func runWorkersCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats workers <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  list       list currently registered workers")
		return
	}
	if len(args) == 0 {
		fmt.Println("Usage: dagnats workers <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  list       list currently registered workers")
		return
	}
	switch args[0] {
	case "list":
		runWorkersListCmd(args[1:])
	default:
		fmt.Printf("unknown workers subcommand: %s\n", args[0])
	}
}

// runWorkersListCmd retrieves and prints all registered workers.
func runWorkersListCmd(args []string) {
	jsonOutput := HasJSONFlag(args)

	svc, nc := connectService()
	defer nc.Close()

	workers, err := svc.ListWorkers(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "list workers: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, workers); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if len(workers) == 0 {
		fmt.Println("No workers registered.")
		return
	}

	printWorkersTable(workers)
}

// printWorkersTable renders workers as a human-readable table.
func printWorkersTable(workers []worker.WorkerRegistration) {
	if len(workers) == 0 {
		panic("printWorkersTable: workers must not be empty")
	}
	if len(workers) > 100000 {
		panic("printWorkersTable: workers exceeds max bound")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER_ID\tTASK_TYPES\tLANGUAGE\tMAX_TASKS")

	for _, worker := range workers {
		taskTypes := strings.Join(worker.TaskTypes, ",")
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n",
			worker.WorkerID, taskTypes, worker.Language,
			worker.MaxTasks,
		)
	}

	w.Flush()
}

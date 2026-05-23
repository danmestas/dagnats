// cli/service.go
// `dagnats service` command — observes the `services` KV bucket
// (ADR-017 / #321). Pure read-only — workers register services via
// the worker.RegisterService SDK method.
package cli

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// runServiceCmd dispatches service subcommands.
func runServiceCmd(args []string) {
	if args == nil {
		panic("runServiceCmd: args must not be nil")
	}
	if HasHelpFlag(args) || len(args) == 0 {
		printServiceUsage()
		return
	}
	switch args[0] {
	case "list":
		runServiceListCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr,
			"unknown service subcommand: %s\n", args[0])
		printServiceUsage()
		exitFunc(1)
	}
}

// printServiceUsage prints the usage for the service command.
func printServiceUsage() {
	fmt.Println("Usage: dagnats service <command> [--json]")
	fmt.Println("Commands:")
	fmt.Println("  list       list registered services")
}

// runServiceListCmd reads the services KV and prints the entries.
// Empty bucket prints a single "No services registered." line; the
// CLI never reports the empty state as an error.
func runServiceListCmd(args []string) {
	if args == nil {
		panic("runServiceListCmd: args must not be nil")
	}
	jsonOutput := HasJSONFlag(args)

	nc, err := connectNATS()
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: cannot connect to NATS: %v\n", err)
		exitFunc(1)
		return
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: jetstream init: %v\n", err)
		exitFunc(1)
		return
	}

	services, err := worker.ListServices(js)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"list services: %v\n", err)
		exitFunc(1)
		return
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, services); err != nil {
			fmt.Fprintf(os.Stderr,
				"format json: %v\n", err)
			exitFunc(1)
			return
		}
		return
	}

	if len(services) == 0 {
		fmt.Println("No services registered.")
		return
	}

	printServicesTable(services)
}

// connectNATS opens a NATS connection using the standard env-var
// resolution. Returns the bare *nats.Conn — service commands do not
// need the api.Service that connectService() builds, so this avoids
// the api.NewService bootstrap (which would fail in tests that don't
// provision the full stream set).
func connectNATS() (*nats.Conn, error) {
	natsURL := GetEnvWithFallback(
		"DAGNATS_NATS_URL", "NATS_URL", nats.DefaultURL,
	)
	return nats.Connect(natsURL)
}

// printServicesTable renders services as a human-readable table.
// Panics on empty input — callers check len() first and print a
// dedicated "no services" message; an empty table would be a UX
// regression silently caused by upstream churn.
func printServicesTable(services []worker.ServiceDef) {
	if len(services) == 0 {
		panic("printServicesTable: services must not be empty")
	}
	const maxRows = 100000
	if len(services) > maxRows {
		panic("printServicesTable: services exceeds max bound")
	}

	w := tabwriter.NewWriter(
		os.Stdout, 0, 0, 2, ' ', 0,
	)
	fmt.Fprintln(w, "NAME\tDESCRIPTION\tREGISTERED")
	for _, s := range services {
		registered := s.RegisteredAt.UTC().Format(time.RFC3339)
		fmt.Fprintf(w, "%s\t%s\t%s\n",
			s.Name, s.Description, registered,
		)
	}
	w.Flush()
}

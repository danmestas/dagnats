// Command dagnats-ci is the CLI entry point for the dagnats-ci add-on.
//
// Subcommand: compile
//
//	dagnats-ci compile <path-to-ci.yml> [--name <workflow-name>]
//
// Reads the given ci.yml, compiles it into a dag.WorkflowDef, and writes
// the JSON to stdout. Exits 1 on any error, printing to stderr.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/danmestas/dagnats-ci/internal/compile"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "dagnats-ci: %v\n", err)
		os.Exit(1)
	}
}

// run is the testable entry point. It receives the argument slice (os.Args[1:])
// and returns an error rather than calling os.Exit so tests can exercise it
// with a temporary ci.yml file and assert on the output.
func run(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: dagnats-ci compile <path-to-ci.yml> [--name <name>]")
	}

	subcommand := args[0]
	if subcommand != "compile" {
		return fmt.Errorf("unknown subcommand %q; supported subcommands: compile", subcommand)
	}

	return runCompile(args[1:])
}

// runCompile implements the compile subcommand. --name may appear before or
// after the positional ci.yml path. Go's flag package stops at the first
// non-flag argument, so we scan args manually to handle both orderings.
func runCompile(args []string) error {
	name := "ci"
	var positional []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name", "-name":
			if i+1 >= len(args) {
				return fmt.Errorf("compile: --name requires a value")
			}
			name = args[i+1]
			i++
		default:
			if len(args[i]) > 1 && args[i][0] == '-' {
				return fmt.Errorf("compile: unknown flag %q", args[i])
			}
			positional = append(positional, args[i])
		}
	}
	if len(positional) == 0 {
		return fmt.Errorf("compile: path to ci.yml is required as a positional argument")
	}
	path := positional[0]

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("compile: read %q: %w", path, err)
	}
	spec, err := compile.ParseSpec(data)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}
	def, err := compile.Compile(name, spec)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}
	out, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		// json.MarshalIndent on a dag.WorkflowDef should never fail; the panic
		// surfaces a regression if a future field adds an unmarshalable type.
		panic(fmt.Sprintf("runCompile: json.MarshalIndent: %v", err))
	}
	fmt.Printf("%s\n", out)
	return nil
}

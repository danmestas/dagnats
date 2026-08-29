// Command dagnats-ci is the CLI entry point for the dagnats-ci add-on.
//
// Subcommand: compile
//
//	dagnats-ci compile <path-to-ci.yml> [--name <workflow-name>]
//
// Reads the given ci.yml, compiles it into a dag.WorkflowDef, and writes
// the JSON to stdout. Any diagnostics (parse or compile problems) are
// printed to stderr as "file:line:col: field: message", one per line, and
// the command exits 1 -- nothing is written to stdout in that case.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/danmestas/dagnats/ci"
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
	def, diags := ci.CompileYAML(name, data)
	if len(diags) > 0 {
		return diagnosticsError(path, diags)
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

// diagnosticsError renders every diagnostic as one "file:line:col: field:
// message" line to stderr and returns a summary error so run's caller exits
// 1. Line/Col/Field are omitted from a line when unset (0 / "") rather than
// printed as misleading zeros.
func diagnosticsError(path string, diags []ci.Diagnostic) error {
	if path == "" {
		panic("diagnosticsError: path must not be empty")
	}
	if len(diags) == 0 {
		panic("diagnosticsError: diags must not be empty")
	}
	for _, d := range diags {
		fmt.Fprintln(os.Stderr, formatDiagnostic(path, d))
	}
	return fmt.Errorf("compile: %d diagnostic(s)", len(diags))
}

// formatDiagnostic renders one diagnostic as "file:line:col: field: message".
func formatDiagnostic(path string, d ci.Diagnostic) string {
	if path == "" {
		panic("formatDiagnostic: path must not be empty")
	}
	loc := path
	if d.Line > 0 {
		if d.Column > 0 {
			loc = fmt.Sprintf("%s:%d:%d", path, d.Line, d.Column)
		} else {
			loc = fmt.Sprintf("%s:%d", path, d.Line)
		}
	}
	if d.Field != "" {
		return fmt.Sprintf("%s: %s: %s", loc, d.Field, d.Message)
	}
	return fmt.Sprintf("%s: %s", loc, d.Message)
}

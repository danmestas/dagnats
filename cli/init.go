// cli/init.go
// Scaffold command that creates a new workflow project directory with
// boilerplate files. Operates offline — no NATS connection required.
package cli

import (
	"fmt"
	"os"
	"regexp"
)

// initResult is the JSON output for the init command.
type initResult struct {
	Name      string   `json:"name"`
	Directory string   `json:"directory"`
	Files     []string `json:"files"`
	Error     string   `json:"error,omitempty"`
}

// namePattern matches valid project names: alphanumeric and hyphens,
// must start and end with an alphanumeric character.
var namePattern = regexp.MustCompile(
	`^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]$`,
)

// validateProjectName checks that name is non-empty and contains
// only alphanumeric characters and hyphens.
func validateProjectName(name string) error {
	if len(name) > 256 {
		panic("validateProjectName: name exceeds max length")
	}
	if name == "" {
		return fmt.Errorf("project name must not be empty")
	}

	// Single-char names: just check alphanumeric.
	if len(name) == 1 {
		if !regexp.MustCompile(`^[a-zA-Z0-9]$`).MatchString(name) {
			return fmt.Errorf(
				"project name must be alphanumeric and hyphens only",
			)
		}
		return nil
	}

	if !namePattern.MatchString(name) {
		return fmt.Errorf(
			"project name must be alphanumeric and hyphens only",
		)
	}
	return nil
}

// scaffoldProject creates the project directory and writes
// boilerplate files. Returns an error if the directory already exists
// or any file write fails.
func scaffoldProject(dir string, name string) error {
	if dir == "" {
		panic("scaffoldProject: dir must not be empty")
	}
	if name == "" {
		panic("scaffoldProject: name must not be empty")
	}

	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf(
			"directory %q already exists", dir,
		)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	if err := writeWorkflowJSON(dir, name); err != nil {
		return fmt.Errorf("write workflow.json: %w", err)
	}

	if err := writeMainGo(dir); err != nil {
		return fmt.Errorf("write main.go: %w", err)
	}

	return nil
}

// writeWorkflowJSON writes the minimal workflow definition file.
func writeWorkflowJSON(dir string, name string) error {
	if dir == "" {
		panic("writeWorkflowJSON: dir must not be empty")
	}
	if name == "" {
		panic("writeWorkflowJSON: name must not be empty")
	}

	schemaURL := "https://raw.githubusercontent.com/" +
		"danmestas/dagnats/main/docs/workflow-schema.json"
	content := fmt.Sprintf(`{
  "$schema": %q,
  "name": %q,
  "version": "1.0",
  "steps": [
    {
      "id": "process",
      "task": "process"
    }
  ]
}
`, schemaURL, name)

	path := dir + "/workflow.json"
	return os.WriteFile(path, []byte(content), 0644)
}

// writeMainGo writes the worker boilerplate with handler stubs.
func writeMainGo(dir string) error {
	if dir == "" {
		panic("writeMainGo: dir must not be empty")
	}
	if len(dir) > 4096 {
		panic("writeMainGo: dir path unreasonably long")
	}

	content := `package main

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	tel := observe.NewNoopTelemetry()
	w := worker.NewWorker(nc, tel)

	w.Handle("process", func(ctx worker.TaskContext) error {
		input := ctx.Input()
		fmt.Printf("[process] input: %s\n", string(input))
		return ctx.Complete(input)
	})

	fmt.Println("Worker ready. Waiting for tasks...")
	w.Start()
}
`

	path := dir + "/main.go"
	return os.WriteFile(path, []byte(content), 0644)
}

// runInitCmd is the CLI entry point for "dagnats init <name>".
func runInitCmd(args []string) {
	if args == nil {
		panic("runInitCmd: args must not be nil")
	}
	if len(args) > 1000 {
		panic("runInitCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	if jsonOutput {
		args = StripJSONFlag(args)
	}

	if len(args) != 1 {
		if jsonOutput {
			out := initResult{
				Error: "usage: dagnats init <name>",
			}
			if err := FormatJSON(os.Stdout, out); err != nil {
				fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			}
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats init <name> [--json]")
		os.Exit(1)
	}

	name := args[0]
	if err := validateProjectName(name); err != nil {
		if jsonOutput {
			out := initResult{Error: err.Error()}
			if fmtErr := FormatJSON(os.Stdout, out); fmtErr != nil {
				fmt.Fprintf(
					os.Stderr, "format json: %v\n", fmtErr,
				)
			}
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "invalid name: %v\n", err)
		os.Exit(1)
	}

	runInitScaffold(name, jsonOutput)
}

// runInitScaffold performs the scaffold and prints results.
// Separated from runInitCmd to stay within the 70-line limit.
func runInitScaffold(name string, jsonOutput bool) {
	if name == "" {
		panic("runInitScaffold: name must not be empty")
	}
	if len(name) > 256 {
		panic("runInitScaffold: name exceeds max length")
	}

	dir := name
	files := []string{"workflow.json", "main.go"}

	if err := scaffoldProject(dir, name); err != nil {
		if jsonOutput {
			out := initResult{Error: err.Error()}
			if fmtErr := FormatJSON(os.Stdout, out); fmtErr != nil {
				fmt.Fprintf(
					os.Stderr, "format json: %v\n", fmtErr,
				)
			}
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "init: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		out := initResult{
			Name:      name,
			Directory: dir,
			Files:     files,
		}
		if err := FormatJSON(os.Stdout, out); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Created %s/\n", dir)
	for _, f := range files {
		fmt.Printf("  %s/%s\n", dir, f)
	}
}

// cli/workflow.go
// Commands for managing workflow definitions: list, register.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/danmestas/dagnats/dag"
)

// runWorkflowCmd dispatches workflow subcommands.
func runWorkflowCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats workflow <command>")
		fmt.Println("Commands:")
		fmt.Println("  list       list registered workflows")
		fmt.Println("  register   register a workflow from a JSON file")
		fmt.Println("  show       show details of a registered workflow")
		fmt.Println("  validate   validate a workflow JSON file")
		return
	}
	if len(args) == 0 {
		fmt.Println("Usage: dagnats workflow <command>")
		fmt.Println("Commands:")
		fmt.Println("  list       list registered workflows")
		fmt.Println("  register   register a workflow from a JSON file")
		fmt.Println("  show       show details of a registered workflow")
		fmt.Println("  validate   validate a workflow JSON file")
		return
	}
	switch args[0] {
	case "list":
		runWorkflowListCmd(args[1:])
	case "register":
		runWorkflowRegisterCmd(args[1:])
	case "show":
		runWorkflowShowCmd(args[1:])
	case "validate":
		runWorkflowValidateCmd(args[1:])
	default:
		fmt.Printf("unknown workflow subcommand: %s\n", args[0])
	}
}

// runWorkflowListCmd retrieves and prints all registered workflows.
func runWorkflowListCmd(args []string) {
	jsonOutput := HasJSONFlag(args)

	svc, nc := connectService()
	defer nc.Close()

	defs, err := svc.ListWorkflows(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "list workflows: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, defs); err != nil {
			fmt.Fprintf(
				os.Stderr, "format json: %v\n", err,
			)
			os.Exit(1)
		}
		return
	}

	if len(defs) == 0 {
		fmt.Println("No workflows registered.")
		return
	}

	printWorkflowListTable(defs)
}

// printWorkflowListTable renders workflows as a human-readable table.
func printWorkflowListTable(defs []dag.WorkflowDef) {
	if len(defs) == 0 {
		panic("printWorkflowListTable: defs must not be empty")
	}
	if len(defs) > 100000 {
		panic("printWorkflowListTable: defs exceeds max bound")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tSTEPS\tTIMEOUT")

	for _, def := range defs {
		timeout := "none"
		if def.Timeout > 0 {
			timeout = def.Timeout.String()
		}
		fmt.Fprintf(
			w, "%s\t%d\t%s\n",
			def.Name, len(def.Steps), timeout,
		)
	}

	w.Flush()
}

// workflowRegisterResult is the JSON output for workflow register.
type workflowRegisterResult struct {
	Name   string `json:"name"`
	Action string `json:"action"`
	Steps  int    `json:"steps"`
}

// runWorkflowRegisterCmd reads a workflow definition file and
// registers it.
func runWorkflowRegisterCmd(args []string) {
	jsonOutput := HasJSONFlag(args)
	if jsonOutput {
		args = StripJSONFlag(args)
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats workflow register <file> [--json]")
		os.Exit(1)
	}
	filePath := args[0]
	if filePath == "" {
		panic(
			"runWorkflowRegisterCmd: filePath must not be empty",
		)
	}

	def := readWorkflowDef(filePath)

	svc, nc := connectService()
	defer nc.Close()

	// Check whether this workflow already exists to distinguish
	// create from update in user feedback.
	_, getErr := svc.GetWorkflow(def.Name)
	isUpdate := getErr == nil

	err := svc.RegisterWorkflow(context.Background(), def)
	if err != nil {
		fmt.Fprintf(
			os.Stderr, "register workflow: %v\n", err,
		)
		os.Exit(1)
	}

	action := "created"
	if isUpdate {
		action = "updated"
	}

	if jsonOutput {
		result := workflowRegisterResult{
			Name:   def.Name,
			Action: action,
			Steps:  len(def.Steps),
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(
				os.Stderr, "format json: %v\n", err,
			)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Workflow %s: %s (%d steps)\n",
		action, def.Name, len(def.Steps))
}

// readWorkflowDef reads and parses a workflow JSON file. Exits on
// error since this is a CLI helper.
func readWorkflowDef(filePath string) dag.WorkflowDef {
	if filePath == "" {
		panic("readWorkflowDef: filePath must not be empty")
	}
	if len(filePath) > 4096 {
		panic("readWorkflowDef: filePath unreasonably long")
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read file: %v\n", err)
		os.Exit(1)
	}

	var def dag.WorkflowDef
	if err := json.Unmarshal(data, &def); err != nil {
		fmt.Fprintf(
			os.Stderr, "parse workflow: %v\n", err,
		)
		os.Exit(1)
	}

	return def
}

// cli/workflow_validate.go
// Offline validation of workflow definition files. Reads JSON from disk,
// unmarshals, and runs dag.Validate — no NATS connection required.
package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/danmestas/dagnats/dag"
)

// runWorkflowValidateCmd validates a workflow JSON file without NATS.
func runWorkflowValidateCmd(args []string) {
	if args == nil {
		panic("runWorkflowValidateCmd: args must not be nil")
	}
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats workflow validate <file>")
		os.Exit(1)
	}
	filePath := args[0]
	if filePath == "" {
		panic(
			"runWorkflowValidateCmd: filePath must not be empty",
		)
	}

	result, err := validateWorkflowFile(filePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(result)
}

// validateWorkflowFile reads, parses, and validates a workflow JSON
// file. Returns a human-readable success message or an error. Separating
// this from the CLI wrapper enables direct testing without os.Exit.
func validateWorkflowFile(filePath string) (string, error) {
	if filePath == "" {
		panic("validateWorkflowFile: filePath must not be empty")
	}
	if len(filePath) > 4096 {
		panic("validateWorkflowFile: filePath unreasonably long")
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}

	var def dag.WorkflowDef
	if err := json.Unmarshal(data, &def); err != nil {
		return "", fmt.Errorf("parse workflow: %w", err)
	}

	if err := dag.Validate(def); err != nil {
		return "", fmt.Errorf("Invalid: %w", err)
	}

	return fmt.Sprintf(
		"Valid: %s (%d steps)", def.Name, len(def.Steps),
	), nil
}

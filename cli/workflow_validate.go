// cli/workflow_validate.go
// Offline validation of workflow definition files. Reads JSON from disk,
// unmarshals, and runs dag.Validate — no NATS connection required.
package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/dag"
)

// workflowValidateResult is the JSON output for workflow validate.
type workflowValidateResult struct {
	Valid bool   `json:"valid"`
	Name  string `json:"name,omitempty"`
	Steps int    `json:"steps,omitempty"`
	Error string `json:"error,omitempty"`
}

// runWorkflowValidateCmd validates a workflow JSON file without NATS.
func runWorkflowValidateCmd(args []string) {
	if args == nil {
		panic("runWorkflowValidateCmd: args must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	if jsonOutput {
		args = StripJSONFlag(args)
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats workflow validate <file> [--json]")
		os.Exit(1)
	}
	filePath := args[0]
	if filePath == "" {
		panic(
			"runWorkflowValidateCmd: filePath must not be empty",
		)
	}

	if jsonOutput {
		runWorkflowValidateJSON(filePath)
		return
	}

	result, err := validateWorkflowFile(filePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(result)
}

// runWorkflowValidateJSON outputs validation result as JSON.
// Does not os.Exit(1) on validation failure so JSON consumers
// can parse the structured error.
func runWorkflowValidateJSON(filePath string) {
	if filePath == "" {
		panic(
			"runWorkflowValidateJSON: filePath must not be empty",
		)
	}
	if len(filePath) > 4096 {
		panic(
			"runWorkflowValidateJSON: filePath unreasonably long",
		)
	}

	def, err := parseAndValidateWorkflow(filePath)
	if err != nil {
		out := workflowValidateResult{
			Valid: false,
			Error: err.Error(),
		}
		if fmtErr := FormatJSON(os.Stdout, out); fmtErr != nil {
			fmt.Fprintf(
				os.Stderr, "format json: %v\n", fmtErr,
			)
			os.Exit(1)
		}
		return
	}

	out := workflowValidateResult{
		Valid: true,
		Name:  def.Name,
		Steps: len(def.Steps),
	}
	if fmtErr := FormatJSON(os.Stdout, out); fmtErr != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", fmtErr)
		os.Exit(1)
	}
}

// parseAndValidateWorkflow reads, parses, and validates a workflow
// JSON file. Returns the parsed def or an error.
func parseAndValidateWorkflow(
	filePath string,
) (dag.WorkflowDef, error) {
	if filePath == "" {
		panic(
			"parseAndValidateWorkflow: filePath must not be empty",
		)
	}
	if len(filePath) > 4096 {
		panic(
			"parseAndValidateWorkflow: filePath unreasonably long",
		)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return dag.WorkflowDef{}, fmt.Errorf(
			"read file: %w", err,
		)
	}

	wf, err := parseWorkflowFile(data)
	if err != nil {
		return dag.WorkflowDef{}, fmt.Errorf(
			"parse workflow: %w", err,
		)
	}

	if err := dag.Validate(wf.WorkflowDef); err != nil {
		return dag.WorkflowDef{}, fmt.Errorf(
			"invalid: %w", err,
		)
	}

	// Validate embedded triggers (#180): same gate as register, so
	// `workflow validate` catches malformed cron / mismatched
	// workflow_id offline.
	if err := validateEmbeddedTriggers(&wf); err != nil {
		return dag.WorkflowDef{}, err
	}

	return wf.WorkflowDef, nil
}

// validateWorkflowFile reads, parses, and validates a workflow JSON
// file. Returns a human-readable success message or an error.
// Separating this from the CLI wrapper enables direct testing
// without os.Exit.
func validateWorkflowFile(
	filePath string,
) (string, error) {
	if filePath == "" {
		panic(
			"validateWorkflowFile: filePath must not be empty",
		)
	}
	if len(filePath) > 4096 {
		panic(
			"validateWorkflowFile: filePath unreasonably long",
		)
	}

	def, err := parseAndValidateWorkflow(filePath)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"Valid: %s (%d steps)", def.Name, len(def.Steps),
	), nil
}

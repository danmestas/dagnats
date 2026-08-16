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
//
// Warnings carries the structured {kind,message} ADR-013 respond-
// reachability warnings (issue #613). The field has no omitempty: the
// acceptance criterion requires the array to be present even when empty
// so JSON consumers can iterate it unconditionally.
type workflowValidateResult struct {
	Valid    bool          `json:"valid"`
	Name     string        `json:"name,omitempty"`
	Steps    int           `json:"steps,omitempty"`
	Error    string        `json:"error,omitempty"`
	Warnings []dag.Warning `json:"warnings"`
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

	wf, err := parseAndValidateWorkflow(filePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Valid: %s (%d steps)\n", wf.Name, len(wf.Steps))
	printRespondWarnings(respondWarnings(wf))
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

	wf, err := parseAndValidateWorkflow(filePath)
	if err != nil {
		out := workflowValidateResult{
			Valid:    false,
			Error:    err.Error(),
			Warnings: []dag.Warning{},
		}
		if fmtErr := FormatJSON(os.Stdout, out); fmtErr != nil {
			fmt.Fprintf(
				os.Stderr, "format json: %v\n", fmtErr,
			)
			os.Exit(1)
		}
		return
	}

	warnings := respondWarnings(wf)
	if warnings == nil {
		warnings = []dag.Warning{}
	}
	out := workflowValidateResult{
		Valid:    true,
		Name:     wf.Name,
		Steps:    len(wf.Steps),
		Warnings: warnings,
	}
	if fmtErr := FormatJSON(os.Stdout, out); fmtErr != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", fmtErr)
		os.Exit(1)
	}
}

// parseAndValidateWorkflow reads, parses, and validates a workflow JSON
// file. Returns the parsed file (definition + embedded triggers) or an
// error. The triggers are retained so callers can compute offline
// respond-reachability warnings from the file's own HTTP trigger
// (issue #613) without a NATS round-trip.
func parseAndValidateWorkflow(
	filePath string,
) (workflowFile, error) {
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
		return workflowFile{}, fmt.Errorf(
			"read file: %w", err,
		)
	}

	wf, err := parseWorkflowFile(data)
	if err != nil {
		return workflowFile{}, fmt.Errorf(
			"parse workflow: %w", err,
		)
	}

	if err := dag.Validate(wf.WorkflowDef); err != nil {
		return workflowFile{}, fmt.Errorf(
			"invalid: %w", err,
		)
	}

	// Validate embedded triggers (#180): same gate as register, so
	// `workflow validate` catches malformed cron / mismatched
	// workflow_id offline.
	if err := validateEmbeddedTriggers(&wf); err != nil {
		return workflowFile{}, err
	}

	return wf, nil
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

	wf, err := parseAndValidateWorkflow(filePath)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"Valid: %s (%d steps)", wf.Name, len(wf.Steps),
	), nil
}

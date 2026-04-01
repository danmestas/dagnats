// cli/workflow_show_stub.go
// Stubs for workflow show and validate commands. Will be replaced
// by the real implementations from the workflow agent.
package cli

import (
	"fmt"
	"os"
)

// runWorkflowShowCmd is a placeholder until the workflow show
// implementation is ready.
func runWorkflowShowCmd(args []string) {
	if args == nil {
		panic("runWorkflowShowCmd: args must not be nil")
	}

	fmt.Fprintln(os.Stderr, "workflow show: not yet implemented")
	os.Exit(1)
}

// runWorkflowValidateCmd is a placeholder until the workflow validate
// implementation is ready.
func runWorkflowValidateCmd(args []string) {
	if args == nil {
		panic("runWorkflowValidateCmd: args must not be nil")
	}

	fmt.Fprintln(os.Stderr, "workflow validate: not yet implemented")
	os.Exit(1)
}

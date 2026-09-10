// cli/workflow_delete.go
// `dagnats workflow delete <name>` removes a registered workflow
// definition. It mirrors `trigger delete`: a --force/--json surface
// over api.Service.DeleteWorkflow, which owns both refusal guards --
// a workflow still referenced by a trigger (#607) and a workflow with
// a non-terminal run (#682) -- unless --force is passed. Deleting the
// definition never touches historical run records. The guards used to
// live partly here (trigger check only); they were pulled down into
// the service so REST inherits the identical contract (#682 review).
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/danmestas/dagnats/internal/api"
)

// workflowDeleteResult is the JSON output for `workflow delete`.
type workflowDeleteResult struct {
	Name   string `json:"name"`
	Action string `json:"action"`
}

// workflowDeleter is the slice of api.Service this command needs.
// Declared as an interface so the delete logic is testable and so the
// dependency surface stays small.
type workflowDeleter interface {
	DeleteWorkflow(ctx context.Context, name string, force bool) error
}

// runWorkflowDeleteCmd deletes a workflow via api.Service.
func runWorkflowDeleteCmd(args []string) {
	runWorkflowDeleteCmdWithWriter(args, os.Stdout)
}

// runWorkflowDeleteCmdWithWriter parses flags, connects, and delegates
// to deleteWorkflow, translating its errors into exit codes: 2 for
// either refusal guard (trigger-reference or non-terminal-run, both
// recoverable via --force), 1 otherwise.
func runWorkflowDeleteCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic("runWorkflowDeleteCmdWithWriter: w must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	force, args := extractForceFlag(args)

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats workflow delete "+
				"<name> [--force] [--json]")
		os.Exit(1)
	}
	name := args[0]
	if name == "" {
		panic("runWorkflowDeleteCmdWithWriter: empty name")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := deleteWorkflow(context.Background(), svc, name, force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		var hasTriggers *api.ErrWorkflowHasTriggers
		var hasNonTerminalRuns *api.ErrWorkflowHasNonTerminalRuns
		if errors.As(err, &hasTriggers) || errors.As(err, &hasNonTerminalRuns) {
			os.Exit(2)
		}
		os.Exit(1)
	}

	if jsonOutput {
		FormatJSON(w, workflowDeleteResult{
			Name: name, Action: "deleted",
		})
		return
	}
	fmt.Fprintf(w, "Workflow deleted: %s\n", name)
}

// deleteWorkflow is a thin pass-through to svc.DeleteWorkflow -- both
// refusal guards (trigger-reference, non-terminal-run) live in the
// service itself (#682 review), so REST and the CLI share exactly one
// guarded-delete implementation instead of the CLI keeping a private
// copy of one of them.
func deleteWorkflow(
	ctx context.Context, svc workflowDeleter, name string, force bool,
) error {
	if ctx == nil {
		panic("deleteWorkflow: ctx must not be nil")
	}
	if svc == nil {
		panic("deleteWorkflow: svc must not be nil")
	}
	return svc.DeleteWorkflow(ctx, name, force)
}

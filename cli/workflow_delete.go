// cli/workflow_delete.go
// `dagnats workflow delete <name>` removes a registered workflow
// definition (#607). It mirrors `trigger delete`: a --force/--json
// surface plus a refusal guard. The guard refuses to delete a workflow
// that still has triggers referencing it (they would keep firing a
// now-missing definition) unless --force is passed. Deleting the
// definition never touches historical run records.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/danmestas/dagnats/internal/trigger"
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
	ListTriggers(ctx context.Context) ([]trigger.TriggerDef, error)
	DeleteWorkflow(ctx context.Context, name string) error
}

// workflowTriggerRefusalError is returned when a delete is refused
// because triggers still reference the workflow and --force was not
// passed. A distinct type lets the CLI wrapper map it to exit code 2,
// matching `trigger delete`'s file-managed refusal.
type workflowTriggerRefusalError struct {
	Workflow   string
	TriggerIDs []string
}

func (e *workflowTriggerRefusalError) Error() string {
	if e.Workflow == "" {
		panic("workflowTriggerRefusalError: Workflow must not be empty")
	}
	if len(e.TriggerIDs) == 0 {
		panic("workflowTriggerRefusalError: TriggerIDs must not be empty")
	}
	return fmt.Sprintf(
		"refused: workflow %q still has trigger(s) referencing it: %s."+
			" Delete the trigger(s) or rerun with --force.",
		e.Workflow, strings.Join(e.TriggerIDs, ", "),
	)
}

// runWorkflowDeleteCmd deletes a workflow via api.Service.
func runWorkflowDeleteCmd(args []string) {
	runWorkflowDeleteCmdWithWriter(args, os.Stdout)
}

// runWorkflowDeleteCmdWithWriter parses flags, connects, and delegates
// to deleteWorkflow, translating its errors into exit codes: 2 for the
// referencing-trigger refusal (recoverable via --force), 1 otherwise.
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
		var refusal *workflowTriggerRefusalError
		if errors.As(err, &refusal) {
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

// deleteWorkflow runs the guarded delete: unless force is set, it
// refuses when any trigger still references name. On the go-ahead it
// removes the definition (which errors if name is unregistered).
func deleteWorkflow(
	ctx context.Context, svc workflowDeleter, name string, force bool,
) error {
	if ctx == nil {
		panic("deleteWorkflow: ctx must not be nil")
	}
	if svc == nil {
		panic("deleteWorkflow: svc must not be nil")
	}
	if !force {
		refs, err := referencingTriggerIDs(ctx, svc, name)
		if err != nil {
			return err
		}
		if len(refs) > 0 {
			return &workflowTriggerRefusalError{
				Workflow: name, TriggerIDs: refs,
			}
		}
	}
	return svc.DeleteWorkflow(ctx, name)
}

// referencingTriggerIDs returns the IDs of triggers whose WorkflowID
// matches name. An empty triggers bucket (no keys) is the benign
// "nothing references it" case, reported as no references.
func referencingTriggerIDs(
	ctx context.Context, svc workflowDeleter, name string,
) ([]string, error) {
	if name == "" {
		panic("referencingTriggerIDs: name must not be empty")
	}
	if svc == nil {
		panic("referencingTriggerIDs: svc must not be nil")
	}
	defs, err := svc.ListTriggers(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no keys found") {
			return nil, nil
		}
		return nil, fmt.Errorf("list triggers: %w", err)
	}
	const maxRefs = 10000
	refs := make([]string, 0)
	for i, def := range defs {
		if i >= maxRefs {
			break
		}
		if def.WorkflowID == name {
			refs = append(refs, def.ID)
		}
	}
	return refs, nil
}

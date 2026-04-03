// cli/run_retry.go
// Re-run a workflow by looking up an existing run's workflow ID and
// starting a fresh run. Useful after fixing worker code.
package cli

import (
	"context"
	"fmt"
	"os"
)

// runRetryResult is the JSON response for run retry.
type runRetryResult struct {
	OriginalRunID string `json:"original_run_id"`
	Workflow      string `json:"workflow"`
	NewRunID      string `json:"new_run_id"`
}

// runRetryCmd looks up an existing run and starts a new run of the
// same workflow. Accepts exactly 1 positional arg (run-id) and an
// optional second arg as input payload.
func runRetryCmd(args []string) {
	if args == nil {
		panic("runRetryCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runRetryCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	hasLast := HasLastFlag(args)
	args = StripLastFlag(args)

	var rawID string
	if len(args) >= 1 {
		rawID = args[0]
	} else if !hasLast {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats run retry"+
				" <run-id> [input] [--last] [--json]")
		exitFunc(1)
		return
	}

	var input []byte
	if len(args) > 1 {
		input = []byte(args[1])
	}

	svc, nc := connectService()
	defer nc.Close()

	runID, err := ResolveRunID(svc, rawID, hasLast)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve run: %v\n", err)
		exitFunc(1)
		return
	}

	ctx := context.Background()
	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run: %v\n", err)
		exitFunc(1)
		return
	}

	newRunID, err := svc.StartRun(
		ctx, run.WorkflowID, input,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start run: %v\n", err)
		exitFunc(1)
		return
	}

	if jsonOutput {
		result := runRetryResult{
			OriginalRunID: runID,
			Workflow:      run.WorkflowID,
			NewRunID:      newRunID,
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			exitFunc(1)
			return
		}
		return
	}

	fmt.Printf("Retrying workflow %s: %s\n",
		run.WorkflowID, newRunID)
}

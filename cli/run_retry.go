// cli/run_retry.go
// Re-run a workflow by looking up an existing run's workflow ID and
// starting a fresh run. Useful after fixing worker code.
package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/danmestas/dagnats/api"
)

// runRetryResult is the JSON response for run retry.
type runRetryResult struct {
	OriginalRunID string `json:"original_run_id"`
	Workflow      string `json:"workflow"`
	NewRunID      string `json:"new_run_id"`
}

// retryOutcome holds the result of retryRun for display.
type retryOutcome struct {
	WorkflowID string
	NewRunID   string
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

	var explicitInput []byte
	hasExplicitInput := len(args) > 1
	if hasExplicitInput {
		explicitInput = []byte(args[1])
	}

	svc, nc := connectService()
	defer nc.Close()

	runID, err := ResolveRunID(svc, rawID, hasLast)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve run: %v\n", err)
		exitFunc(1)
		return
	}

	outcome, err := retryRun(
		svc, runID, explicitInput, hasExplicitInput,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		exitFunc(1)
		return
	}

	printRetryResult(jsonOutput, runID, outcome)
}

// retryRun loads the original run, resolves input, and starts a new
// run. Explicit input overrides the original run's stored input.
func retryRun(
	svc *api.Service,
	runID string,
	explicitInput []byte,
	hasExplicitInput bool,
) (retryOutcome, error) {
	if svc == nil {
		panic("retryRun: svc must not be nil")
	}
	if runID == "" {
		panic("retryRun: runID must not be empty")
	}
	ctx := context.Background()
	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		return retryOutcome{}, fmt.Errorf("get run: %w", err)
	}

	input := explicitInput
	if !hasExplicitInput && len(run.Input) > 0 {
		input = run.Input
	}

	newRunID, err := svc.StartRun(
		ctx, run.WorkflowID, input,
	)
	if err != nil {
		return retryOutcome{},
			fmt.Errorf("start run: %w", err)
	}
	return retryOutcome{
		WorkflowID: run.WorkflowID,
		NewRunID:   newRunID,
	}, nil
}

// printRetryResult outputs the retry result in JSON or plain text.
func printRetryResult(
	jsonOutput bool, runID string, out retryOutcome,
) {
	if !jsonOutput {
		fmt.Printf("Retrying workflow %s: %s\n",
			out.WorkflowID, out.NewRunID)
		return
	}
	result := runRetryResult{
		OriginalRunID: runID,
		Workflow:      out.WorkflowID,
		NewRunID:      out.NewRunID,
	}
	if err := FormatJSON(os.Stdout, result); err != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", err)
		exitFunc(1)
	}
}

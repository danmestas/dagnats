// cli/resolve.go
// Resolves short run ID prefixes and --last flag to full 32-char run IDs.
// Enables prefix matching (min 8 chars) and "most recent run" shortcuts
// so users avoid copy-pasting full hex strings between commands.
package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// ResolveRunID resolves a run ID from user input. It supports:
//   - Full 32-char IDs (returned as-is, no lookup)
//   - Short prefixes (8-31 chars, matched against existing runs)
//   - --last flag (returns the most recently created run)
func ResolveRunID(
	svc *api.Service, input string, hasLastFlag bool,
) (string, error) {
	if svc == nil {
		panic("ResolveRunID: svc must not be nil")
	}
	if len(input) > 4096 {
		panic("ResolveRunID: input exceeds max bound")
	}

	if input == "" && !hasLastFlag {
		return "", fmt.Errorf("run ID required")
	}

	// Full 32-char ID: no lookup needed.
	if len(input) == 32 {
		return input, nil
	}

	// Prefix too short to be useful.
	if len(input) > 0 && len(input) < 8 {
		return "", fmt.Errorf(
			"run ID prefix must be at least 8 characters",
		)
	}

	return resolveFromList(svc, input, hasLastFlag)
}

// resolveFromList fetches runs and matches by --last or prefix.
func resolveFromList(
	svc *api.Service, input string, hasLastFlag bool,
) (string, error) {
	if svc == nil {
		panic("resolveFromList: svc must not be nil")
	}
	if !hasLastFlag && input == "" {
		panic("resolveFromList: must have input or lastFlag")
	}

	runs, err := svc.ListRuns(context.Background(), "")
	if err != nil {
		return "", fmt.Errorf("list runs: %w", err)
	}

	if hasLastFlag && input == "" {
		return resolveLastRun(runs)
	}

	return resolvePrefixMatch(runs, input)
}

// resolveLastRun returns the run ID with the newest CreatedAt.
// ListRuns returns runs sorted newest-first, so index 0 is newest.
func resolveLastRun(
	runs []dag.WorkflowRun,
) (string, error) {
	if runs == nil {
		panic("resolveLastRun: runs must not be nil")
	}
	if len(runs) > 1000 {
		panic("resolveLastRun: runs exceeds max bound")
	}

	if len(runs) == 0 {
		return "", fmt.Errorf("no runs found")
	}

	return runs[0].RunID, nil
}

// resolvePrefixMatch finds runs matching the given prefix.
// Returns the full run ID if exactly one matches.
func resolvePrefixMatch(
	runs []dag.WorkflowRun, prefix string,
) (string, error) {
	if len(runs) > 1000 {
		panic("resolvePrefixMatch: runs exceeds max bound")
	}
	if prefix == "" {
		panic("resolvePrefixMatch: prefix must not be empty")
	}

	var matched string
	matchCount := 0

	for _, run := range runs {
		if strings.HasPrefix(run.RunID, prefix) {
			matched = run.RunID
			matchCount++
		}
	}

	if matchCount == 0 {
		return "", fmt.Errorf(
			"no run matching prefix %q", prefix,
		)
	}
	if matchCount > 1 {
		return "", fmt.Errorf(
			"ambiguous prefix %q: %d runs match",
			prefix, matchCount,
		)
	}

	return matched, nil
}

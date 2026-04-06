// cli/hint.go
// Guided next-step hints printed to stderr after key commands.
// Hints are suppressed in JSON mode so they never interfere with
// machine-readable output or piped workflows.
package cli

import (
	"fmt"
	"os"
)

// printHint writes guided next-step lines to stderr. Each line is
// indented with two spaces for visual separation from command output.
// No-op when jsonOutput is true, keeping machine output clean.
func printHint(jsonOutput bool, lines ...string) {
	if len(lines) == 0 {
		panic("printHint: lines must not be empty")
	}
	const maxLines = 100
	if len(lines) > maxLines {
		panic("printHint: lines exceeds max bound")
	}

	if jsonOutput {
		return
	}

	fmt.Fprintln(os.Stderr)
	for _, line := range lines {
		fmt.Fprintf(os.Stderr, "  %s\n", line)
	}
}

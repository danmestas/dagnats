// cli/run_watch_stub.go
// Stub for the run watch command. Will be replaced by the real
// implementation from the run agent.
package cli

import (
	"fmt"
	"os"
)

// runWatchCmd is a placeholder until the standalone run watch
// implementation is ready.
func runWatchCmd(args []string) {
	if args == nil {
		panic("runWatchCmd: args must not be nil")
	}

	fmt.Fprintln(os.Stderr, "run watch: not yet implemented")
	os.Exit(1)
}

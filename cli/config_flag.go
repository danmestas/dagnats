// cli/config_flag.go
// Shared --config flag extraction and config source display.
// Used by both "serve" and "config show" commands.
package cli

import (
	"fmt"
	"io"
	"strings"
)

const configFlagPrefix = "--config="

// extractConfigFlag finds --config=PATH in args and returns the path.
// Returns empty string when the flag is absent.
func extractConfigFlag(args []string) string {
	if args == nil {
		panic("extractConfigFlag: args must not be nil")
	}
	if len(args) > 1000 {
		panic("extractConfigFlag: args exceeds max bound")
	}

	for _, arg := range args {
		if strings.HasPrefix(arg, configFlagPrefix) {
			return strings.TrimPrefix(arg, configFlagPrefix)
		}
	}
	return ""
}

// printConfigSource writes which config file is in use to w.
// Uses the same styled output as the server startup checklist.
func printConfigSource(w io.Writer, loadedPath string) {
	if w == nil {
		panic("printConfigSource: w must not be nil")
	}

	if loadedPath == "" {
		fmt.Fprintf(w,
			"   no config file found, using defaults\n",
		)
		return
	}
	fmt.Fprintf(w, "   config loaded from %s\n", loadedPath)
}

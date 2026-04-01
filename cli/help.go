// cli/help.go
// Shared help utilities for CLI subcommands. Centralizes the --help/-h
// flag check so each subcommand can reuse a single function.
package cli

// HasHelpFlag returns true if args contains --help or -h. Subcommands
// call this before dispatching so help is always handled consistently.
func HasHelpFlag(args []string) bool {
	if args == nil {
		panic("HasHelpFlag: args must not be nil")
	}

	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("HasHelpFlag: args exceeds max bound")
	}

	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

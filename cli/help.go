// cli/help.go
// Shared help flag detection for all CLI commands. Keeps the manual
// dispatch pattern consistent while adding --help/-h support.
package cli

// HasHelpFlag returns true when args contains "--help" or "-h".
// Only exact matches count — "--helper" or "-help" do not trigger help.
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

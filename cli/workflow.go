package cli

import "fmt"

// runWorkflowCmd dispatches workflow subcommands. Stubs are placeholders until
// HTTP client integration is added in a later task.
func runWorkflowCmd(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: dagnats workflow <list|register>")
		return
	}
	switch args[0] {
	case "list":
		fmt.Println("(workflow list not yet implemented)")
	case "register":
		fmt.Println("(workflow register not yet implemented)")
	default:
		fmt.Printf("unknown workflow subcommand: %s\n", args[0])
	}
}

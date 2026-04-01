// cli/version.go
// Version output for the CLI. Version is settable via ldflags at build
// time: go build -ldflags "-X github.com/danmestas/dagnats/cli.Version=1.0.0"
package cli

import "fmt"

// Version is the current CLI version. Set via ldflags at build time;
// defaults to "dev" for local development builds.
var Version = "dev"

// printVersion prints the version string to stdout.
func printVersion() {
	if Version == "" {
		panic("printVersion: Version must not be empty")
	}

	fmt.Printf("dagnats %s\n", Version)
}

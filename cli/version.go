// cli/version.go
// Version output for the CLI. The version string lives in
// internal/buildinfo, stamped via ldflags at build time.
package cli

import (
	"fmt"

	"github.com/danmestas/dagnats/internal/buildinfo"
)

// printVersion prints the version string to stdout.
func printVersion() {
	if buildinfo.Version == "" {
		panic("printVersion: Version must not be empty")
	}
	if len(buildinfo.Version) > 100 {
		panic("printVersion: Version string unreasonably long")
	}

	fmt.Printf("dagnats %s\n", buildinfo.Version)
}

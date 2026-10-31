// Package buildinfo holds the dagnats build version. It sits below cli
// and server so both, and the program package's version-range check,
// can read it without an import cycle. Set it at build time:
//
//	go build -ldflags "-X github.com/danmestas/dagnats/internal/buildinfo.Version=1.0.0"
package buildinfo

// Version is the dagnats build version. Set via ldflags at build time;
// "dev" for local development builds.
var Version = "dev"

// worker/identity.go
// Process identity helpers used to populate WorkerRegistration's Pid /
// Hostname / Version fields (#289). Resolved once per process and
// cached, since none of these can change mid-run.
package worker

import (
	"os"
	"runtime/debug"
	"sync"
)

// processIdentity is the cached host/version pair for this worker
// process. Resolved lazily once and reused for every Register call.
type processIdentity struct {
	hostname string
	version  string
	pid      int
}

var (
	identityOnce sync.Once
	identity     processIdentity
)

// loadIdentity returns the cached identity for this process,
// resolving it on first call. Hostname falls back to "unknown" when
// os.Hostname fails (e.g. sandboxed environments without a hostname);
// Version falls back to "dev" when the binary was built without
// module info (e.g. `go run`).
func loadIdentity() processIdentity {
	identityOnce.Do(func() {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		identity = processIdentity{
			hostname: host,
			version:  resolveBuildVersion(),
			pid:      os.Getpid(),
		}
	})
	return identity
}

// resolveBuildVersion reads the module version embedded by `go
// build`. Returns "dev" when running under `go test` or `go run`,
// which is the same fallback observe/setup.go uses.
func resolveBuildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	v := info.Main.Version
	if v == "" || v == "(devel)" {
		return "dev"
	}
	return v
}

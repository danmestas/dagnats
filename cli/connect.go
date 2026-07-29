// cli/connect.go
// Single connection point for all CLI commands. Establishes NATS connection
// and wraps it in an api.Service for uniform access to control plane operations.
package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/internal/api"

	"github.com/nats-io/nats.go"
)

// exitFunc is the function called on fatal errors. Replaced in tests.
var exitFunc = os.Exit

// SwapExitFunc replaces the package-level exit hook and returns the
// prior value. Test fixtures use it to intercept os.Exit so a
// non-zero exit from in-process cli.Run does not terminate the test
// runner. Always pair with a deferred SwapExitFunc(prev) to restore.
func SwapExitFunc(next func(int)) func(int) {
	if next == nil {
		panic("SwapExitFunc: next must not be nil")
	}
	prev := exitFunc
	exitFunc = next
	return prev
}

// connectService creates an api.Service bound to NATS. Prints a
// friendly error and exits with code 1 if connection fails or
// required NATS resources are missing.
func connectService() (*api.Service, *nats.Conn) {
	natsURL := GetEnvWithFallback(
		"DAGNATS_NATS_URL", "NATS_URL", nats.DefaultURL,
	)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: cannot connect to NATS at %s\n"+
				"Hint: run 'dagnats serve' to start the server\n",
			natsURL)
		exitFunc(1)
		return nil, nil
	}
	svc, initErr := tryNewService(nc)
	if initErr != "" {
		nc.Close()
		fmt.Fprintf(os.Stderr,
			"Error: %s\n"+
				"Hint: run 'dagnats serve' to start the server\n",
			initErr)
		exitFunc(1)
		return nil, nil
	}
	return svc, nc
}

// tryNewService probes for the NATS resources api.NewService requires
// and constructs the service only once they are provisioned. Returns
// the service on success, or a friendly error message for the
// operationally-expected "server not provisioned yet" case -- the CLI
// may run before `dagnats serve` finishes bootstrapping. A nil nc is a
// programmer error and panics.
func tryNewService(
	nc *nats.Conn,
) (svc *api.Service, errMsg string) {
	if nc == nil {
		panic("tryNewService: nc must not be nil")
	}
	if err := api.ResourcesReady(nc); err != nil {
		return nil, err.Error()
	}
	return api.NewService(nc), ""
}

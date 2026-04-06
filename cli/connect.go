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

// tryNewService wraps api.NewService and recovers from panics that
// occur when required NATS resources (KV buckets, JetStream) are
// missing. Returns the service on success or an error message.
// NOTE: This recover-from-panic pattern is a pragmatic workaround.
// api.NewService panics on missing resources (TigerStyle: programmer
// error). Long-term, NewService should return (*Service, error) for
// conditions that are operationally expected (server not running).
func tryNewService(
	nc *nats.Conn,
) (svc *api.Service, errMsg string) {
	if nc == nil {
		panic("tryNewService: nc must not be nil")
	}
	defer func() {
		if r := recover(); r != nil {
			errMsg = fmt.Sprintf("%v", r)
		}
	}()
	svc = api.NewService(nc)
	return svc, ""
}

// cli/connect.go
// Single connection point for all CLI commands. Establishes NATS connection
// and wraps it in an api.Service for uniform access to control plane operations.
package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// connectService creates an api.Service bound to NATS. Exits with code 1
// if connection fails. Caller must close the returned nats.Conn when done.
func connectService() (*api.Service, *nats.Conn) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to NATS: %v\n", err)
		os.Exit(1)
	}
	svc := api.NewService(nc, observe.NewNoopTelemetry())
	return svc, nc
}

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/danmestas/dagnats/cli"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func main() {
	url := cli.GetEnvWithFallback(
		"DAGNATS_NATS_URL", "NATS_URL", nats.DefaultURL,
	)
	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()
	err = natsutil.SetupAll(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup NATS resources: %v\n", err)
		os.Exit(1)
	}
	telShutdown, telErr := observe.InitTelemetry(
		context.Background(), observe.Config{
			ServiceName:  "dagnats-api",
			NATSConn:     nc,
			OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		},
	)
	if telErr != nil {
		fmt.Fprintf(os.Stderr, "failed to init telemetry: %v\n", telErr)
		os.Exit(1)
	}
	defer telShutdown(context.Background())
	svc := api.NewService(nc)
	natsAPI := api.NewNATSAPI(svc, nc, cli.Version)
	natsAPI.Start()
	// Must stay registered AFTER `defer nc.Close()` above: LIFO ordering
	// makes this run FIRST, so micro drains while the connection is still
	// open. Do not insert an nc.Drain()/nc.Close() defer below this line.
	defer natsAPI.Stop()
	handler := api.NewRESTHandler(svc)
	addr := cli.GetEnvWithFallback(
		"DAGNATS_LISTEN_ADDR", "LISTEN_ADDR", ":8080",
	)
	fmt.Printf("dagnats-api listening on %s\n", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

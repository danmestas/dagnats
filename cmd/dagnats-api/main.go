package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/danmestas/dagnats/cli"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/observe/simple"
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
	tel, shutdown := simple.SetupTelemetry(nc)
	defer shutdown()
	svc := api.NewService(nc, tel)
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

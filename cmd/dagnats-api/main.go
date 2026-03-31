package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe/simple"
	"github.com/danmestas/dagnats/ui"
	"github.com/nats-io/nats.go"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"failed to connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()
	err = natsutil.SetupAll(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"failed to setup NATS resources: %v\n", err)
		os.Exit(1)
	}
	tel, shutdown := simple.SetupTelemetry(nc)
	defer shutdown()
	svc := api.NewService(nc, tel)

	mux := http.NewServeMux()

	// Register REST API routes (/api/*).
	api.RegisterAPIRoutes(mux, svc)

	// Register dashboard UI (HTML pages + SSE + static).
	uiHandler := ui.NewHandler(svc)
	uiHandler.RegisterRoutes(mux)

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	fmt.Printf("dagnats-api listening on %s\n", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

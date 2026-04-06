package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/danmestas/dagnats/cli"
	"github.com/danmestas/dagnats/internal/engine"
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
	_, shutdown := simple.SetupTelemetry(nc)
	defer shutdown()
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	fmt.Println("dagnats-engine started")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down...")
	orch.Stop()
}

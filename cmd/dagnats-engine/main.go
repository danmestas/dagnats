package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/danmestas/dagnats/cli"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
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
	replicas, err := cli.JetStreamReplicasFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}
	// 0 lets SetupAll derive the factor from the cluster nc reached, so a
	// split binary never provisions a replicated cluster at one replica.
	err = natsutil.SetupAll(nc, natsutil.WithCluster(
		natsutil.ClusterOptions{ReplicasOverride: replicas},
	))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup NATS resources: %v\n", err)
		os.Exit(1)
	}
	telShutdown, telErr := observe.InitTelemetry(
		context.Background(), observe.Config{
			ServiceName:  "dagnats-engine",
			NATSConn:     nc,
			OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		},
	)
	if telErr != nil {
		fmt.Fprintf(os.Stderr, "failed to init telemetry: %v\n", telErr)
		os.Exit(1)
	}
	defer telShutdown(context.Background())
	// The sleep-timer consumer is shared with any `dagnats serve` nodes,
	// so this engine may receive debounce timers too.
	debounceFire, err := trigger.NewDebounceTimerHandler(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open debounce state: %v\n", err)
		os.Exit(1)
	}
	orch := engine.NewOrchestrator(nc, engine.WithDebounceHandler(debounceFire))
	if err := orch.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start orchestrator: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("dagnats-engine started")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down...")
	orch.Stop()
}

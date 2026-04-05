// cli/otlp_bridge.go
// CLI command that runs the OTLP bridge as a standalone process.
// Consumes from the NATS TELEMETRY stream and exports to an
// OTLP/HTTP endpoint until interrupted by SIGINT/SIGTERM.
package cli

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/internal/observe/otlp"
	"github.com/nats-io/nats.go"
)

// runOTLPBridgeCmd starts the OTLP bridge process. Blocks until
// interrupted. Reads endpoint from --endpoint flag or the
// OTEL_EXPORTER_OTLP_ENDPOINT environment variable.
func runOTLPBridgeCmd(args []string) {
	if args == nil {
		panic("runOTLPBridgeCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runOTLPBridgeCmd: args exceeds max bound")
	}

	if HasHelpFlag(args) {
		printOTLPBridgeHelp()
		return
	}

	endpoint := parseOTLPEndpoint(args)
	if endpoint == "" {
		fmt.Fprintln(os.Stderr,
			"Error: --endpoint or "+
				"OTEL_EXPORTER_OTLP_ENDPOINT required")
		exitFunc(1)
		return
	}

	natsURL := GetEnvWithFallback(
		"DAGNATS_NATS_URL", "NATS_URL", nats.DefaultURL,
	)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: cannot connect to NATS at %s\n",
			natsURL)
		exitFunc(1)
		return
	}
	defer nc.Close()

	cfg := otlp.BridgeConfig{
		Endpoint:      endpoint,
		BatchSize:     100,
		FlushInterval: 5 * time.Second,
		ServiceName:   "dagnats",
	}

	bridge := otlp.NewBridge(nc, cfg)
	bridge.Start()

	fmt.Fprintf(os.Stderr,
		"OTLP bridge started → %s\n", endpoint,
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Fprintln(os.Stderr, "Shutting down OTLP bridge...")
	bridge.Stop()
}

// parseOTLPEndpoint extracts the endpoint from --endpoint flag
// or falls back to OTEL_EXPORTER_OTLP_ENDPOINT env var.
func parseOTLPEndpoint(args []string) string {
	if args == nil {
		panic("parseOTLPEndpoint: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("parseOTLPEndpoint: args exceeds max bound")
	}

	for _, arg := range args {
		if strings.HasPrefix(arg, "--endpoint=") {
			return strings.TrimPrefix(
				arg, "--endpoint=",
			)
		}
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}

// printOTLPBridgeHelp prints usage for the otlp-bridge command.
func printOTLPBridgeHelp() {
	fmt.Println("Usage: dagnats otlp-bridge [flags]")
	fmt.Println()
	fmt.Println("Bridges the NATS TELEMETRY stream" +
		" to an OTLP/HTTP endpoint.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --endpoint=URL  " +
		"OTLP/HTTP endpoint (or OTEL_EXPORTER_OTLP_ENDPOINT)")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  dagnats otlp-bridge" +
		" --endpoint=http://localhost:4318")
}

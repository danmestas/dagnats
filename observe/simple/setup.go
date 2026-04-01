// observe/simple/setup.go
// SetupTelemetry wires together all simple telemetry components into a single
// observe.Telemetry. Zero-config: service name defaults to binary name,
// Jaeger export activates only if JAEGER_ENDPOINT is set.
package simple

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// SetupTelemetry creates the simple telemetry stack using the provided NATS
// connection. Returns a ready Telemetry bundle and a shutdown function that
// flushes in-flight spans and cancels background goroutines.
// Panics on nil nc — a programmer error that must surface immediately.
func SetupTelemetry(nc *nats.Conn) (*observe.Telemetry, func()) {
	if nc == nil {
		panic("SetupTelemetry: nc must not be nil")
	}
	if len(os.Args) == 0 {
		panic("SetupTelemetry: os.Args must not be empty")
	}
	serviceName := filepath.Base(os.Args[0])

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("SetupTelemetry: JetStream unavailable: %v", err)
		return observe.NewNoopTelemetry(), func() {}
	}

	metrics := NewMetricsCollector(js, serviceName)
	collector := NewTraceCollector(js, serviceName, metrics)
	logger := NewLogCollector(js, serviceName)
	errors := NewErrorReporter(collector, logger)

	ctx, cancel := context.WithCancel(context.Background())

	if endpoint := os.Getenv("JAEGER_ENDPOINT"); endpoint != "" {
		go ExportToJaeger(ctx, js, endpoint, logger)
	}

	shutdown := func() {
		cancel()
		collector.Flush()
	}

	return &observe.Telemetry{
		Tracer:  collector,
		Logger:  logger,
		Metrics: metrics,
		Errors:  errors,
	}, shutdown
}

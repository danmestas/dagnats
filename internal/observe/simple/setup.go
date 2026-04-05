// observe/simple/setup.go
// SetupTelemetry wires together all simple telemetry components into a single
// observe.Telemetry. Zero-config: service name defaults to binary name.
// Telemetry data is published to NATS JetStream for collection by an
// external OTel collector (e.g. SigNoz).
package simple

import (
	"log"
	"os"
	"path/filepath"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SetupTelemetry creates the simple telemetry stack using the
// provided NATS connection. Returns a ready Telemetry bundle and
// a shutdown function that flushes in-flight spans.
// Panics on nil nc -- a programmer error.
func SetupTelemetry(nc *nats.Conn) (*observe.Telemetry, func()) {
	if nc == nil {
		panic("SetupTelemetry: nc must not be nil")
	}
	if len(os.Args) == 0 {
		panic("SetupTelemetry: os.Args must not be empty")
	}
	serviceName := filepath.Base(os.Args[0])

	js, err := jetstream.New(nc)
	if err != nil {
		log.Printf(
			"SetupTelemetry: JetStream unavailable: %v", err,
		)
		return observe.NewNoopTelemetry(), func() {}
	}

	metrics := NewMetricsCollector(js, serviceName)
	collector := NewTraceCollector(js, serviceName, metrics)
	logger := NewLogCollector(js, serviceName)
	errors := NewErrorReporter(collector, logger)

	shutdown := func() {
		collector.Flush()
	}

	return &observe.Telemetry{
		Tracer:  collector,
		Logger:  logger,
		Metrics: metrics,
		Errors:  errors,
	}, shutdown
}

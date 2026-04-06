// config.go defines the telemetry configuration struct.
// Kept separate from setup.go so callers can construct
// Config without importing OTel SDK types.
package observe

import "github.com/nats-io/nats.go"

// Config controls how InitTelemetry wires OTel providers.
// ServiceName and NATSConn are required — InitTelemetry panics
// if either is zero-valued. OTLPEndpoint enables dual export
// to an OTLP/HTTP collector (e.g. SigNoz) when non-empty.
type Config struct {
	// ServiceName identifies this process in telemetry data.
	ServiceName string

	// NATSConn is used to create a JetStream context for the
	// NATS-backed exporters. Caller owns the connection.
	NATSConn *nats.Conn

	// OTLPEndpoint enables OTLP/HTTP export when non-empty.
	// Example: "100.105.156.92:4318".
	OTLPEndpoint string

	// Resource holds additional OTel resource attributes
	// merged into the auto-detected set (host, OS, process).
	Resource map[string]string
}

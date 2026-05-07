// config.go defines the telemetry configuration struct.
// Kept separate from setup.go so callers can construct
// Config without importing OTel SDK types.
package observe

import "github.com/nats-io/nats.go"

// Config controls how InitTelemetry wires OTel providers.
// ServiceName and NATSConn are required — InitTelemetry panics
// if either is zero-valued. OTLPEndpoint, when non-empty, gates
// whether OTLP/HTTP export is enabled at all; the actual
// endpoint, headers, TLS, protocol, and per-signal overrides
// come from the standard OTel SDK env vars (see #184).
type Config struct {
	// ServiceName identifies this process in telemetry data.
	ServiceName string

	// NATSConn is used to create a JetStream context for the
	// NATS-backed exporters. Caller owns the connection.
	NATSConn *nats.Conn

	// OTLPEndpoint is a sentinel that gates OTLP exporter
	// construction: when non-empty, dagnats constructs OTLP
	// exporters and the OTel SDK reads its standard env vars
	// (OTEL_EXPORTER_OTLP_ENDPOINT, _HEADERS, _PROTOCOL,
	// _INSECURE, per-signal variants, etc.) for the actual
	// endpoint and transport configuration. The string value
	// itself is no longer passed to the SDK.
	OTLPEndpoint string

	// Resource holds additional OTel resource attributes
	// merged into the auto-detected set (host, OS, process).
	Resource map[string]string
}

// setup.go is the single entry point for OTel provider setup.
// One call to InitTelemetry wires tracing, metrics, and logging
// with NATS-backed exporters (always) and OTLP/HTTP exporters
// (when configured). This is a deep module: rich behavior behind
// a minimal interface.
package observe

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/danmestas/dagnats/observe/natsexporter"
)

// InitTelemetry creates and registers OTel TracerProvider,
// MeterProvider, and LoggerProvider. Returns a shutdown function
// that flushes and closes all three providers. Panics on
// programmer errors (nil conn, empty service name).
func InitTelemetry(
	ctx context.Context, cfg Config,
) (func(context.Context), error) {
	if cfg.NATSConn == nil {
		panic("InitTelemetry: NATSConn must not be nil")
	}
	if cfg.ServiceName == "" {
		panic("InitTelemetry: ServiceName must not be empty")
	}

	js, err := jetstream.New(cfg.NATSConn)
	if err != nil {
		return nil, fmt.Errorf("create jetstream: %w", err)
	}

	res, err := buildResource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	tp, err := setupTracerProvider(ctx, js, res, cfg)
	if err != nil {
		return nil, fmt.Errorf("tracer provider: %w", err)
	}

	mp, err := setupMeterProvider(ctx, js, res, cfg)
	if err != nil {
		shutdownSafe(ctx, tp)
		return nil, fmt.Errorf("meter provider: %w", err)
	}

	lp, err := setupLoggerProvider(ctx, js, res, cfg)
	if err != nil {
		shutdownSafe(ctx, tp)
		shutdownSafe(ctx, mp)
		return nil, fmt.Errorf("logger provider: %w", err)
	}

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	EnsureDefaultPropagator()

	shutdown := func(ctx context.Context) {
		shutdownSafe(ctx, tp)
		shutdownSafe(ctx, mp)
		shutdownSafe(ctx, lp)
	}
	return shutdown, nil
}

// shutdownable abstracts the Shutdown method shared by all
// three OTel SDK providers.
type shutdownable interface {
	Shutdown(ctx context.Context) error
}

// shutdownSafe calls Shutdown and ignores the error — used
// during cleanup where we cannot propagate errors.
func shutdownSafe(ctx context.Context, s shutdownable) {
	if s == nil {
		return
	}
	_ = s.Shutdown(ctx)
}

// buildResource constructs the OTel resource using the detector pipeline
// so that OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME are honored for
// all emitters (primarily OTLP to collectors like SigNoz). Precedence:
// cfg.Resource (caller) > env > our built-in attrs (incl. cfg.ServiceName).
// Partial detector errors (e.g. malformed env var) are handled via otel.Handle
// and a usable resource is still returned.
func buildResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	if cfg.ServiceName == "" {
		panic("buildResource: ServiceName must not be empty")
	}

	version, goVersion := extractBuildInfo()
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	builtins := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(version),
		semconv.HostName(hostname),
		semconv.HostArchKey.String(runtime.GOARCH),
		semconv.OSTypeKey.String(runtime.GOOS),
		attribute.Int("process.pid", os.Getpid()),
		attribute.String("process.runtime.name", "go"),
		attribute.String("process.runtime.version", goVersion),
	}

	caller := make([]attribute.KeyValue, 0, len(cfg.Resource))
	for k, v := range cfg.Resource {
		caller = append(caller, attribute.String(k, v))
	}

	res, err := resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithFromEnv(),
		resource.WithAttributes(builtins...),
		resource.WithAttributes(caller...),
	)
	if err != nil {
		otel.Handle(err)
		if res == nil {
			return nil, fmt.Errorf("build resource: %w", err)
		}
	}
	return res, nil
}

// extractBuildInfo reads version and Go version from the
// embedded build info. Returns "unknown" when unavailable.
func extractBuildInfo() (version, goVersion string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown", "unknown"
	}
	version = info.Main.Version
	if version == "" || version == "(devel)" {
		version = "dev"
	}
	goVersion = info.GoVersion
	if goVersion == "" {
		goVersion = "unknown"
	}
	return version, goVersion
}

// setupTracerProvider creates a TracerProvider with NATS and
// optional OTLP span exporters.
func setupTracerProvider(
	ctx context.Context,
	js jetstream.JetStream,
	res *resource.Resource,
	cfg Config,
) (*trace.TracerProvider, error) {
	if js == nil {
		panic("setupTracerProvider: js must not be nil")
	}
	if res == nil {
		panic("setupTracerProvider: res must not be nil")
	}

	natsExp := natsexporter.NewSpanExporter(js)
	opts := []trace.TracerProviderOption{
		trace.WithResource(res),
		trace.WithBatcher(natsExp),
	}

	if cfg.OTLPEndpoint != "" {
		// No explicit endpoint or transport-security options:
		// the OTel SDK reads the standard env vars
		// (OTEL_EXPORTER_OTLP_ENDPOINT, _HEADERS, _PROTOCOL,
		// _INSECURE, per-signal variants, etc.) directly. See
		// #184 for why dagnats no longer wraps these knobs.
		otlpExp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otlp trace: %w", err)
		}
		opts = append(opts, trace.WithBatcher(otlpExp))
	}

	return trace.NewTracerProvider(opts...), nil
}

// setupMeterProvider creates a MeterProvider with NATS and
// optional OTLP metric exporters.
func setupMeterProvider(
	ctx context.Context,
	js jetstream.JetStream,
	res *resource.Resource,
	cfg Config,
) (*metric.MeterProvider, error) {
	if js == nil {
		panic("setupMeterProvider: js must not be nil")
	}
	if res == nil {
		panic("setupMeterProvider: res must not be nil")
	}

	natsExp := natsexporter.NewMetricExporter(js)
	// Collect every 10s rather than the SDK's 60s default: the console
	// charts need a dense-enough cumulative series to draw a meaningful
	// line over the recent window. One point/minute leaves the graphs
	// near-empty on a fresh process.
	const natsMetricInterval = 10 * time.Second
	opts := []metric.Option{
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(
			natsExp, metric.WithInterval(natsMetricInterval),
		)),
	}

	if cfg.OTLPEndpoint != "" {
		// SDK reads env vars; see #184.
		otlpExp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otlp metric: %w", err)
		}
		opts = append(opts, metric.WithReader(
			metric.NewPeriodicReader(otlpExp),
		))
	}

	return metric.NewMeterProvider(opts...), nil
}

// setupLoggerProvider creates a LoggerProvider with NATS and
// optional OTLP log exporters.
func setupLoggerProvider(
	ctx context.Context,
	js jetstream.JetStream,
	res *resource.Resource,
	cfg Config,
) (*log.LoggerProvider, error) {
	if js == nil {
		panic("setupLoggerProvider: js must not be nil")
	}
	if res == nil {
		panic("setupLoggerProvider: res must not be nil")
	}

	natsExp := natsexporter.NewLogExporter(
		js,
	)
	opts := []log.LoggerProviderOption{
		log.WithResource(res),
		log.WithProcessor(log.NewBatchProcessor(natsExp)),
	}

	if cfg.OTLPEndpoint != "" {
		// SDK reads env vars; see #184.
		otlpExp, err := otlploghttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otlp log: %w", err)
		}
		opts = append(opts, log.WithProcessor(
			log.NewBatchProcessor(otlpExp),
		))
	}

	return log.NewLoggerProvider(opts...), nil
}

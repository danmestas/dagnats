// cli/observe_status.go
// Observability pipeline health check. Connects to NATS, reads
// TELEMETRY stream stats, checks OTLP endpoint configuration,
// and probes for a local sidecar collector.
package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	observeMaxArgs        = 100
	otlpProbeTimeout      = 2 * time.Second
	sidecarProbeTimeout   = 1 * time.Second
	sidecarDefaultAddress = "localhost:4318"
	streamInfoTimeout     = 5 * time.Second
)

// observeStatus holds the full status report for JSON output.
type observeStatus struct {
	Telemetry *telemetryStatus `json:"telemetry"`
	OTLP      *otlpStatus      `json:"otlp"`
	Sidecar   *sidecarStatus   `json:"sidecar"`
}

// telemetryStatus holds TELEMETRY stream health data.
type telemetryStatus struct {
	Status    string `json:"status"`
	Messages  uint64 `json:"messages,omitempty"`
	Bytes     uint64 `json:"bytes,omitempty"`
	Consumers int    `json:"consumers,omitempty"`
	FirstMsg  string `json:"first_msg,omitempty"`
	LastMsg   string `json:"last_msg,omitempty"`
}

// otlpStatus holds OTLP export configuration.
type otlpStatus struct {
	Endpoint string `json:"endpoint"`
	Status   string `json:"status"`
}

// sidecarStatus holds sidecar detection results.
type sidecarStatus struct {
	Status  string `json:"status"`
	Address string `json:"address"`
}

// runObserveCmd dispatches observe subcommands.
func runObserveCmd(args []string) {
	if args == nil {
		panic("runObserveCmd: args must not be nil")
	}
	if len(args) > observeMaxArgs {
		panic("runObserveCmd: args exceeds max bound")
	}
	if HasHelpFlag(args) || len(args) == 0 {
		printObserveUsage()
		return
	}
	switch args[0] {
	case "status":
		runObserveStatusCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown observe command: %s\n",
			args[0])
		printObserveUsage()
		exitFunc(1)
	}
}

// printObserveUsage prints help for observe subcommands.
func printObserveUsage() {
	fmt.Println("Usage: dagnats observe <command>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  status  show observability pipeline health")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --json       output in JSON format")
	fmt.Println("  --nats-url   NATS server URL override")
}

// runObserveStatusCmd checks observability health and prints.
func runObserveStatusCmd(args []string) {
	if args == nil {
		panic("runObserveStatusCmd: args must not be nil")
	}
	if len(args) > observeMaxArgs {
		panic("runObserveStatusCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	natsURL := extractNatsURL(args)
	args = stripNatsURLFlag(args)

	if HasHelpFlag(args) {
		fmt.Println(
			"Usage: dagnats observe status [--json] [--nats-url=URL]")
		return
	}

	status := collectObserveStatus(natsURL)

	if jsonOutput {
		err := FormatJSON(os.Stdout, status)
		if err != nil {
			fmt.Fprintf(os.Stderr, "json error: %v\n", err)
			exitFunc(1)
		}
		return
	}

	printObserveStatus(status)
}

// extractNatsURL pulls --nats-url=X from args or falls back
// to the environment variable / default.
func extractNatsURL(args []string) string {
	if args == nil {
		panic("extractNatsURL: args must not be nil")
	}
	if len(args) > observeMaxArgs {
		panic("extractNatsURL: args exceeds max bound")
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "--nats-url=") {
			return strings.TrimPrefix(arg, "--nats-url=")
		}
	}
	return GetEnvWithFallback(
		"DAGNATS_NATS_URL", "NATS_URL", nats.DefaultURL,
	)
}

// stripNatsURLFlag removes --nats-url=X from args.
func stripNatsURLFlag(args []string) []string {
	if args == nil {
		panic("stripNatsURLFlag: args must not be nil")
	}
	result := make([]string, 0, len(args))
	for _, arg := range args {
		if !strings.HasPrefix(arg, "--nats-url=") {
			result = append(result, arg)
		}
	}
	return result
}

// collectObserveStatus gathers all observability health data.
func collectObserveStatus(natsURL string) observeStatus {
	if natsURL == "" {
		panic("collectObserveStatus: natsURL must not be empty")
	}
	if len(natsURL) > 1024 {
		panic("collectObserveStatus: natsURL exceeds max length")
	}

	status := observeStatus{
		Telemetry: collectTelemetryStatus(natsURL),
		OTLP:      collectOTLPStatus(),
		Sidecar:   collectSidecarStatus(),
	}
	return status
}

// collectTelemetryStatus connects to NATS and reads stream info.
func collectTelemetryStatus(
	natsURL string,
) *telemetryStatus {
	if natsURL == "" {
		panic(
			"collectTelemetryStatus: natsURL must not be empty",
		)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return &telemetryStatus{
			Status: "nats disconnected",
		}
	}
	defer nc.Close()

	return readTelemetryStreamInfo(nc)
}

// readTelemetryStreamInfo reads the TELEMETRY stream info.
func readTelemetryStreamInfo(
	nc *nats.Conn,
) *telemetryStatus {
	if nc == nil {
		panic("readTelemetryStreamInfo: nc must not be nil")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return &telemetryStatus{Status: "jetstream unavailable"}
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), streamInfoTimeout,
	)
	defer cancel()

	stream, err := js.Stream(ctx, "TELEMETRY")
	if err != nil {
		return &telemetryStatus{Status: "stream not found"}
	}

	info := stream.CachedInfo()
	return buildTelemetryStatus(*info)
}

// buildTelemetryStatus converts StreamInfo into our status type.
func buildTelemetryStatus(
	info jetstream.StreamInfo,
) *telemetryStatus {
	ts := &telemetryStatus{
		Status:    "ok",
		Messages:  info.State.Msgs,
		Bytes:     info.State.Bytes,
		Consumers: info.State.Consumers,
	}
	if !info.State.FirstTime.IsZero() {
		ts.FirstMsg = info.State.FirstTime.UTC().
			Format(time.RFC3339)
	}
	if !info.State.LastTime.IsZero() {
		ts.LastMsg = info.State.LastTime.UTC().
			Format(time.RFC3339)
	}
	return ts
}

// collectOTLPStatus checks for OTLP endpoint configuration.
func collectOTLPStatus() *otlpStatus {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return &otlpStatus{
			Endpoint: "",
			Status:   "not configured",
		}
	}
	return probeOTLPEndpoint(endpoint)
}

// probeOTLPEndpoint checks if the OTLP endpoint is reachable.
func probeOTLPEndpoint(endpoint string) *otlpStatus {
	if endpoint == "" {
		panic("probeOTLPEndpoint: endpoint must not be empty")
	}
	if len(endpoint) > 1024 {
		panic("probeOTLPEndpoint: endpoint exceeds max length")
	}

	address := extractHostPort(endpoint)
	conn, err := net.DialTimeout(
		"tcp", address, otlpProbeTimeout,
	)
	if err != nil {
		return &otlpStatus{
			Endpoint: endpoint,
			Status:   "configured (unreachable)",
		}
	}
	conn.Close()
	return &otlpStatus{
		Endpoint: endpoint,
		Status:   "configured (reachable)",
	}
}

// extractHostPort strips the scheme from a URL to get host:port.
func extractHostPort(endpoint string) string {
	if endpoint == "" {
		panic("extractHostPort: endpoint must not be empty")
	}
	if len(endpoint) > 1024 {
		panic("extractHostPort: endpoint exceeds max length")
	}
	addr := endpoint
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	addr = strings.TrimPrefix(addr, "grpc://")
	// If no port specified, assume 4318 (OTLP HTTP default).
	if !strings.Contains(addr, ":") {
		addr = addr + ":4318"
	}
	// Strip trailing path.
	if idx := strings.Index(addr, "/"); idx != -1 {
		addr = addr[:idx]
	}
	return addr
}

// collectSidecarStatus probes the local sidecar collector.
func collectSidecarStatus() *sidecarStatus {
	conn, err := net.DialTimeout(
		"tcp", sidecarDefaultAddress, sidecarProbeTimeout,
	)
	if err != nil {
		return &sidecarStatus{
			Address: sidecarDefaultAddress,
			Status:  "not detected",
		}
	}
	conn.Close()
	return &sidecarStatus{
		Address: sidecarDefaultAddress,
		Status:  "detected",
	}
}

// printObserveStatus renders human-readable output.
func printObserveStatus(status observeStatus) {
	if status.Telemetry == nil {
		panic("printObserveStatus: telemetry must not be nil")
	}
	if status.OTLP == nil {
		panic("printObserveStatus: otlp must not be nil")
	}
	if status.Sidecar == nil {
		panic("printObserveStatus: sidecar must not be nil")
	}

	fmt.Println("Observability Status")
	printTelemetrySection(status.Telemetry)
	printOTLPSection(status.OTLP)
	printSidecarSection(status.Sidecar)
}

// printTelemetrySection renders the TELEMETRY stream section.
func printTelemetrySection(ts *telemetryStatus) {
	if ts == nil {
		panic("printTelemetrySection: ts must not be nil")
	}
	fmt.Println("  TELEMETRY Stream:")
	if ts.Status != "ok" {
		fmt.Printf("    Status:     %s\n", ts.Status)
		return
	}
	fmt.Printf("    Messages:   %s\n", formatCount(ts.Messages))
	fmt.Printf("    Bytes:      %s\n", formatBytes(ts.Bytes))
	fmt.Printf("    Consumers:  %d\n", ts.Consumers)
	if ts.FirstMsg != "" {
		fmt.Printf("    First Msg:  %s\n", ts.FirstMsg)
	}
	if ts.LastMsg != "" {
		fmt.Printf("    Last Msg:   %s\n", ts.LastMsg)
	}
}

// printOTLPSection renders the OTLP export section.
func printOTLPSection(os *otlpStatus) {
	if os == nil {
		panic("printOTLPSection: os must not be nil")
	}
	fmt.Println()
	fmt.Println("  OTLP Export:")
	if os.Endpoint == "" {
		fmt.Printf("    Endpoint:   %s\n", "not configured")
		fmt.Printf("    Status:     %s\n", os.Status)
		return
	}
	fmt.Printf("    Endpoint:   %s\n", os.Endpoint)
	fmt.Printf("    Status:     %s\n", os.Status)
}

// printSidecarSection renders the sidecar detection section.
func printSidecarSection(sc *sidecarStatus) {
	if sc == nil {
		panic("printSidecarSection: sc must not be nil")
	}
	fmt.Println()
	fmt.Println("  Sidecar:")
	fmt.Printf("    Status:     %s\n", sc.Status)
}

// cli/metrics.go
// Metric snapshot viewing from the NATS TELEMETRY JetStream stream.
// Subscribes to telemetry.metrics subjects, collects data points
// within a time window, and displays summary statistics.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	metricsScanTimeout = 10 * time.Second
	metricsPointMax    = 10000
)

// metricShowFlags holds parsed flags for the metrics show command.
type metricShowFlags struct {
	name       string
	since      time.Duration
	service    string
	jsonOutput bool
}

// metricPoint is a single data point extracted from a metric record.
type metricPoint struct {
	Value       float64 `json:"value"`
	ServiceName string  `json:"service_name"`
	Timestamp   string  `json:"timestamp"`
}

// metricSummary holds computed statistics for display or JSON output.
type metricSummary struct {
	Name            string  `json:"name"`
	ServiceName     string  `json:"service_name"`
	Since           string  `json:"since"`
	Points          int     `json:"points"`
	Min             float64 `json:"min"`
	Max             float64 `json:"max"`
	Avg             float64 `json:"avg"`
	LatestValue     float64 `json:"latest_value"`
	LatestTimestamp string  `json:"latest_timestamp"`
}

// runMetricsCmd dispatches metrics subcommands.
func runMetricsCmd(args []string) {
	if args == nil {
		panic("runMetricsCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runMetricsCmd: args exceeds max bound")
	}
	if HasHelpFlag(args) {
		printMetricsUsage()
		return
	}
	if len(args) == 0 {
		printMetricsUsage()
		return
	}
	switch args[0] {
	case "show":
		runMetricsShowCmd(args[1:])
	default:
		printMetricsUsage()
	}
}

// printMetricsUsage prints metrics subcommand help text.
func printMetricsUsage() {
	fmt.Println("Usage: dagnats metrics show [flags]")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --name=X      metric name (required)")
	fmt.Println("  --since=5m    lookback duration (default 5m)")
	fmt.Println("  --service=X   filter by service name")
	fmt.Println("  --json        machine-readable output")
}

// runMetricsShowCmd collects metric points and displays a summary.
func runMetricsShowCmd(args []string) {
	if args == nil {
		panic("runMetricsShowCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runMetricsShowCmd: args exceeds max bound")
	}

	if HasHelpFlag(args) {
		printMetricsUsage()
		return
	}

	flags := parseMetricShowFlags(args)
	if flags.name == "" {
		fmt.Fprintln(os.Stderr,
			"Error: --name is required")
		printMetricsUsage()
		exitFunc(1)
		return
	}

	_, nc := connectService()
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		exitFunc(1)
		return
	}

	points := collectMetricPoints(js, flags)
	if len(points) == 0 {
		fmt.Fprintf(os.Stderr,
			"No metrics found for %s in the last %s\n",
			flags.name, flags.since)
		exitFunc(1)
		return
	}

	summary := computeMetricSummary(flags, points)
	if flags.jsonOutput {
		if fmtErr := FormatJSON(os.Stdout, summary); fmtErr != nil {
			fmt.Fprintf(os.Stderr,
				"format json: %v\n", fmtErr)
			exitFunc(1)
		}
		return
	}
	printMetricSummary(summary)
}

// parseMetricShowFlags extracts flags from args.
func parseMetricShowFlags(args []string) metricShowFlags {
	if args == nil {
		panic("parseMetricShowFlags: args must not be nil")
	}
	if len(args) > 100 {
		panic("parseMetricShowFlags: args exceeds max bound")
	}

	flags := metricShowFlags{since: 5 * time.Minute}
	flags.jsonOutput = HasJSONFlag(args)

	for _, arg := range args {
		if strings.HasPrefix(arg, "--name=") {
			flags.name = strings.TrimPrefix(arg, "--name=")
		}
		if strings.HasPrefix(arg, "--since=") {
			val := strings.TrimPrefix(arg, "--since=")
			dur, err := time.ParseDuration(val)
			if err != nil {
				fmt.Fprintf(os.Stderr,
					"invalid --since: %v\n", err)
				exitFunc(1)
				return flags
			}
			flags.since = dur
		}
		if strings.HasPrefix(arg, "--service=") {
			flags.service = strings.TrimPrefix(
				arg, "--service=",
			)
		}
	}
	return flags
}

// buildMetricSubject constructs the NATS subject filter for
// metric subscriptions based on optional service and name filters.
func buildMetricSubject(
	service, name string,
) string {
	if len(service) > 200 {
		panic(
			"buildMetricSubject: service name unreasonably long",
		)
	}
	if len(name) > 200 {
		panic(
			"buildMetricSubject: metric name unreasonably long",
		)
	}
	svc := "*"
	if service != "" {
		svc = service
	}
	return "telemetry.metrics." + svc + "." + name
}

// collectMetricPoints reads metric records from the TELEMETRY
// stream and extracts numeric values. Bounded by scan timeout
// and metricsPointMax.
func collectMetricPoints(
	js jetstream.JetStream, flags metricShowFlags,
) []metricPoint {
	if js == nil {
		panic("collectMetricPoints: js must not be nil")
	}
	if flags.name == "" {
		panic("collectMetricPoints: name must not be empty")
	}

	startTime := time.Now().Add(-flags.since)
	subject := buildMetricSubject(flags.service, flags.name)

	ctx, cancel := context.WithTimeout(
		context.Background(), metricsScanTimeout,
	)
	defer cancel()

	cons, err := js.OrderedConsumer(
		ctx, "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
			DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
			OptStartTime:   &startTime,
		},
	)
	if err != nil {
		handleStreamError(err, flags.name)
		return nil
	}

	return fetchMetricPoints(cons)
}

// handleStreamError prints a helpful message when the TELEMETRY
// stream or consumer cannot be created.
func handleStreamError(err error, name string) {
	if err == nil {
		panic("handleStreamError: err must not be nil")
	}
	if name == "" {
		panic("handleStreamError: name must not be empty")
	}
	errStr := err.Error()
	if strings.Contains(errStr, "stream not found") {
		fmt.Fprintln(os.Stderr,
			"Error: TELEMETRY stream not found.\n"+
				"Hint: run 'dagnats serve' to start "+
				"the server with telemetry enabled.")
		return
	}
	fmt.Fprintf(os.Stderr,
		"consumer for %s: %v\n", name, err)
}

// fetchMetricPoints reads messages from an ordered consumer and
// extracts numeric values. Bounded by metricsPointMax.
func fetchMetricPoints(
	cons jetstream.Consumer,
) []metricPoint {
	if cons == nil {
		panic("fetchMetricPoints: cons must not be nil")
	}

	points := make([]metricPoint, 0, 256)
	for i := 0; i < metricsPointMax; i++ {
		msg, fetchErr := cons.Next(
			jetstream.FetchMaxWait(time.Second),
		)
		if fetchErr != nil {
			break
		}
		pt, ok := parseMetricPoint(msg.Data())
		if !ok {
			continue
		}
		points = append(points, pt)
	}
	return points
}

// natsMetricRecord mirrors the JSON shape published by the
// MetricExporter. The Data field is a raw JSON value that
// varies by metric type (gauge, sum, histogram).
type natsMetricRecord struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Unit        string          `json:"unit,omitempty"`
	ServiceName string          `json:"serviceName"`
	Data        json.RawMessage `json:"data"`
	Timestamp   string          `json:"timestamp"`
}

// parseMetricPoint extracts a numeric value from a metric record.
// Returns false when the record cannot be parsed or has no numeric
// value. Supports gauge and sum data types from the OTel SDK.
func parseMetricPoint(
	data []byte,
) (metricPoint, bool) {
	if data == nil {
		panic("parseMetricPoint: data must not be nil")
	}
	if len(data) > 1<<20 {
		panic("parseMetricPoint: data exceeds 1MB bound")
	}

	var rec natsMetricRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return metricPoint{}, false
	}

	val, ok := extractNumericValue(rec.Data)
	if !ok {
		return metricPoint{}, false
	}

	return metricPoint{
		Value:       val,
		ServiceName: rec.ServiceName,
		Timestamp:   rec.Timestamp,
	}, true
}

// extractNumericValue attempts to pull a float64 from OTel metric
// data JSON. Handles gauge and sum DataPoints arrays, falling back
// to a top-level "value" field.
func extractNumericValue(
	raw json.RawMessage,
) (float64, bool) {
	if raw == nil {
		return 0, false
	}
	if len(raw) > 1<<20 {
		panic("extractNumericValue: raw exceeds 1MB bound")
	}

	// Try OTel SDK gauge/sum with DataPoints array.
	var dpContainer struct {
		DataPoints []struct {
			Value   *float64    `json:"Value"`
			Int     *int64      `json:"Int"`
			AsFloat json.Number `json:"asFloat,omitempty"`
			AsInt   json.Number `json:"asInt,omitempty"`
		} `json:"DataPoints"`
	}
	if err := json.Unmarshal(raw, &dpContainer); err == nil {
		if len(dpContainer.DataPoints) > 0 {
			dp := dpContainer.DataPoints[0]
			return extractFromDataPoint(dp.Value, dp.Int), true
		}
	}

	// Fallback: simple {"value": N} shape.
	var simple struct {
		Value float64 `json:"value"`
	}
	if err := json.Unmarshal(raw, &simple); err == nil {
		if simple.Value != 0 {
			return simple.Value, true
		}
	}

	return 0, false
}

// extractFromDataPoint returns the numeric value from a data point,
// preferring float over int.
func extractFromDataPoint(
	floatVal *float64, intVal *int64,
) float64 {
	if floatVal != nil {
		return *floatVal
	}
	if intVal != nil {
		return float64(*intVal)
	}
	return 0
}

// computeMetricSummary calculates min, max, avg over collected
// points.
func computeMetricSummary(
	flags metricShowFlags, points []metricPoint,
) metricSummary {
	if len(points) == 0 {
		panic("computeMetricSummary: points must not be empty")
	}
	if len(points) > metricsPointMax {
		panic("computeMetricSummary: points exceeds max bound")
	}

	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64
	sum := 0.0
	latest := points[0]

	for _, pt := range points {
		if pt.Value < minVal {
			minVal = pt.Value
		}
		if pt.Value > maxVal {
			maxVal = pt.Value
		}
		sum += pt.Value
		if pt.Timestamp >= latest.Timestamp {
			latest = pt
		}
	}

	svc := latest.ServiceName
	if flags.service != "" {
		svc = flags.service
	}

	return metricSummary{
		Name:            flags.name,
		ServiceName:     svc,
		Since:           flags.since.String(),
		Points:          len(points),
		Min:             minVal,
		Max:             maxVal,
		Avg:             sum / float64(len(points)),
		LatestValue:     latest.Value,
		LatestTimestamp: latest.Timestamp,
	}
}

// printMetricSummary displays a human-readable metric summary.
func printMetricSummary(s metricSummary) {
	if s.Name == "" {
		panic("printMetricSummary: Name must not be empty")
	}
	if s.Points <= 0 {
		panic("printMetricSummary: Points must be positive")
	}

	fmt.Printf("Metric: %s\n", s.Name)
	fmt.Printf("  Service:  %s\n", s.ServiceName)
	fmt.Printf("  Since:    %s ago\n", s.Since)
	fmt.Printf("  Points:   %d\n", s.Points)
	fmt.Printf("  Min:      %.2f\n", s.Min)
	fmt.Printf("  Max:      %.2f\n", s.Max)
	fmt.Printf("  Avg:      %.2f\n", s.Avg)
	fmt.Printf("  Latest:   %.2f (%s)\n",
		s.LatestValue, s.LatestTimestamp)
}

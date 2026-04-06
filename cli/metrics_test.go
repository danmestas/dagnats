// cli/metrics_test.go
// Tests for metrics show command. Unit tests cover flag parsing, subject
// building, point parsing, and summary computation. Integration tests
// use embedded NATS to verify end-to-end metric collection.
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// --- Flag parsing tests ---

func TestParseMetricShowFlags(t *testing.T) {
	args := []string{
		"--name=api.request.duration",
		"--since=10m",
		"--service=dagnats-api",
		"--json",
	}

	flags := parseMetricShowFlags(args)

	// Positive: all flags parsed correctly
	if flags.name != "api.request.duration" {
		t.Fatalf("name: got %q", flags.name)
	}
	if flags.since != 10*time.Minute {
		t.Fatalf("since: got %v", flags.since)
	}
	if flags.service != "dagnats-api" {
		t.Fatalf("service: got %q", flags.service)
	}
	if !flags.jsonOutput {
		t.Fatal("jsonOutput should be true")
	}

	// Negative: since should not exceed a day
	if flags.since > 24*time.Hour {
		t.Fatal("since should not exceed 24h for this test")
	}
}

func TestParseMetricShowFlagsDefaults(t *testing.T) {
	flags := parseMetricShowFlags([]string{})

	// Positive: default since is 5m
	if flags.since != 5*time.Minute {
		t.Fatalf("default since should be 5m, got %v",
			flags.since)
	}

	// Negative: no filters should be set
	if flags.name != "" {
		t.Fatal("default name should be empty")
	}
	if flags.service != "" {
		t.Fatal("default service should be empty")
	}
	if flags.jsonOutput {
		t.Fatal("default jsonOutput should be false")
	}
}

// --- Subject building tests ---

func TestBuildMetricSubject(t *testing.T) {
	// With name only
	got := buildMetricSubject("", "api.request.duration")
	if got != "telemetry.metrics.*.api.request.duration" {
		t.Fatalf("expected wildcard service, got %q", got)
	}

	// With service and name
	got = buildMetricSubject("engine", "task.count")
	if got != "telemetry.metrics.engine.task.count" {
		t.Fatalf("expected specific subject, got %q", got)
	}

	// Negative: result must not be empty
	if got == "" {
		t.Fatal("subject must not be empty")
	}
}

// --- Point parsing tests ---

func TestParseMetricPointGauge(t *testing.T) {
	rec := natsMetricRecord{
		Name:        "cpu.usage",
		ServiceName: "testsvc",
		Timestamp:   "2026-04-06T12:00:00Z",
		Data: json.RawMessage(`{
			"DataPoints": [{"Value": 42.5}]
		}`),
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	pt, ok := parseMetricPoint(data)

	// Positive: should parse successfully
	if !ok {
		t.Fatal("expected successful parse")
	}
	if pt.Value != 42.5 {
		t.Fatalf("expected 42.5, got %f", pt.Value)
	}

	// Positive: service and timestamp preserved
	if pt.ServiceName != "testsvc" {
		t.Fatalf("service: got %q", pt.ServiceName)
	}
	if pt.Timestamp != "2026-04-06T12:00:00Z" {
		t.Fatalf("timestamp: got %q", pt.Timestamp)
	}

	// Negative: value should not be zero
	if pt.Value == 0 {
		t.Fatal("value should not be zero for this input")
	}
}

func TestParseMetricPointIntValue(t *testing.T) {
	rec := natsMetricRecord{
		Name:        "request.count",
		ServiceName: "api",
		Timestamp:   "2026-04-06T12:00:00Z",
		Data: json.RawMessage(`{
			"DataPoints": [{"Int": 100}]
		}`),
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	pt, ok := parseMetricPoint(data)

	// Positive: should parse int as float
	if !ok {
		t.Fatal("expected successful parse")
	}
	if pt.Value != 100.0 {
		t.Fatalf("expected 100, got %f", pt.Value)
	}

	// Negative: should not be negative
	if pt.Value < 0 {
		t.Fatal("count should not be negative")
	}
}

func TestParseMetricPointInvalid(t *testing.T) {
	_, ok := parseMetricPoint([]byte("not json"))

	// Positive: should return false for invalid data
	if ok {
		t.Fatal("expected false for invalid JSON")
	}

	// Negative: should not panic on bad input
	_, ok2 := parseMetricPoint([]byte(`{"data": null}`))
	if ok2 {
		t.Fatal("null data should return false")
	}
}

// --- extractNumericValue tests ---

func TestExtractNumericValueGauge(t *testing.T) {
	raw := json.RawMessage(
		`{"DataPoints":[{"Value":3.14}]}`,
	)
	val, ok := extractNumericValue(raw)

	// Positive: should extract float value
	if !ok {
		t.Fatal("expected successful extraction")
	}
	if val != 3.14 {
		t.Fatalf("expected 3.14, got %f", val)
	}

	// Negative: should not be zero
	if val == 0 {
		t.Fatal("value should not be zero")
	}
}

func TestExtractNumericValueNil(t *testing.T) {
	_, ok := extractNumericValue(nil)

	// Positive: nil returns false
	if ok {
		t.Fatal("nil should return false")
	}

	// Negative: should not panic
	_, ok2 := extractNumericValue(json.RawMessage(`{}`))
	if ok2 {
		t.Fatal("empty object should return false")
	}
}

// --- Summary computation tests ---

func TestComputeMetricSummary(t *testing.T) {
	flags := metricShowFlags{
		name:  "api.latency",
		since: 5 * time.Minute,
	}
	points := []metricPoint{
		{Value: 10.0, ServiceName: "api",
			Timestamp: "2026-04-06T12:00:00Z"},
		{Value: 20.0, ServiceName: "api",
			Timestamp: "2026-04-06T12:00:01Z"},
		{Value: 30.0, ServiceName: "api",
			Timestamp: "2026-04-06T12:00:02Z"},
	}

	s := computeMetricSummary(flags, points)

	// Positive: min/max/avg computed correctly
	if s.Min != 10.0 {
		t.Fatalf("min: expected 10, got %f", s.Min)
	}
	if s.Max != 30.0 {
		t.Fatalf("max: expected 30, got %f", s.Max)
	}
	if s.Avg != 20.0 {
		t.Fatalf("avg: expected 20, got %f", s.Avg)
	}

	// Positive: latest is the most recent timestamp
	if s.LatestValue != 30.0 {
		t.Fatalf("latest: expected 30, got %f", s.LatestValue)
	}
	if s.LatestTimestamp != "2026-04-06T12:00:02Z" {
		t.Fatalf("latest ts: got %q", s.LatestTimestamp)
	}

	// Positive: metadata preserved
	if s.Name != "api.latency" {
		t.Fatalf("name: got %q", s.Name)
	}
	if s.Points != 3 {
		t.Fatalf("points: got %d", s.Points)
	}

	// Negative: min should not exceed max
	if s.Min > s.Max {
		t.Fatal("min must not exceed max")
	}
}

func TestComputeMetricSummarySinglePoint(t *testing.T) {
	flags := metricShowFlags{
		name:    "single",
		since:   time.Minute,
		service: "svc",
	}
	points := []metricPoint{
		{Value: 7.77, ServiceName: "svc",
			Timestamp: "2026-04-06T12:00:00Z"},
	}

	s := computeMetricSummary(flags, points)

	// Positive: single point means min=max=avg=latest
	if s.Min != 7.77 || s.Max != 7.77 || s.Avg != 7.77 {
		t.Fatalf("single point: min=%f max=%f avg=%f",
			s.Min, s.Max, s.Avg)
	}

	// Negative: points must be exactly 1
	if s.Points != 1 {
		t.Fatalf("expected 1 point, got %d", s.Points)
	}
}

// --- Print formatting tests ---

func TestPrintMetricSummaryOutput(t *testing.T) {
	s := metricSummary{
		Name:            "api.request.duration",
		ServiceName:     "dagnats-api",
		Since:           "5m0s",
		Points:          142,
		Min:             1.20,
		Max:             856.30,
		Avg:             45.67,
		LatestValue:     23.40,
		LatestTimestamp: "2026-04-06T12:34:56Z",
	}

	output := captureMetricsOutput(func() {
		printMetricSummary(s)
	})

	// Positive: contains metric name
	if !strings.Contains(output, "api.request.duration") {
		t.Fatalf("missing metric name in output:\n%s", output)
	}

	// Positive: contains service name
	if !strings.Contains(output, "dagnats-api") {
		t.Fatalf("missing service name in output:\n%s", output)
	}

	// Positive: contains stats
	if !strings.Contains(output, "856.30") {
		t.Fatalf("missing max value in output:\n%s", output)
	}

	// Negative: should not contain JSON braces
	if strings.Contains(output, "{") {
		t.Fatal("human output should not contain JSON braces")
	}
}

// --- Integration test ---

func TestMetricsShowIntegration(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Unsetenv("NATS_URL")

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Publish 3 metric records as JSON.
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		val := float64((i + 1) * 10)
		ts := now.Add(
			time.Duration(i) * time.Second,
		).Format(time.RFC3339Nano)
		rec := natsMetricRecord{
			Name:        "test.latency",
			ServiceName: "testsvc",
			Timestamp:   ts,
			Data: json.RawMessage(
				`{"DataPoints":[{"Value":` +
					floatStr(val) + `}]}`,
			),
		}
		data, marshalErr := json.Marshal(rec)
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}
		subject := "telemetry.metrics.testsvc.test.latency"
		_, pubErr := jsLegacy.Publish(subject, data)
		if pubErr != nil {
			t.Fatalf("publish: %v", pubErr)
		}
	}

	js, jsErr := jetstream.New(nc)
	if jsErr != nil {
		t.Fatalf("jetstream.New: %v", jsErr)
	}

	flags := metricShowFlags{
		name:  "test.latency",
		since: time.Hour,
	}
	points := collectMetricPoints(js, flags)

	// Positive: should collect all 3 points
	if len(points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(points))
	}

	// Positive: values should match published data
	sum := 0.0
	for _, pt := range points {
		sum += pt.Value
	}
	if sum != 60.0 {
		t.Fatalf("expected sum 60, got %f", sum)
	}

	// Negative: no point should have empty service
	for _, pt := range points {
		if pt.ServiceName == "" {
			t.Fatal("service name must not be empty")
		}
	}

	// Verify summary computation with collected points
	summary := computeMetricSummary(flags, points)
	if summary.Min != 10.0 {
		t.Fatalf("min: expected 10, got %f", summary.Min)
	}
	if summary.Max != 30.0 {
		t.Fatalf("max: expected 30, got %f", summary.Max)
	}
}

func TestMetricsShowJSONOutput(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Unsetenv("NATS_URL")

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)
	rec := natsMetricRecord{
		Name:        "json.metric",
		ServiceName: "jsonsvc",
		Timestamp:   ts,
		Data: json.RawMessage(
			`{"DataPoints":[{"Value":99.9}]}`,
		),
	}
	data, marshalErr := json.Marshal(rec)
	if marshalErr != nil {
		t.Fatalf("marshal: %v", marshalErr)
	}
	_, pubErr := jsLegacy.Publish(
		"telemetry.metrics.jsonsvc.json.metric", data,
	)
	if pubErr != nil {
		t.Fatalf("publish: %v", pubErr)
	}

	output := captureMetricsOutput(func() {
		runMetricsShowCmd([]string{
			"--name=json.metric",
			"--since=1h",
			"--json",
		})
	})

	// Positive: output should be valid JSON
	var result metricSummary
	if err := json.Unmarshal(
		[]byte(output), &result,
	); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, output)
	}

	// Positive: name should match
	if result.Name != "json.metric" {
		t.Fatalf("name: got %q", result.Name)
	}

	// Negative: points must be positive
	if result.Points <= 0 {
		t.Fatal("points must be positive in JSON output")
	}
}

func TestMetricsShowNoResults(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Unsetenv("NATS_URL")

	exitCalled := false
	oldExit := exitFunc
	exitFunc = func(code int) {
		exitCalled = true
	}
	defer func() { exitFunc = oldExit }()

	output := captureMetricsStderr(func() {
		runMetricsShowCmd([]string{
			"--name=nonexistent.metric",
			"--since=1m",
		})
	})

	// Positive: should report no metrics found
	if !strings.Contains(output, "No metrics found") &&
		!strings.Contains(output, "nonexistent.metric") &&
		!exitCalled {
		t.Fatalf(
			"expected no-results message, got:\n%s", output)
	}

	// Negative: exit should have been called
	if !exitCalled {
		t.Fatal("expected exit to be called")
	}
}

func TestMetricsShowMissingName(t *testing.T) {
	exitCalled := false
	exitCode := 0
	oldExit := exitFunc
	exitFunc = func(code int) {
		exitCalled = true
		exitCode = code
	}
	defer func() { exitFunc = oldExit }()

	runMetricsShowCmd([]string{})

	// Positive: should exit with error
	if !exitCalled {
		t.Fatal("expected exit to be called")
	}
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
}

// --- Helpers ---

func floatStr(f float64) string {
	return strings.TrimRight(
		strings.TrimRight(
			fmt.Sprintf("%.6f", f), "0",
		), ".",
	)
}

// captureMetricsOutput captures stdout from a function.
func captureMetricsOutput(fn func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 16384)
	n, _ := r.Read(buf)
	return string(buf[:n])
}

// captureMetricsStderr captures stderr from a function.
func captureMetricsStderr(fn func()) string {
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	fn()

	w.Close()
	os.Stderr = oldStderr

	buf := make([]byte, 16384)
	n, _ := r.Read(buf)
	return string(buf[:n])
}

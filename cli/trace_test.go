// cli/trace_test.go
// Tests for trace view and search commands. Unit tests cover span
// parsing, tree building, status labels, and flag parsing. Integration
// tests use embedded NATS to verify end-to-end span collection.
package cli

import (
	"context"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// --- extractTraceID tests (preserved from original) ---

func TestExtractTraceID(t *testing.T) {
	tp := "00-abcdef1234567890abcdef1234567890-" +
		"1234567890abcdef-01"
	got := extractTraceID(tp)

	// Positive: should return the trace ID segment
	if got != "abcdef1234567890abcdef1234567890" {
		t.Fatalf("expected trace ID, got %q", got)
	}

	// Negative: should not return the full traceparent
	if got == tp {
		t.Fatal(
			"should not return the full traceparent string",
		)
	}
}

func TestExtractTraceIDEmpty(t *testing.T) {
	got := extractTraceID("")

	// Positive: empty input returns empty string
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}

	// Negative: should not panic on empty input
	if len(got) != 0 {
		t.Fatal("length should be zero")
	}
}

func TestExtractTraceIDInvalid(t *testing.T) {
	// Malformed: wrong version
	got := extractTraceID("01-abc-def-01")
	if got != "" {
		t.Fatalf(
			"wrong version should return empty, got %q", got,
		)
	}

	// Malformed: too few parts
	got = extractTraceID("00-abc-def")
	if got != "" {
		t.Fatalf(
			"too few parts should return empty, got %q", got,
		)
	}

	// Negative: none of these should return a trace ID
	got = extractTraceID("00-a-b-c-d")
	if got != "" {
		t.Fatalf(
			"too many parts should return empty, got %q", got,
		)
	}
}

// --- Span parsing tests ---

func TestParseSpan(t *testing.T) {
	traceID := make([]byte, 16)
	traceID[0] = 0xab
	spanID := make([]byte, 8)
	spanID[0] = 0xcd

	sp := &tracepb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		Name:              "test.operation",
		StartTimeUnixNano: 1000000000,
		EndTimeUnixNano:   1050000000,
		Status: &tracepb.Status{
			Code: tracepb.Status_STATUS_CODE_OK,
		},
	}
	data, err := protojson.Marshal(sp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed, err := parseSpan(data)

	// Positive: should parse without error
	if err != nil {
		t.Fatalf("parseSpan: %v", err)
	}

	// Positive: name should match
	if parsed.Name != "test.operation" {
		t.Fatalf("expected test.operation, got %q", parsed.Name)
	}

	// Negative: trace ID should not be empty
	if len(parsed.TraceId) == 0 {
		t.Fatal("trace ID should not be empty")
	}
}

func TestParseSpanInvalid(t *testing.T) {
	_, err := parseSpan([]byte("not json"))

	// Positive: should return an error
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}

	// Negative: error message should not be empty
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

// --- Span helper tests ---

func TestSpanHexIDs(t *testing.T) {
	traceID := []byte{
		0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
		0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
	}
	spanID := []byte{
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
	}
	parentID := []byte{
		0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
	}

	sp := &tracepb.Span{
		TraceId:      traceID,
		SpanId:       spanID,
		ParentSpanId: parentID,
	}

	tid := spanHexTraceID(sp)
	// Positive: should be hex-encoded
	if tid != hex.EncodeToString(traceID) {
		t.Fatalf("trace ID mismatch: %q", tid)
	}

	sid := spanHexSpanID(sp)
	// Positive: should be hex-encoded
	if sid != hex.EncodeToString(spanID) {
		t.Fatalf("span ID mismatch: %q", sid)
	}

	pid := spanHexParentID(sp)
	// Positive: should be hex-encoded
	if pid != hex.EncodeToString(parentID) {
		t.Fatalf("parent ID mismatch: %q", pid)
	}

	// Negative: no parent should return empty
	noParent := &tracepb.Span{
		TraceId: traceID,
		SpanId:  spanID,
	}
	if spanHexParentID(noParent) != "" {
		t.Fatal("no parent should return empty string")
	}
}

func TestSpanDurationMs(t *testing.T) {
	sp := &tracepb.Span{
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   1_050_000_000,
	}

	dur := spanDurationMs(sp)

	// Positive: 50ms difference
	if dur != 50 {
		t.Fatalf("expected 50ms, got %d", dur)
	}

	// Negative: should not be negative
	if dur < 0 {
		t.Fatal("duration must not be negative")
	}
}

func TestSpanDurationMsZero(t *testing.T) {
	sp := &tracepb.Span{
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   1_000_000_000,
	}

	dur := spanDurationMs(sp)

	// Positive: equal times should yield 0
	if dur != 0 {
		t.Fatalf("expected 0ms, got %d", dur)
	}

	// Negative: should not be negative
	if dur < 0 {
		t.Fatal("duration must not be negative")
	}
}

func TestSpanStatusLabel(t *testing.T) {
	tests := []struct {
		name   string
		status *tracepb.Status
		want   string
	}{
		{
			"ok",
			&tracepb.Status{
				Code: tracepb.Status_STATUS_CODE_OK,
			},
			"ok",
		},
		{
			"error",
			&tracepb.Status{
				Code: tracepb.Status_STATUS_CODE_ERROR,
			},
			"error",
		},
		{
			"unset",
			&tracepb.Status{
				Code: tracepb.Status_STATUS_CODE_UNSET,
			},
			"unset",
		},
		{"nil status", nil, "unset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &tracepb.Span{Status: tt.status}
			got := spanStatusLabel(sp)

			// Positive: should match expected
			if got != tt.want {
				t.Fatalf("expected %q, got %q",
					tt.want, got)
			}

			// Negative: should not be empty
			if got == "" {
				t.Fatal("status label must not be empty")
			}
		})
	}
}

func TestIsRootSpan(t *testing.T) {
	root := &tracepb.Span{
		SpanId: []byte{0x01},
	}
	child := &tracepb.Span{
		SpanId:       []byte{0x02},
		ParentSpanId: []byte{0x01},
	}

	// Positive: no parent = root
	if !isRootSpan(root) {
		t.Fatal("span without parent should be root")
	}

	// Negative: span with parent is not root
	if isRootSpan(child) {
		t.Fatal("span with parent should not be root")
	}
}

// --- Tree building tests ---

func TestBuildSpanTrees(t *testing.T) {
	traceID := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	}
	rootSID := []byte{0x01, 0, 0, 0, 0, 0, 0, 0}
	childSID1 := []byte{0x02, 0, 0, 0, 0, 0, 0, 0}
	childSID2 := []byte{0x03, 0, 0, 0, 0, 0, 0, 0}

	spans := []*tracepb.Span{
		{
			TraceId:           traceID,
			SpanId:            rootSID,
			Name:              "root",
			StartTimeUnixNano: 1000,
			EndTimeUnixNano:   5000,
		},
		{
			TraceId:           traceID,
			SpanId:            childSID1,
			ParentSpanId:      rootSID,
			Name:              "child-a",
			StartTimeUnixNano: 2000,
			EndTimeUnixNano:   3000,
		},
		{
			TraceId:           traceID,
			SpanId:            childSID2,
			ParentSpanId:      rootSID,
			Name:              "child-b",
			StartTimeUnixNano: 3000,
			EndTimeUnixNano:   4000,
		},
	}

	trees := buildSpanTrees(spans)
	tid := hex.EncodeToString(traceID)

	// Positive: should have one trace
	if len(trees) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(trees))
	}

	roots := trees[tid]
	// Positive: should have one root
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}

	// Positive: root should have 2 children
	if len(roots[0].Children) != 2 {
		t.Fatalf("expected 2 children, got %d",
			len(roots[0].Children))
	}

	// Positive: children sorted by start time
	if roots[0].Children[0].Span.Name != "child-a" {
		t.Fatalf("first child should be child-a, got %q",
			roots[0].Children[0].Span.Name)
	}

	// Negative: root should not have a parent
	if spanHexParentID(roots[0].Span) != "" {
		t.Fatal("root should not have a parent span ID")
	}
}

func TestBuildSpanTreesMultipleTraces(t *testing.T) {
	tid1 := []byte{
		0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
	}
	tid2 := []byte{
		0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
	}

	spans := []*tracepb.Span{
		{
			TraceId: tid1,
			SpanId:  []byte{0x0a, 0, 0, 0, 0, 0, 0, 0},
			Name:    "trace1-root",
		},
		{
			TraceId: tid2,
			SpanId:  []byte{0x0b, 0, 0, 0, 0, 0, 0, 0},
			Name:    "trace2-root",
		},
	}

	trees := buildSpanTrees(spans)

	// Positive: should have two distinct traces
	if len(trees) != 2 {
		t.Fatalf("expected 2 traces, got %d", len(trees))
	}

	// Negative: neither trace should be empty
	for tid, roots := range trees {
		if len(roots) == 0 {
			t.Fatalf("trace %s has no roots", tid)
		}
	}
}

// --- Flag parsing tests ---

func TestParseTraceSearchFlags(t *testing.T) {
	args := []string{
		"--service=engine",
		"--status=error",
		"--since=30m",
		"--limit=50",
		"--json",
	}

	flags := parseTraceSearchFlags(args)

	// Positive: all flags parsed
	if flags.service != "engine" {
		t.Fatalf("service: got %q", flags.service)
	}
	if flags.status != "error" {
		t.Fatalf("status: got %q", flags.status)
	}
	if flags.since != 30*time.Minute {
		t.Fatalf("since: got %v", flags.since)
	}
	if flags.limit != 50 {
		t.Fatalf("limit: got %d", flags.limit)
	}
	if !flags.jsonOutput {
		t.Fatal("jsonOutput should be true")
	}

	// Negative: limit should not exceed max
	if flags.limit > traceSearchMax {
		t.Fatal("limit should not exceed traceSearchMax")
	}
}

func TestParseTraceSearchFlagsDefaults(t *testing.T) {
	flags := parseTraceSearchFlags([]string{})

	// Positive: defaults applied
	if flags.since != time.Hour {
		t.Fatalf("default since should be 1h, got %v",
			flags.since)
	}
	if flags.limit != 100 {
		t.Fatalf("default limit should be 100, got %d",
			flags.limit)
	}

	// Negative: should not have filters set
	if flags.service != "" {
		t.Fatal("default service should be empty")
	}
	if flags.status != "" {
		t.Fatal("default status should be empty")
	}
}

func TestBuildSearchSubject(t *testing.T) {
	// Positive: no service = wildcard
	got := buildSearchSubject("")
	if got != "telemetry.spans.>" {
		t.Fatalf("expected wildcard subject, got %q", got)
	}

	// Positive: with service
	got = buildSearchSubject("engine")
	if got != "telemetry.spans.engine.>" {
		t.Fatalf("expected engine subject, got %q", got)
	}

	// Negative: subject must not be empty
	if got == "" {
		t.Fatal("subject must not be empty")
	}
}

// --- Integration test ---

func TestCollectRunSpansIntegration(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	runID := "test-run-001"
	traceID := []byte{
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
	}

	// Publish 3 spans as OTLP proto JSON.
	rootSID := []byte{0x01, 0, 0, 0, 0, 0, 0, 0}
	child1SID := []byte{0x02, 0, 0, 0, 0, 0, 0, 0}
	child2SID := []byte{0x03, 0, 0, 0, 0, 0, 0, 0}

	spans := []*tracepb.Span{
		{
			TraceId:           traceID,
			SpanId:            rootSID,
			Name:              "workflow.run",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   1_100_000_000,
			Status: &tracepb.Status{
				Code: tracepb.Status_STATUS_CODE_OK,
			},
		},
		{
			TraceId:           traceID,
			SpanId:            child1SID,
			ParentSpanId:      rootSID,
			Name:              "engine.dispatch",
			StartTimeUnixNano: 1_010_000_000,
			EndTimeUnixNano:   1_060_000_000,
			Status: &tracepb.Status{
				Code: tracepb.Status_STATUS_CODE_OK,
			},
		},
		{
			TraceId:           traceID,
			SpanId:            child2SID,
			ParentSpanId:      rootSID,
			Name:              "worker.execute",
			StartTimeUnixNano: 1_060_000_000,
			EndTimeUnixNano:   1_090_000_000,
			Status: &tracepb.Status{
				Code:    tracepb.Status_STATUS_CODE_ERROR,
				Message: "task failed",
			},
		},
	}

	for _, sp := range spans {
		data, marshalErr := protojson.Marshal(sp)
		if marshalErr != nil {
			t.Fatalf("marshal span: %v", marshalErr)
		}
		subject := "telemetry.spans.testsvc." + runID
		_, pubErr := jsLegacy.Publish(subject, data)
		if pubErr != nil {
			t.Fatalf("publish span: %v", pubErr)
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	collected := collectRunSpans(js, runID)

	// Positive: should collect all 3 spans
	if len(collected) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(collected))
	}

	// Positive: first span name should be recognizable
	names := make(map[string]bool)
	for _, sp := range collected {
		names[sp.Name] = true
	}
	if !names["workflow.run"] {
		t.Fatal("should contain workflow.run span")
	}

	// Negative: should not contain unknown spans
	if names["unknown"] {
		t.Fatal("should not contain unknown span")
	}
}

func TestCollectRunSpansBuildTree(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	runID := "tree-test-001"
	traceID := []byte{
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
	}
	rootSID := []byte{0x10, 0, 0, 0, 0, 0, 0, 0}
	childSID := []byte{0x20, 0, 0, 0, 0, 0, 0, 0}

	spans := []*tracepb.Span{
		{
			TraceId:           traceID,
			SpanId:            rootSID,
			Name:              "root-op",
			StartTimeUnixNano: 5000,
			EndTimeUnixNano:   9000,
			Status: &tracepb.Status{
				Code: tracepb.Status_STATUS_CODE_OK,
			},
		},
		{
			TraceId:           traceID,
			SpanId:            childSID,
			ParentSpanId:      rootSID,
			Name:              "child-op",
			StartTimeUnixNano: 6000,
			EndTimeUnixNano:   8000,
			Status: &tracepb.Status{
				Code: tracepb.Status_STATUS_CODE_OK,
			},
		},
	}

	for _, sp := range spans {
		data, marshalErr := protojson.Marshal(sp)
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}
		subject := "telemetry.spans.svc." + runID
		_, pubErr := jsLegacy.Publish(subject, data)
		if pubErr != nil {
			t.Fatalf("publish: %v", pubErr)
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	collected := collectRunSpans(js, runID)
	trees := buildSpanTrees(collected)

	tid := hex.EncodeToString(traceID)

	// Positive: one trace with one root
	roots := trees[tid]
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}

	// Positive: root has one child
	if len(roots[0].Children) != 1 {
		t.Fatalf("expected 1 child, got %d",
			len(roots[0].Children))
	}

	// Positive: child name matches
	childName := roots[0].Children[0].Span.Name
	if childName != "child-op" {
		t.Fatalf("expected child-op, got %q", childName)
	}

	// Negative: child should have no children
	if len(roots[0].Children[0].Children) != 0 {
		t.Fatal("leaf node should have no children")
	}
}

func TestTraceViewOutputIntegration(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	runID := "output-test-001"
	traceID := []byte{
		0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04,
		0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
	}
	rootSID := []byte{0xaa, 0, 0, 0, 0, 0, 0, 0}

	sp := &tracepb.Span{
		TraceId:           traceID,
		SpanId:            rootSID,
		Name:              "api.start_run",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   1_023_000_000,
		Status: &tracepb.Status{
			Code: tracepb.Status_STATUS_CODE_OK,
		},
	}
	data, err := protojson.Marshal(sp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	subject := "telemetry.spans.api." + runID
	_, err = jsLegacy.Publish(subject, data)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	output := captureTraceOutput(func() {
		runTraceViewCmd([]string{runID})
	})

	// Positive: output contains trace header
	if !strings.Contains(output, "Trace:") {
		t.Fatalf(
			"output should contain Trace header, got:\n%s",
			output,
		)
	}

	// Positive: output contains span name
	if !strings.Contains(output, "api.start_run") {
		t.Fatalf(
			"output should contain span name, got:\n%s",
			output,
		)
	}

	// Positive: output contains duration
	if !strings.Contains(output, "23ms") {
		t.Fatalf(
			"output should contain 23ms duration, got:\n%s",
			output,
		)
	}

	// Negative: output should not contain raw base64
	if strings.Contains(output, "3q2+7w") {
		t.Fatal("output should not contain base64 trace ID")
	}
}

func TestTraceSearchIntegration(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Publish spans for two traces.
	tid1 := []byte{
		0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
	}
	tid2 := []byte{
		0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x02,
	}

	publishSpan := func(
		traceID, spanID []byte,
		name string, code tracepb.Status_StatusCode,
	) {
		sp := &tracepb.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              name,
			StartTimeUnixNano: uint64(time.Now().UnixNano()),
			EndTimeUnixNano: uint64(
				time.Now().Add(10 * time.Millisecond).UnixNano(),
			),
			Status: &tracepb.Status{Code: code},
		}
		data, marshalErr := protojson.Marshal(sp)
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}
		_, pubErr := jsLegacy.Publish(
			"telemetry.spans.engine.run1", data,
		)
		if pubErr != nil {
			t.Fatalf("publish: %v", pubErr)
		}
	}

	publishSpan(
		tid1, []byte{0x11, 0, 0, 0, 0, 0, 0, 0},
		"ok-trace", tracepb.Status_STATUS_CODE_OK,
	)
	publishSpan(
		tid2, []byte{0x22, 0, 0, 0, 0, 0, 0, 0},
		"err-trace", tracepb.Status_STATUS_CODE_ERROR,
	)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Search all traces.
	startTime := time.Now().Add(-time.Hour)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	cons, err := js.OrderedConsumer(
		ctx, "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{
				"telemetry.spans.engine.>",
			},
			DeliverPolicy: jetstream.DeliverByStartTimePolicy,
			OptStartTime:  &startTime,
		},
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	flags := traceSearchFlags{
		since: time.Hour,
		limit: 100,
	}
	results := collectSearchResults(cons, flags)

	// Positive: should find 2 traces
	if len(results) != 2 {
		t.Fatalf("expected 2 traces, got %d", len(results))
	}

	// Negative: no result should have empty trace ID
	for _, r := range results {
		if r.TraceID == "" {
			t.Fatal("trace ID must not be empty")
		}
	}
}

func TestFilterAndSortResults(t *testing.T) {
	now := time.Now()
	traces := map[string]*traceInfo{
		"tid-ok": {
			root: &tracepb.Span{
				Name: "ok-root",
				StartTimeUnixNano: uint64(
					now.Add(-time.Minute).UnixNano(),
				),
				EndTimeUnixNano: uint64(
					now.Add(-time.Minute).
						Add(50 * time.Millisecond).UnixNano(),
				),
				Status: &tracepb.Status{
					Code: tracepb.Status_STATUS_CODE_OK,
				},
			},
			count: 3,
		},
		"tid-err": {
			root: &tracepb.Span{
				Name:              "err-root",
				StartTimeUnixNano: uint64(now.UnixNano()),
				EndTimeUnixNano: uint64(
					now.Add(100 * time.Millisecond).UnixNano(),
				),
				Status: &tracepb.Status{
					Code: tracepb.Status_STATUS_CODE_ERROR,
				},
			},
			count: 1,
		},
	}

	// No filter: both returned
	all := filterAndSortResults(traces, traceSearchFlags{
		limit: 100,
	})
	if len(all) != 2 {
		t.Fatalf("expected 2 results, got %d", len(all))
	}

	// Filter by error status
	errOnly := filterAndSortResults(traces, traceSearchFlags{
		status: "error",
		limit:  100,
	})
	if len(errOnly) != 1 {
		t.Fatalf("expected 1 error result, got %d",
			len(errOnly))
	}
	if errOnly[0].Status != "error" {
		t.Fatalf("expected error status, got %q",
			errOnly[0].Status)
	}

	// Negative: ok filter should exclude error
	okOnly := filterAndSortResults(traces, traceSearchFlags{
		status: "ok",
		limit:  100,
	})
	for _, r := range okOnly {
		if r.Status == "error" {
			t.Fatal("ok filter should exclude error traces")
		}
	}
}

// captureTraceOutput captures stdout from a function. Reused
// pattern from dlq_test.go adapted for trace output.
func captureTraceOutput(fn func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 8192)
	n, _ := r.Read(buf)
	return string(buf[:n])
}

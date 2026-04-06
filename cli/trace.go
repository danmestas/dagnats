// cli/trace.go
// Trace viewing and search commands. Reads OTLP proto JSON spans
// from the NATS TELEMETRY stream and displays them as trees or
// summary tables.
package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// extractTraceID pulls the trace ID from a W3C traceparent string.
// Format: "00-{traceID}-{spanID}-{flags}". Returns "" if invalid.
func extractTraceID(traceparent string) string {
	if traceparent == "" {
		return ""
	}
	if len(traceparent) > 256 {
		panic("extractTraceID: traceparent exceeds max length")
	}
	if strings.Count(traceparent, "-") > 10 {
		panic("extractTraceID: too many segments")
	}
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return ""
	}
	return parts[1]
}

// runTraceCmd dispatches trace subcommands.
func runTraceCmd(args []string) {
	if args == nil {
		panic("runTraceCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runTraceCmd: args exceeds max bound")
	}
	if HasHelpFlag(args) {
		printTraceUsage()
		return
	}
	if len(args) == 0 {
		printTraceUsage()
		return
	}
	switch args[0] {
	case "search":
		runTraceSearchCmd(args[1:])
	default:
		runTraceViewCmd(args)
	}
}

// printTraceUsage prints trace subcommand help text.
func printTraceUsage() {
	fmt.Println("Usage: dagnats trace <run-id> [--json]")
	fmt.Println("       dagnats trace search [flags]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  <run-id>  view trace tree for a run")
	fmt.Println("  search    find traces by service/status")
	fmt.Println()
	fmt.Println("Search flags:")
	fmt.Println("  --service=X    filter by service name")
	fmt.Println("  --status=S     filter: ok or error")
	fmt.Println("  --since=1h     lookback duration (default 1h)")
	fmt.Println("  --limit=100    max traces (default 100)")
	fmt.Println("  --json         machine-readable output")
}

const (
	traceViewTimeout  = 5 * time.Second
	traceViewSpanMax  = 10000
	traceSearchMaxSec = 30
	traceSearchMax    = 1000
)

// --- Trace View ---

// runTraceViewCmd displays the trace tree for a specific run.
func runTraceViewCmd(args []string) {
	if args == nil {
		panic("runTraceViewCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runTraceViewCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) == 0 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trace <run-id> [--json]")
		os.Exit(1)
	}
	runID := args[0]

	_, nc := connectService()
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	spans := collectRunSpans(js, runID)
	if len(spans) == 0 {
		fmt.Fprintln(os.Stderr, "No spans found for run.")
		os.Exit(1)
	}

	if jsonOutput {
		printSpansJSON(spans)
		return
	}

	printSpanTrees(spans)
}

// collectRunSpans reads all spans for a run from the TELEMETRY
// stream. Bounded by traceViewTimeout and traceViewSpanMax.
func collectRunSpans(
	js jetstream.JetStream, runID string,
) []*tracepb.Span {
	if js == nil {
		panic("collectRunSpans: js must not be nil")
	}
	if runID == "" {
		panic("collectRunSpans: runID must not be empty")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), traceViewTimeout,
	)
	defer cancel()

	subject := "telemetry.spans.*." + runID
	cons, err := js.OrderedConsumer(
		ctx, "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
			DeliverPolicy:  jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "consumer: %v\n", err)
		return nil
	}

	spans := make([]*tracepb.Span, 0, 128)
	for i := 0; i < traceViewSpanMax; i++ {
		msg, fetchErr := cons.Next(
			jetstream.FetchMaxWait(time.Second),
		)
		if fetchErr != nil {
			break
		}
		sp, parseErr := parseSpan(msg.Data())
		if parseErr != nil {
			continue
		}
		spans = append(spans, sp)
	}
	return spans
}

// parseSpan deserializes OTLP proto JSON into a tracepb.Span.
func parseSpan(data []byte) (*tracepb.Span, error) {
	if data == nil {
		panic("parseSpan: data must not be nil")
	}
	if len(data) > 1<<20 {
		panic("parseSpan: data exceeds 1MB bound")
	}
	sp := &tracepb.Span{}
	err := protojson.Unmarshal(data, sp)
	return sp, err
}

// spanHexTraceID returns the hex-encoded trace ID string.
func spanHexTraceID(sp *tracepb.Span) string {
	if sp == nil {
		panic("spanHexTraceID: span must not be nil")
	}
	return hex.EncodeToString(sp.TraceId)
}

// spanHexSpanID returns the hex-encoded span ID string.
func spanHexSpanID(sp *tracepb.Span) string {
	if sp == nil {
		panic("spanHexSpanID: span must not be nil")
	}
	return hex.EncodeToString(sp.SpanId)
}

// spanHexParentID returns the hex-encoded parent span ID, or "".
func spanHexParentID(sp *tracepb.Span) string {
	if sp == nil {
		panic("spanHexParentID: span must not be nil")
	}
	if len(sp.ParentSpanId) == 0 {
		return ""
	}
	return hex.EncodeToString(sp.ParentSpanId)
}

// spanDurationMs computes span duration in milliseconds.
func spanDurationMs(sp *tracepb.Span) int64 {
	if sp == nil {
		panic("spanDurationMs: span must not be nil")
	}
	startNs := int64(sp.StartTimeUnixNano)
	endNs := int64(sp.EndTimeUnixNano)
	if endNs <= startNs {
		return 0
	}
	return (endNs - startNs) / 1_000_000
}

// spanStatusLabel returns "ok", "error", or "unset".
func spanStatusLabel(sp *tracepb.Span) string {
	if sp == nil {
		panic("spanStatusLabel: span must not be nil")
	}
	if sp.Status == nil {
		return "unset"
	}
	switch sp.Status.Code {
	case tracepb.Status_STATUS_CODE_OK:
		return "ok"
	case tracepb.Status_STATUS_CODE_ERROR:
		return "error"
	default:
		return "unset"
	}
}

// --- Tree building and display ---

// spanNode is a tree node wrapping a span with children.
type spanNode struct {
	Span     *tracepb.Span
	Children []*spanNode
}

// buildSpanTrees groups spans by trace ID, links parent/child,
// and returns root nodes sorted by start time.
func buildSpanTrees(
	spans []*tracepb.Span,
) map[string][]*spanNode {
	if len(spans) > traceViewSpanMax {
		panic("buildSpanTrees: spans exceeds max bound")
	}

	// Index all nodes by spanID.
	nodeMap := make(map[string]*spanNode, len(spans))
	for _, sp := range spans {
		sid := spanHexSpanID(sp)
		nodeMap[sid] = &spanNode{Span: sp}
	}

	// Group roots by traceID.
	traceRoots := make(map[string][]*spanNode)
	for _, sp := range spans {
		sid := spanHexSpanID(sp)
		pid := spanHexParentID(sp)
		tid := spanHexTraceID(sp)
		node := nodeMap[sid]

		if pid != "" {
			if parent, ok := nodeMap[pid]; ok {
				parent.Children = append(
					parent.Children, node,
				)
				continue
			}
		}
		traceRoots[tid] = append(traceRoots[tid], node)
	}

	// Sort children by start time at each level.
	for _, node := range nodeMap {
		sortNodeChildren(node)
	}
	// Sort roots by start time.
	for tid := range traceRoots {
		sortNodes(traceRoots[tid])
	}
	return traceRoots
}

// sortNodes sorts span nodes by start time ascending.
func sortNodes(nodes []*spanNode) {
	if len(nodes) > traceViewSpanMax {
		panic("sortNodes: nodes exceeds max bound")
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Span.StartTimeUnixNano <
			nodes[j].Span.StartTimeUnixNano
	})
}

// sortNodeChildren sorts a node's children by start time.
func sortNodeChildren(node *spanNode) {
	if node == nil {
		panic("sortNodeChildren: node must not be nil")
	}
	if len(node.Children) > 1 {
		sortNodes(node.Children)
	}
}

// printSpanTrees renders all trace trees to stdout.
func printSpanTrees(spans []*tracepb.Span) {
	if len(spans) == 0 {
		panic("printSpanTrees: spans must not be empty")
	}
	if len(spans) > traceViewSpanMax {
		panic("printSpanTrees: spans exceeds max bound")
	}

	trees := buildSpanTrees(spans)
	traceIDs := sortedTraceIDs(trees)

	for _, tid := range traceIDs {
		roots := trees[tid]
		displayTID := tid
		if len(displayTID) > 16 {
			displayTID = displayTID[:16] + "..."
		}
		fmt.Printf("Trace: %s\n", displayTID)
		for i, root := range roots {
			isLast := i == len(roots)-1
			printNode(root, "", isLast)
		}
		fmt.Println()
	}
}

// sortedTraceIDs returns trace IDs sorted alphabetically.
func sortedTraceIDs(
	trees map[string][]*spanNode,
) []string {
	if len(trees) > traceSearchMax {
		panic("sortedTraceIDs: trees exceeds max bound")
	}
	ids := make([]string, 0, len(trees))
	for tid := range trees {
		ids = append(ids, tid)
	}
	sort.Strings(ids)
	return ids
}

// printNode renders a single tree node with box-drawing lines.
func printNode(
	node *spanNode, prefix string, isLast bool,
) {
	if node == nil {
		panic("printNode: node must not be nil")
	}

	connector := "├─ "
	if isLast {
		connector = "└─ "
	}

	sp := node.Span
	dur := spanDurationMs(sp)
	status := spanStatusLabel(sp)
	statusColored := colorTraceStatus(status)

	fmt.Printf("%s%s%s (%dms) [%s]\n",
		prefix, connector, sp.Name,
		dur, statusColored,
	)

	childPrefix := prefix + "│  "
	if isLast {
		childPrefix = prefix + "   "
	}

	for i, child := range node.Children {
		childIsLast := i == len(node.Children)-1
		printNode(child, childPrefix, childIsLast)
	}
}

// colorTraceStatus applies color to a trace status label.
func colorTraceStatus(status string) string {
	switch status {
	case "ok":
		return ColorGreen("ok")
	case "error":
		return ColorRed("error")
	default:
		return ColorGray("unset")
	}
}

// printSpansJSON outputs spans as a JSON array.
func printSpansJSON(spans []*tracepb.Span) {
	if spans == nil {
		panic("printSpansJSON: spans must not be nil")
	}
	if len(spans) > traceViewSpanMax {
		panic("printSpansJSON: spans exceeds max bound")
	}

	type jsonSpan struct {
		TraceID    string `json:"trace_id"`
		SpanID     string `json:"span_id"`
		ParentID   string `json:"parent_span_id,omitempty"`
		Name       string `json:"name"`
		DurationMs int64  `json:"duration_ms"`
		Status     string `json:"status"`
		StartTime  string `json:"start_time"`
	}

	out := make([]jsonSpan, 0, len(spans))
	for _, sp := range spans {
		startNs := int64(sp.StartTimeUnixNano)
		startTime := time.Unix(
			0, startNs,
		).UTC().Format(time.RFC3339Nano)
		out = append(out, jsonSpan{
			TraceID:    spanHexTraceID(sp),
			SpanID:     spanHexSpanID(sp),
			ParentID:   spanHexParentID(sp),
			Name:       sp.Name,
			DurationMs: spanDurationMs(sp),
			Status:     spanStatusLabel(sp),
			StartTime:  startTime,
		})
	}
	if err := FormatJSON(os.Stdout, out); err != nil {
		fmt.Fprintf(os.Stderr, "format json: %v\n", err)
		os.Exit(1)
	}
}

// --- Trace Search ---

// traceSearchFlags holds parsed search flags.
type traceSearchFlags struct {
	service    string
	status     string
	since      time.Duration
	limit      int
	jsonOutput bool
}

// parseTraceSearchFlags extracts flags from args.
func parseTraceSearchFlags(args []string) traceSearchFlags {
	if args == nil {
		panic("parseTraceSearchFlags: args must not be nil")
	}
	if len(args) > 100 {
		panic("parseTraceSearchFlags: args exceeds max bound")
	}

	flags := traceSearchFlags{
		since: time.Hour,
		limit: 100,
	}
	flags.jsonOutput = HasJSONFlag(args)

	for _, arg := range args {
		if strings.HasPrefix(arg, "--service=") {
			flags.service = strings.TrimPrefix(
				arg, "--service=",
			)
		}
		if strings.HasPrefix(arg, "--status=") {
			flags.status = strings.TrimPrefix(
				arg, "--status=",
			)
		}
		if strings.HasPrefix(arg, "--since=") {
			val := strings.TrimPrefix(arg, "--since=")
			dur, err := time.ParseDuration(val)
			if err != nil {
				fmt.Fprintf(os.Stderr,
					"invalid --since: %v\n", err)
				os.Exit(1)
			}
			flags.since = dur
		}
		if strings.HasPrefix(arg, "--limit=") {
			val := strings.TrimPrefix(arg, "--limit=")
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				fmt.Fprintln(os.Stderr,
					"--limit must be a positive integer")
				os.Exit(1)
			}
			if n > traceSearchMax {
				n = traceSearchMax
			}
			flags.limit = n
		}
	}
	return flags
}

// buildSearchSubject constructs the filter subject for search.
func buildSearchSubject(service string) string {
	if len(service) > 200 {
		panic(
			"buildSearchSubject: service name unreasonably long",
		)
	}
	if service == "" {
		return "telemetry.spans.>"
	}
	return "telemetry.spans." + service + ".>"
}

// runTraceSearchCmd finds traces matching filter criteria.
func runTraceSearchCmd(args []string) {
	if args == nil {
		panic("runTraceSearchCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runTraceSearchCmd: args exceeds max bound")
	}

	if HasHelpFlag(args) {
		printTraceUsage()
		return
	}

	flags := parseTraceSearchFlags(args)
	_, nc := connectService()
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	results := searchTraces(js, flags)
	if len(results) == 0 {
		fmt.Fprintln(os.Stderr, "No traces found.")
		os.Exit(1)
	}

	if flags.jsonOutput {
		if err := FormatJSON(os.Stdout, results); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	printSearchResults(results)
}

// traceSearchResult represents a found trace for display.
type traceSearchResult struct {
	TraceID    string `json:"trace_id"`
	RootName   string `json:"root_name"`
	DurationMs int64  `json:"duration_ms"`
	Status     string `json:"status"`
	StartTime  string `json:"start_time"`
	SpanCount  int    `json:"span_count"`
}

// searchTraces scans spans from the TELEMETRY stream and
// collects unique traces with their root span info.
func searchTraces(
	js jetstream.JetStream, flags traceSearchFlags,
) []traceSearchResult {
	if js == nil {
		panic("searchTraces: js must not be nil")
	}

	startTime := time.Now().Add(-flags.since)
	subject := buildSearchSubject(flags.service)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(traceSearchMaxSec)*time.Second,
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
		fmt.Fprintf(os.Stderr, "consumer: %v\n", err)
		return nil
	}

	return collectSearchResults(cons, flags)
}

// collectSearchResults reads spans and aggregates by trace ID.
func collectSearchResults(
	cons jetstream.Consumer, flags traceSearchFlags,
) []traceSearchResult {
	if cons == nil {
		panic("collectSearchResults: cons must not be nil")
	}

	traces := make(map[string]*traceInfo)

	const scanMax = 100_000
	for i := 0; i < scanMax; i++ {
		if len(traces) >= traceSearchMax {
			break
		}
		msg, fetchErr := cons.Next(
			jetstream.FetchMaxWait(time.Second),
		)
		if fetchErr != nil {
			break
		}
		sp, parseErr := parseSpan(msg.Data())
		if parseErr != nil {
			continue
		}
		tid := spanHexTraceID(sp)
		info, exists := traces[tid]
		if !exists {
			info = &traceInfo{}
			traces[tid] = info
		}
		info.count++
		if isRootSpan(sp) {
			info.root = sp
		}
	}

	return filterAndSortResults(traces, flags)
}

// isRootSpan returns true when a span has no parent.
func isRootSpan(sp *tracepb.Span) bool {
	if sp == nil {
		panic("isRootSpan: span must not be nil")
	}
	return len(sp.ParentSpanId) == 0
}

// filterAndSortResults converts the trace map to results,
// applying status filter and limit.
func filterAndSortResults(
	traces map[string]*traceInfo,
	flags traceSearchFlags,
) []traceSearchResult {
	if len(traces) > traceSearchMax+1 {
		panic(
			"filterAndSortResults: traces exceeds max bound",
		)
	}

	results := make([]traceSearchResult, 0, len(traces))
	for tid, info := range traces {
		result := traceSearchResult{
			TraceID:   tid,
			SpanCount: info.count,
		}
		if info.root != nil {
			result.RootName = info.root.Name
			result.DurationMs = spanDurationMs(info.root)
			result.Status = spanStatusLabel(info.root)
			startNs := int64(info.root.StartTimeUnixNano)
			result.StartTime = time.Unix(
				0, startNs,
			).UTC().Format(time.RFC3339)
		} else {
			result.RootName = "(no root)"
			result.Status = "unset"
		}

		if flags.status != "" && result.Status != flags.status {
			continue
		}
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].StartTime > results[j].StartTime
	})

	if len(results) > flags.limit {
		results = results[:flags.limit]
	}
	return results
}

// traceInfo tracks a trace during search scanning.
type traceInfo struct {
	root  *tracepb.Span
	count int
}

// printSearchResults displays search results in a table.
func printSearchResults(results []traceSearchResult) {
	if len(results) == 0 {
		panic("printSearchResults: results must not be empty")
	}
	if len(results) > traceSearchMax {
		panic("printSearchResults: results exceeds max bound")
	}

	fmt.Printf("%-18s %-30s %8s %-6s %s\n",
		"TRACE ID", "ROOT SPAN", "DURATION",
		"STATUS", "TIME",
	)
	for _, r := range results {
		tid := r.TraceID
		if len(tid) > 16 {
			tid = tid[:16]
		}
		name := r.RootName
		if len(name) > 28 {
			name = name[:28] + ".."
		}
		durStr := strconv.FormatInt(r.DurationMs, 10) + "ms"
		status := colorTraceStatus(r.Status)
		ts := r.StartTime
		if len(ts) > 19 {
			ts = ts[:19]
		}
		fmt.Printf("%-18s %-30s %8s %-6s %s\n",
			tid, name, durStr, status, ts,
		)
	}
}

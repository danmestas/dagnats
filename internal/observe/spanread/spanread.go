// Package spanread reads OTLP proto-JSON spans for a single run from
// the NATS TELEMETRY stream and links them into per-trace span trees.
//
// The logic was extracted from cli/trace.go so both the CLI trace view
// and the console run-trace tab share one source of truth for span
// collection, parsing, and tree building.
package spanread

import (
	"context"
	"encoding/hex"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// MaxSpans bounds how many spans a single run trace may carry. Both the
// collection loop and the tree builders panic above this bound so a
// runaway trace fails loudly rather than exhausting memory.
const MaxSpans = 10000

// CollectRunSpans reads up to maxSpans spans for runID from the
// TELEMETRY stream via an ordered consumer. The drain ends when the
// stream is exhausted (a 1s fetch wait elapses) or maxSpans is reached.
// Returns the collected spans; a consumer-creation failure returns the
// error so the caller can surface it.
func CollectRunSpans(
	ctx context.Context, js jetstream.JetStream,
	runID string, maxSpans int,
) ([]*tracepb.Span, error) {
	if js == nil {
		panic("CollectRunSpans: js must not be nil")
	}
	if runID == "" {
		panic("CollectRunSpans: runID must not be empty")
	}

	subject := "telemetry.spans.*." + runID
	cons, err := js.OrderedConsumer(
		ctx, "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
			DeliverPolicy:  jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		return nil, err
	}

	spans := make([]*tracepb.Span, 0, 128)
	for i := 0; i < maxSpans; i++ {
		msg, fetchErr := cons.Next(
			jetstream.FetchMaxWait(time.Second),
		)
		if fetchErr != nil {
			break
		}
		sp, parseErr := ParseSpan(msg.Data())
		if parseErr != nil {
			continue
		}
		spans = append(spans, sp)
	}
	return spans, nil
}

// ParseSpan deserializes OTLP proto JSON into a tracepb.Span.
func ParseSpan(data []byte) (*tracepb.Span, error) {
	if data == nil {
		panic("ParseSpan: data must not be nil")
	}
	if len(data) > 1<<20 {
		panic("ParseSpan: data exceeds 1MB bound")
	}
	sp := &tracepb.Span{}
	err := protojson.Unmarshal(data, sp)
	return sp, err
}

// HexTraceID returns the hex-encoded trace ID string.
func HexTraceID(sp *tracepb.Span) string {
	if sp == nil {
		panic("HexTraceID: span must not be nil")
	}
	return hex.EncodeToString(sp.TraceId)
}

// HexSpanID returns the hex-encoded span ID string.
func HexSpanID(sp *tracepb.Span) string {
	if sp == nil {
		panic("HexSpanID: span must not be nil")
	}
	return hex.EncodeToString(sp.SpanId)
}

// HexParentID returns the hex-encoded parent span ID, or "".
func HexParentID(sp *tracepb.Span) string {
	if sp == nil {
		panic("HexParentID: span must not be nil")
	}
	if len(sp.ParentSpanId) == 0 {
		return ""
	}
	return hex.EncodeToString(sp.ParentSpanId)
}

// DurationMs computes span duration in milliseconds.
func DurationMs(sp *tracepb.Span) int64 {
	if sp == nil {
		panic("DurationMs: span must not be nil")
	}
	startNs := int64(sp.StartTimeUnixNano)
	endNs := int64(sp.EndTimeUnixNano)
	if endNs <= startNs {
		return 0
	}
	return (endNs - startNs) / 1_000_000
}

// StatusLabel returns "ok", "error", or "unset".
func StatusLabel(sp *tracepb.Span) string {
	if sp == nil {
		panic("StatusLabel: span must not be nil")
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

// SpanNode is a tree node wrapping a span with its children.
type SpanNode struct {
	Span     *tracepb.Span
	Children []*SpanNode
}

// BuildSpanTrees groups spans by trace ID, links parent/child, and
// returns root nodes per trace ID sorted by start time. A span whose
// parent is not present in the input becomes a root of its trace.
func BuildSpanTrees(spans []*tracepb.Span) map[string][]*SpanNode {
	if len(spans) > MaxSpans {
		panic("BuildSpanTrees: spans exceeds max bound")
	}

	// Index all nodes by spanID.
	nodeMap := make(map[string]*SpanNode, len(spans))
	for _, sp := range spans {
		sid := HexSpanID(sp)
		nodeMap[sid] = &SpanNode{Span: sp}
	}

	// Group roots by traceID.
	traceRoots := make(map[string][]*SpanNode)
	for _, sp := range spans {
		sid := HexSpanID(sp)
		pid := HexParentID(sp)
		tid := HexTraceID(sp)
		node := nodeMap[sid]

		if pid != "" {
			if parent, ok := nodeMap[pid]; ok {
				parent.Children = append(parent.Children, node)
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
func sortNodes(nodes []*SpanNode) {
	if len(nodes) > MaxSpans {
		panic("sortNodes: nodes exceeds max bound")
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Span.StartTimeUnixNano <
			nodes[j].Span.StartTimeUnixNano
	})
}

// sortNodeChildren sorts a node's children by start time.
func sortNodeChildren(node *SpanNode) {
	if node == nil {
		panic("sortNodeChildren: node must not be nil")
	}
	if len(node.Children) > 1 {
		sortNodes(node.Children)
	}
}

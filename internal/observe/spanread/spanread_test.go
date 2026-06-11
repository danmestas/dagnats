// Methodology: pure unit tests over canned tracepb.Span values. No
// NATS — CollectRunSpans's drain loop is exercised by the cli/trace
// integration tests; here we pin the parsing + tree-building helpers
// that the CLI and console both depend on. Each test asserts both the
// positive shape and a negative-space property (orphan-as-root, etc.).
package spanread

import (
	"encoding/hex"
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// makeSpan builds a span with hex-decoded ids and a start time so the
// tree builder has stable ordering input.
func makeSpan(
	traceHex, spanHex, parentHex, name string,
	startNano uint64, code tracepb.Status_StatusCode,
) *tracepb.Span {
	tid, _ := hex.DecodeString(traceHex)
	sid, _ := hex.DecodeString(spanHex)
	var pid []byte
	if parentHex != "" {
		pid, _ = hex.DecodeString(parentHex)
	}
	return &tracepb.Span{
		TraceId:           tid,
		SpanId:            sid,
		ParentSpanId:      pid,
		Name:              name,
		StartTimeUnixNano: startNano,
		EndTimeUnixNano:   startNano + 5_000_000, // +5ms
		Status:            &tracepb.Status{Code: code},
	}
}

func TestBuildSpanTrees_linksRootAndChildren(t *testing.T) {
	const traceHex = "0102030405060708090a0b0c0d0e0f10"
	root := makeSpan(traceHex, "a1a1a1a1a1a1a1a1", "",
		"startRun", 100, tracepb.Status_STATUS_CODE_OK)
	// child2 starts before child1 so we can prove start-time sort.
	child1 := makeSpan(traceHex, "b1b1b1b1b1b1b1b1",
		"a1a1a1a1a1a1a1a1", "step:fetch", 300,
		tracepb.Status_STATUS_CODE_OK)
	child2 := makeSpan(traceHex, "c2c2c2c2c2c2c2c2",
		"a1a1a1a1a1a1a1a1", "step:parse", 200,
		tracepb.Status_STATUS_CODE_ERROR)

	trees := BuildSpanTrees([]*tracepb.Span{root, child1, child2})

	roots := trees[traceHex]
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	if roots[0].Span.Name != "startRun" {
		t.Fatalf("expected startRun root, got %q", roots[0].Span.Name)
	}
	if len(roots[0].Children) != 2 {
		t.Fatalf("expected 2 children, got %d",
			len(roots[0].Children))
	}
	// Children sorted by start time: child2 (200) before child1 (300).
	if roots[0].Children[0].Span.Name != "step:parse" {
		t.Fatalf("expected step:parse first (start-time sort), got %q",
			roots[0].Children[0].Span.Name)
	}
	if roots[0].Children[1].Span.Name != "step:fetch" {
		t.Fatalf("expected step:fetch second, got %q",
			roots[0].Children[1].Span.Name)
	}
}

func TestBuildSpanTrees_orphanBecomesRoot(t *testing.T) {
	const traceHex = "1112131415161718191a1b1c1d1e1f20"
	// Parent id references a span not present in the input.
	orphan := makeSpan(traceHex, "d1d1d1d1d1d1d1d1",
		"ffffffffffffffff", "orphan", 50,
		tracepb.Status_STATUS_CODE_UNSET)

	trees := BuildSpanTrees([]*tracepb.Span{orphan})

	roots := trees[traceHex]
	if len(roots) != 1 {
		t.Fatalf("expected orphan promoted to root, got %d roots",
			len(roots))
	}
	if len(roots[0].Children) != 0 {
		t.Fatalf("orphan root should have no children, got %d",
			len(roots[0].Children))
	}
}

func TestParseSpan_roundTrips(t *testing.T) {
	const traceHex = "2122232425262728292a2b2c2d2e2f30"
	orig := makeSpan(traceHex, "e1e1e1e1e1e1e1e1", "",
		"round", 1000, tracepb.Status_STATUS_CODE_OK)

	data, err := protojson.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := ParseSpan(data)
	if err != nil {
		t.Fatalf("ParseSpan: %v", err)
	}
	if got.Name != "round" {
		t.Fatalf("expected name round, got %q", got.Name)
	}
	if HexTraceID(got) != traceHex {
		t.Fatalf("expected trace %q, got %q", traceHex, HexTraceID(got))
	}
	if HexSpanID(got) != "e1e1e1e1e1e1e1e1" {
		t.Fatalf("unexpected span id %q", HexSpanID(got))
	}
}

func TestHelpers_onKnownSpan(t *testing.T) {
	const traceHex = "3132333435363738393a3b3c3d3e3f40"
	sp := makeSpan(traceHex, "f1f1f1f1f1f1f1f1",
		"a0a0a0a0a0a0a0a0", "known", 0,
		tracepb.Status_STATUS_CODE_ERROR)

	if HexParentID(sp) != "a0a0a0a0a0a0a0a0" {
		t.Fatalf("unexpected parent id %q", HexParentID(sp))
	}
	// 5ms span (end = start + 5_000_000ns).
	if DurationMs(sp) != 5 {
		t.Fatalf("expected 5ms, got %d", DurationMs(sp))
	}
	if StatusLabel(sp) != "error" {
		t.Fatalf("expected error status, got %q", StatusLabel(sp))
	}

	// Negative space: a root span reports no parent and unset status.
	rootless := makeSpan(traceHex, "0909090909090909", "",
		"root", 0, tracepb.Status_STATUS_CODE_UNSET)
	if HexParentID(rootless) != "" {
		t.Fatalf("expected empty parent id, got %q",
			HexParentID(rootless))
	}
	if StatusLabel(rootless) != "unset" {
		t.Fatalf("expected unset status, got %q",
			StatusLabel(rootless))
	}
}

// Methodology: drive the run-trace tab end-to-end against the in-memory
// fakeDataSource (no NATS) plus pure unit coverage of flattenSpanTree
// over canned spanread trees. Each test asserts the positive shape and
// a negative-space property (empty state shows no fabricated span,
// status-ok does not leak the failed class).
package console

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/observe/spanread"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// seedTraceRun wires a fake with one completed run so the trace-tab
// route resolves, and returns the mounted handler.
func seedTraceRun(t *testing.T, fake *fakeDataSource) http.Handler {
	t.Helper()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-tr", "alpha", dag.RunStatusCompleted,
			map[string]dag.StepState{
				"first": {Status: dag.StepStatusCompleted, Attempts: 1},
			}, time.Now().Add(-time.Minute)),
	}
	return mountWithFake(t, fake)
}

func TestServeRunTraceTab_rendersRows(t *testing.T) {
	fake := newFakeDS()
	fake.runTrace = []TraceRow{
		{Depth: 0, Name: "startRun", DurationMs: 2410,
			Status: "ok", SpanID: "a1"},
		{Depth: 1, Name: "step:fetch", DurationMs: 1100,
			Status: "ok", SpanID: "b2"},
	}
	h := seedTraceRun(t, fake)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/api/run/run-tr/trace-tab", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s",
			rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	// Positive: both span names + the patch target appear.
	for _, sub := range []string{"startRun", "step:fetch", "panel-trace"} {
		if !strings.Contains(body, sub) {
			t.Errorf("trace fragment missing %q; body=%s", sub, body)
		}
	}
	// Negative: the honest empty-state copy must NOT appear when spans
	// exist.
	if strings.Contains(body, "No spans recorded") {
		t.Errorf("populated trace must not show empty state; body=%s",
			body)
	}
}

func TestServeRunTraceTab_emptyState(t *testing.T) {
	fake := newFakeDS()
	fake.runTrace = nil // run produced no telemetry
	h := seedTraceRun(t, fake)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/api/run/run-tr/trace-tab", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s",
			rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	// Positive: the honest empty-state copy renders.
	if !strings.Contains(body, "No spans recorded for this run") {
		t.Errorf("empty trace missing empty-state copy; body=%s", body)
	}
	// Negative: no fabricated span row.
	if strings.Contains(body, "run-trace-row-") {
		t.Errorf("empty trace must not fabricate a span row; body=%s",
			body)
	}
}

func TestServeRunTraceTab_statusHighlight(t *testing.T) {
	fake := newFakeDS()
	fake.runTrace = []TraceRow{
		{Depth: 0, Name: "errStep", DurationMs: 10,
			Status: "error", SpanID: "e1"},
	}
	h := seedTraceRun(t, fake)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/api/run/run-tr/trace-tab", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	errBody := rr.Body.String()
	// Positive: error rows carry the status-failed highlight class.
	if !strings.Contains(errBody, "status-failed") {
		t.Errorf("error span must use status-failed; body=%s", errBody)
	}

	// Negative space: an ok-only trace must NOT carry status-failed.
	okFake := newFakeDS()
	okFake.runTrace = []TraceRow{
		{Depth: 0, Name: "okStep", DurationMs: 10,
			Status: "ok", SpanID: "o1"},
	}
	okH := seedTraceRun(t, okFake)
	okRR := httptest.NewRecorder()
	okH.ServeHTTP(okRR, httptest.NewRequest(
		http.MethodGet, "/console/api/run/run-tr/trace-tab", nil))
	if strings.Contains(okRR.Body.String(), "status-failed") {
		t.Errorf("ok trace must not contain status-failed; body=%s",
			okRR.Body.String())
	}
}

func TestFlattenSpanTree(t *testing.T) {
	const traceHex = "0102030405060708090a0b0c0d0e0f10"
	mk := func(spanHex, parentHex, name string, start uint64,
		code tracepb.Status_StatusCode) *tracepb.Span {
		tid, _ := hex.DecodeString(traceHex)
		sid, _ := hex.DecodeString(spanHex)
		var pid []byte
		if parentHex != "" {
			pid, _ = hex.DecodeString(parentHex)
		}
		return &tracepb.Span{
			TraceId: tid, SpanId: sid, ParentSpanId: pid,
			Name:              name,
			StartTimeUnixNano: start,
			EndTimeUnixNano:   start + 1_000_000,
			Status:            &tracepb.Status{Code: code},
		}
	}
	root := mk("a1a1a1a1a1a1a1a1", "", "root", 100,
		tracepb.Status_STATUS_CODE_OK)
	// kid2 starts before kid1 to prove start-time order survives flatten.
	kid1 := mk("b1b1b1b1b1b1b1b1", "a1a1a1a1a1a1a1a1", "kid-late", 300,
		tracepb.Status_STATUS_CODE_OK)
	kid2 := mk("c2c2c2c2c2c2c2c2", "a1a1a1a1a1a1a1a1", "kid-early", 200,
		tracepb.Status_STATUS_CODE_ERROR)

	trees := spanread.BuildSpanTrees(
		[]*tracepb.Span{root, kid1, kid2})
	rows := flattenSpanTree(trees)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Pre-order depths: root(0), then its children at depth 1.
	gotDepths := []int{rows[0].Depth, rows[1].Depth, rows[2].Depth}
	wantDepths := []int{0, 1, 1}
	for i := range wantDepths {
		if gotDepths[i] != wantDepths[i] {
			t.Fatalf("depth[%d] = %d, want %d (depths=%v)",
				i, gotDepths[i], wantDepths[i], gotDepths)
		}
	}
	// Name order: root, then kid-early (200) before kid-late (300).
	wantNames := []string{"root", "kid-early", "kid-late"}
	for i, want := range wantNames {
		if rows[i].Name != want {
			t.Fatalf("row[%d].Name = %q, want %q",
				i, rows[i].Name, want)
		}
	}
	// Negative: the error child surfaces its status, not the root's ok.
	if rows[1].Status != "error" {
		t.Fatalf("kid-early status = %q, want error", rows[1].Status)
	}
}

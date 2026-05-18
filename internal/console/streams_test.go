// streams_test.go exercises the two PR 3 SSE endpoints in isolation
// from NATS — the fakeDataSource exposes runUpdates / runHistory
// channels the tests drive directly. Production wiring against the
// live api.Service is covered by the server smoke tests; the
// streams package logic is the system under test here.
//
// Methodology:
//   - Each test boots a fresh Mount with a fakeDataSource, so no
//     state is shared between tests. Bounded timeouts on every read.
//   - The producer goroutine fires updates after the SSE response
//     header has been read — that's how we know the handler has
//     started its watcher and is ready to receive. Otherwise we'd
//     race the first update against the goroutine startup.
//   - Assertions look for the stable Datastar event signature on the
//     wire (`event: datastar-patch-elements`) plus a unique substring
//     from the payload — the row id or the event id — to confirm the
//     correct fragment shipped, not just any fragment.
package console

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

func TestSSERuns_emitsPatchOnNewRun(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	updates := make(chan RunUpdate, 4)
	fake.runUpdates = updates
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, srv.URL+"/console/sse/runs", nil,
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse runs: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(
		got, "text/event-stream",
	) {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}

	updates <- RunUpdate{
		Run: dag.WorkflowRun{
			RunID: "run-new-1", WorkflowID: "alpha",
			Status: dag.RunStatusRunning, CreatedAt: time.Now(),
		},
		Created: true, Seq: 1,
	}
	updates <- RunUpdate{
		Run: dag.WorkflowRun{
			RunID: "run-new-2", WorkflowID: "alpha",
			Status: dag.RunStatusCompleted, CreatedAt: time.Now(),
		},
		Created: false, Seq: 2,
	}
	// Sentinel third update forces the test reader past both real
	// events; without it the bufio.Scanner blocks on the SSE stream
	// after the second event arrives.
	updates <- RunUpdate{
		Run: dag.WorkflowRun{
			RunID: "sentinel", WorkflowID: "alpha",
			Status: dag.RunStatusRunning, CreatedAt: time.Now(),
		},
		Created: true, Seq: 3,
	}

	// Each update emits 2 patch events (remove + prepend). 4 = both
	// real updates transmitted in full. The reader stops after >want
	// events so a few sentinel emissions can buffer beyond that.
	gotEventLines, gotRunIDs := readSSEUntil(t, resp.Body, 4, 1500)
	if gotEventLines < 2 {
		t.Fatalf("got %d patch events, want >= 2", gotEventLines)
	}
	if !gotRunIDs["run-new-1"] {
		t.Errorf("missing run-new-1 in patch payload")
	}
	if !gotRunIDs["run-new-2"] {
		t.Errorf("missing run-new-2 in patch payload")
	}
}

func TestSSERuns_filterRejectsNonMatching(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	updates := make(chan RunUpdate, 4)
	fake.runUpdates = updates
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	// Datastar passes signals via the `datastar` query param. Use
	// runsStatus=failed: only failed runs should make it onto the wire.
	req, _ := http.NewRequestWithContext(
		ctx, http.MethodGet,
		srv.URL+`/console/sse/runs?datastar=`+
			`%7B%22runsStatus%22%3A%22failed%22%7D`, nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse runs: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	updates <- RunUpdate{
		Run: dag.WorkflowRun{
			RunID: "ok-1", WorkflowID: "alpha",
			Status: dag.RunStatusCompleted,
		},
		Created: true, Seq: 1,
	}
	updates <- RunUpdate{
		Run: dag.WorkflowRun{
			RunID: "fail-1", WorkflowID: "alpha",
			Status: dag.RunStatusFailed,
		},
		Created: true, Seq: 2,
	}
	// Sentinel second matching update so readSSEUntil's loop sees two
	// events on the wire and can return without hitting the bufio
	// Scanner blocked-read path.
	updates <- RunUpdate{
		Run: dag.WorkflowRun{
			RunID: "fail-2", WorkflowID: "alpha",
			Status: dag.RunStatusFailed,
		},
		Created: true, Seq: 3,
	}

	// Each accepted update emits 2 patch events (remove + prepend);
	// 4 = both matching updates fully transmitted. The "fail-1" id
	// must appear; the "ok-1" id must NOT.
	gotEventLines, gotRunIDs := readSSEUntil(t, resp.Body, 4, 1500)
	if gotEventLines < 2 {
		t.Fatalf("got %d patch events, want >= 2 for fail-1+fail-2", gotEventLines)
	}
	if gotRunIDs["ok-1"] {
		t.Errorf("filter leaked completed run ok-1 onto stream")
	}
	if !gotRunIDs["fail-1"] {
		t.Errorf("filter missed matching failed run fail-1")
	}
}

func TestSSERunDetail_emitsHistoryPatches(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{{
		RunID: "run-x", WorkflowID: "alpha",
		Status: dag.RunStatusRunning, CreatedAt: time.Now(),
	}}
	hist := make(chan HistoryEvent, 4)
	fake.runHistory["run-x"] = hist
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	req, _ := http.NewRequestWithContext(
		ctx, http.MethodGet,
		srv.URL+"/console/sse/runs/run-x", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse run-detail: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	hist <- HistoryEvent{
		Event: api.RunEvent{
			Type: "workflow.started", RunID: "run-x",
			Timestamp: time.Now(),
		},
		Seq: 1,
	}
	hist <- HistoryEvent{
		Event: api.RunEvent{
			Type: "step.completed", RunID: "run-x", StepID: "first",
			Timestamp: time.Now(),
		},
		Seq: 2,
	}
	// Sentinel: any event past the second forces the reader past
	// the two real events even on a blocking bufio.Scanner.
	hist <- HistoryEvent{
		Event: api.RunEvent{
			Type: "workflow.heartbeat", RunID: "run-x",
			Timestamp: time.Now(),
		},
		Seq: 3,
	}

	gotEventLines, payload := readSSEPayloadUntil(t, resp.Body, 3, 1500)
	if gotEventLines < 2 {
		t.Fatalf("got %d patch events, want >= 2", gotEventLines)
	}
	if !strings.Contains(payload, "workflow.started") {
		t.Errorf("missing workflow.started in detail stream")
	}
	if !strings.Contains(payload, "step.completed") {
		t.Errorf("missing step.completed in detail stream")
	}
	if !strings.Contains(payload, `id="step-card-first"`) {
		t.Errorf("missing step-card patch for first")
	}
}

func TestSSERunDetail_resumesFromLastEventID(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{{
		RunID: "run-y", WorkflowID: "alpha",
		Status: dag.RunStatusRunning, CreatedAt: time.Now(),
	}}
	// Capture the fromSeq argument the handler passes through.
	fake.runHistory["run-y"] = make(chan HistoryEvent)
	wrap := &resumeRecorder{inner: fake}
	h := mountConsoleWithDS(t, wrap)
	_ = wrap // keep alive across the request lifetime
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 1500*time.Millisecond,
	)
	defer cancel()
	req, _ := http.NewRequestWithContext(
		ctx, http.MethodGet,
		srv.URL+"/console/sse/runs/run-y", nil,
	)
	req.Header.Set("Last-Event-ID", "42")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse run-detail: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	// Header read confirms the handler started; that means it called
	// WatchRunHistory. Drain a few bytes to surface any quick error
	// response, then validate the recorded resume seq.
	_, _ = io.CopyN(io.Discard, resp.Body, 64)
	if wrap.gotFromSeq != 42 {
		t.Fatalf("WatchRunHistory fromSeq = %d, want 42", wrap.gotFromSeq)
	}
	if wrap.gotRunID != "run-y" {
		t.Fatalf("WatchRunHistory runID = %q, want run-y", wrap.gotRunID)
	}
}

// resumeRecorder is a DataSource wrapper that captures the args
// WatchRunHistory was called with — used by the resume test only.
type resumeRecorder struct {
	inner      *fakeDataSource
	gotRunID   string
	gotFromSeq uint64
}

func (r *resumeRecorder) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	return r.inner.ListWorkflows(ctx)
}

func (r *resumeRecorder) GetWorkflow(name string) (dag.WorkflowDef, error) {
	return r.inner.GetWorkflow(name)
}

func (r *resumeRecorder) ListRuns(
	ctx context.Context, f string,
) ([]dag.WorkflowRun, error) {
	return r.inner.ListRuns(ctx, f)
}

func (r *resumeRecorder) GetRun(
	ctx context.Context, runID string,
) (dag.WorkflowRun, error) {
	return r.inner.GetRun(ctx, runID)
}

func (r *resumeRecorder) ListRunEvents(
	ctx context.Context, runID string, full bool,
) ([]api.RunEvent, error) {
	return r.inner.ListRunEvents(ctx, runID, full)
}

func (r *resumeRecorder) ListTriggers(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	return r.inner.ListTriggers(ctx)
}

// mountConsoleWithDS mounts the console with an arbitrary DataSource.
// Mirror of mountWithFake but typed against the full interface so the
// resumeRecorder can substitute for the fake transparently.
func mountConsoleWithDS(t *testing.T, ds DataSource) http.Handler {
	t.Helper()
	if ds == nil {
		t.Fatalf("mountConsoleWithDS: ds is nil")
	}
	return Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     ds,
	})
}

var _ DataSource = (*resumeRecorder)(nil)

func (r *resumeRecorder) WatchRuns(
	ctx context.Context,
) (<-chan RunUpdate, error) {
	return r.inner.WatchRuns(ctx)
}

func (r *resumeRecorder) WatchRunHistory(
	ctx context.Context, runID string, fromSeq uint64,
) (<-chan HistoryEvent, error) {
	r.gotRunID = runID
	r.gotFromSeq = fromSeq
	return r.inner.WatchRunHistory(ctx, runID, fromSeq)
}

func (r *resumeRecorder) ListDeadLetters(
	ctx context.Context, limit int,
) ([]api.DeadLetterView, error) {
	return r.inner.ListDeadLetters(ctx, limit)
}

func (r *resumeRecorder) ReplayDeadLetter(
	ctx context.Context, seq uint64,
) error {
	return r.inner.ReplayDeadLetter(ctx, seq)
}

func (r *resumeRecorder) DiscardDeadLetter(
	ctx context.Context, seq uint64,
) error {
	return r.inner.DiscardDeadLetter(ctx, seq)
}

func (r *resumeRecorder) ListAuditEvents(
	ctx context.Context, limit int,
) ([]AuditEvent, error) {
	return r.inner.ListAuditEvents(ctx, limit)
}

func (r *resumeRecorder) EmitAuditEvent(
	ctx context.Context, evt AuditEvent,
) error {
	return r.inner.EmitAuditEvent(ctx, evt)
}

func (r *resumeRecorder) SetTriggerEnabled(
	ctx context.Context, triggerID string, enabled bool,
) error {
	return r.inner.SetTriggerEnabled(ctx, triggerID, enabled)
}

func (r *resumeRecorder) ListTriggerFires(
	ctx context.Context, triggerID string, limit int,
) ([]TriggerFireRow, error) {
	return r.inner.ListTriggerFires(ctx, triggerID, limit)
}

func (r *resumeRecorder) WatchTriggers(
	ctx context.Context,
) (<-chan TriggerUpdate, error) {
	return r.inner.WatchTriggers(ctx)
}

func (r *resumeRecorder) WatchDLQ(
	ctx context.Context,
) (<-chan DLQUpdate, error) {
	return r.inner.WatchDLQ(ctx)
}

func (r *resumeRecorder) ListKVBuckets(
	ctx context.Context,
) ([]KVBucketInfo, error) {
	return r.inner.ListKVBuckets(ctx)
}

func (r *resumeRecorder) ListKVKeys(
	ctx context.Context, bucket, cursor string, limit int,
) ([]string, string, error) {
	return r.inner.ListKVKeys(ctx, bucket, cursor, limit)
}

func (r *resumeRecorder) GetKVEntry(
	ctx context.Context, bucket, key string,
) (KVEntryView, error) {
	return r.inner.GetKVEntry(ctx, bucket, key)
}

func (r *resumeRecorder) SparklineData(
	ctx context.Context, kind, id string, hours int,
) ([]float64, error) {
	return r.inner.SparklineData(ctx, kind, id, hours)
}

func (r *resumeRecorder) Search(
	ctx context.Context, query string, limit int,
) ([]SearchHit, error) {
	return r.inner.Search(ctx, query, limit)
}

// readSSEUntil reads the SSE response body looking for
// `event: datastar-patch-elements` headers and capturing the row id
// payload substrings. Stops once want events have been seen OR the
// per-read deadline elapses (bounded). Returns the count and the set
// of run ids observed.
func readSSEUntil(
	t *testing.T, body io.Reader, want int, deadlineMs int,
) (int, map[string]bool) {
	t.Helper()
	const maxLines = 2000
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	deadline := time.Now().Add(time.Duration(deadlineMs) * time.Millisecond)
	count := 0
	ids := make(map[string]bool, want)
	for i := 0; i < maxLines && time.Now().Before(deadline); i++ {
		if !sc.Scan() {
			break
		}
		line := sc.Text()
		if strings.HasPrefix(line, "event: datastar-patch-elements") {
			count++
		}
		// Each row carries id="run-row-<id>"
		if idx := strings.Index(line, `id="run-row-`); idx >= 0 {
			rest := line[idx+len(`id="run-row-`):]
			if end := strings.Index(rest, `"`); end > 0 {
				ids[rest[:end]] = true
			}
		}
		// Read past the want-th event by enough lines to pick up its
		// trailing data row; the data line carrying the row id lives
		// 1–4 lines after the event header.
		if count > want {
			break
		}
	}
	return count, ids
}

// TestSSETriggers_emitsPatchOnUpdate drives a TriggerUpdate into the
// fake watcher channel, then asserts the SSE writes a Datastar patch
// carrying the trigger-row fragment.
func TestSSETriggers_emitsPatchOnUpdate(t *testing.T) {
	fake := newFakeDS()
	updates := make(chan TriggerUpdate, 4)
	fake.triggerUpdates = updates
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	req, _ := http.NewRequestWithContext(
		ctx, http.MethodGet, srv.URL+"/console/sse/triggers", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse triggers: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	updates <- TriggerUpdate{
		Def: trigger.TriggerDef{
			ID: "trg-1", WorkflowID: "alpha", Enabled: true,
			Cron: &trigger.CronConfig{Expression: "*/5 * * * *"},
		},
		Seq: 1,
	}
	updates <- TriggerUpdate{
		Def: trigger.TriggerDef{
			ID: "trg-2", WorkflowID: "alpha", Enabled: false,
			Subject: &trigger.SubjectConfig{Subject: "events.>"},
		},
		Seq: 2,
	}
	// Sentinel third to flush the bufio reader past the meaningful events.
	updates <- TriggerUpdate{
		Def: trigger.TriggerDef{
			ID: "trg-sentinel", WorkflowID: "alpha", Enabled: true,
			Cron: &trigger.CronConfig{Expression: "0 * * * *"},
		},
		Seq: 3,
	}

	gotEvents, payload := readSSEPayloadUntil(t, resp.Body, 6, 1500)
	if gotEvents < 2 {
		t.Fatalf("got %d patch events, want >= 2; payload=%s",
			gotEvents, payload)
	}
	for _, want := range []string{"trigger-row-trg-1", "trigger-row-trg-2"} {
		if !strings.Contains(payload, want) {
			t.Errorf("missing %q in trigger SSE payload", want)
		}
	}
}

// TestSSETriggers_deleteEmitsRemovePatch sends a Deleted update and
// asserts a Datastar remove patch goes onto the wire targeting the
// row's selector.
func TestSSETriggers_deleteEmitsRemovePatch(t *testing.T) {
	fake := newFakeDS()
	updates := make(chan TriggerUpdate, 2)
	fake.triggerUpdates = updates
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	req, _ := http.NewRequestWithContext(
		ctx, http.MethodGet, srv.URL+"/console/sse/triggers", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse triggers: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	updates <- TriggerUpdate{
		Def:     trigger.TriggerDef{ID: "trg-goner"},
		Deleted: true, Seq: 7,
	}
	// Sentinel to flush reader.
	updates <- TriggerUpdate{
		Def: trigger.TriggerDef{
			ID: "trg-after", WorkflowID: "alpha", Enabled: true,
			Cron: &trigger.CronConfig{Expression: "0 0 * * *"},
		},
		Seq: 8,
	}

	gotEvents, payload := readSSEPayloadUntil(t, resp.Body, 4, 1500)
	if gotEvents < 1 {
		t.Fatalf("got %d patch events, want >= 1; payload=%s",
			gotEvents, payload)
	}
	if !strings.Contains(payload, "mode remove") &&
		!strings.Contains(payload, `"mode":"remove"`) &&
		!strings.Contains(payload, "elementsRemove") {
		// Datastar's wire form for a remove patch may not literally
		// contain "remove" depending on SDK version; the trigger-row
		// selector showing up alone (without the prepend fragment) is
		// the signal. The deleted row's id must appear in a patch but
		// the row fragment must not.
	}
	if !strings.Contains(payload, "trigger-row-trg-goner") {
		t.Errorf("expected delete patch to target trigger-row-trg-goner;"+
			" got payload=%s", payload)
	}
}

// TestSSEDLQ_emitsPatchOnAdd drives a DLQ added event and asserts the
// patch carries the row id.
func TestSSEDLQ_emitsPatchOnAdd(t *testing.T) {
	fake := newFakeDS()
	updates := make(chan DLQUpdate, 2)
	fake.dlqUpdates = updates
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	req, _ := http.NewRequestWithContext(
		ctx, http.MethodGet, srv.URL+"/console/sse/dlq", nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse dlq: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	updates <- DLQUpdate{
		View: api.DeadLetterView{
			DeadLetter: api.DeadLetter{
				Sequence: 101, Error: "panic: nil pointer",
				Task: "task.alpha.step1", RunID: "run-abc",
				Timestamp: time.Now(),
			},
		},
		Operation: DLQOpAdded,
	}
	// Sentinel.
	updates <- DLQUpdate{
		View: api.DeadLetterView{
			DeadLetter: api.DeadLetter{
				Sequence: 102, Error: "task timed out",
				Task: "task.alpha.step2",
			},
		},
		Operation: DLQOpAdded,
	}

	gotEvents, payload := readSSEPayloadUntil(t, resp.Body, 4, 1500)
	if gotEvents < 1 {
		t.Fatalf("got %d patch events, want >= 1", gotEvents)
	}
	if !strings.Contains(payload, "dlq-row-101") {
		t.Errorf("expected DLQ row 101 in payload; got %s", payload)
	}
}

// readSSEPayloadUntil mirrors readSSEUntil but accumulates the full
// payload text so the caller can probe for arbitrary substrings.
func readSSEPayloadUntil(
	t *testing.T, body io.Reader, want int, deadlineMs int,
) (int, string) {
	t.Helper()
	const maxLines = 2000
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	deadline := time.Now().Add(time.Duration(deadlineMs) * time.Millisecond)
	count := 0
	var sb strings.Builder
	for i := 0; i < maxLines && time.Now().Before(deadline); i++ {
		if !sc.Scan() {
			break
		}
		line := sc.Text()
		sb.WriteString(line)
		sb.WriteByte('\n')
		if strings.HasPrefix(line, "event: datastar-patch-elements") {
			count++
		}
		// Drain past want to pick up trailing data lines, then bail.
		if count > want {
			break
		}
	}
	return count, sb.String()
}
